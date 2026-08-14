# src/gmv_max_produk/pipelines/run_daily_etl.py

import io
import os 
from datetime import date

from dotenv import load_dotenv

load_dotenv()

from src.gmv_max_produk.utils.gsheet_client import get_gspread_client
from src.gmv_max_produk.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
)
from src.gmv_max_produk.ingestion.fetch_gmv_max_produk_gsheet import (
    fetch_gmv_max_produk,
    SHEET_REGISTRY,
)
from src.gmv_max_produk.transform.clean_bronze import build_bronze_maxp
from src.gmv_max_produk.transform.merge_silver_duckdb import test_merge_to_silver_duckdb

WATERMARK_PATH = "watermarks/gmv_max.json"


def run_daily_etl():
    print("== Start ETL GMV Max Produk ==")

    # 1) Client
    gc = get_gspread_client()
    minio_client, minio_bucket = get_minio_client()

    # 2) Date key: partition pakai YYYYMMDD, nama file pakai YYYYMMDDHH
    #    (jam agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    today_obj = date.today()
    today_key = today_obj.strftime("%Y%m%d")
    run_key = today_obj.strftime("%Y%m%d%H")

    # 3) Per-sheet watermark check
    # sheet_registry hanya dibutuhkan utk FAILSAFE migrasi format lama (sheet_name -> creds).
    sheet_registry = {name: os.getenv(env_key) for name, env_key in SHEET_REGISTRY.items()}
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, sheet_registry=sheet_registry
    )

    # 4) Ingest from GSheet (each sheet tagged with sheet_name)
    df_raw = fetch_gmv_max_produk(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")

    # 5) Bronze Transformation + per-sheet incremental filter
    df_bronze, sheet_max_dates = build_bronze_maxp(
        df_raw, sheet_watermarks=watermark_map
    )
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        print("== ETL GMV Max DONE ==")
        return

    # 6) Parquet conversion & Load to MinIO
    file_path = f"gmv/max/date={today_key}/max_{run_key}.parquet"
    folder_path = f"gmv/max/date={today_key}/"

    # Folder partition marker
    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    # Convert & Upload Parquet
    parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
    minio_client.put_object(
        minio_bucket,
        file_path,
        io.BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    print(f"[MINIO] Successfully uploaded Parquet file to: {file_path}")

    # 7) Update per-sheet watermark (selalu tulis format baru)
    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
        sheet_registry=sheet_registry,
    )

    # 8) Testing Load to Bronze & Silver via DuckDB (In-Memory)
    test_merge_to_silver_duckdb(df_bronze)

    print("== ETL GMV Max Produk DONE ==")


if __name__ == "__main__":
    run_daily_etl()
