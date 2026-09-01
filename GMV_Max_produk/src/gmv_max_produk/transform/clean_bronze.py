# src/gmv_max_produk/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone
import pandas as pd

from src.gmv_max_produk.utils.transform_utils import (
    parse_mixed_dates,
    to_snake_case,
)
from src.gmv_max_produk.utils.minio_client import filter_by_sheet_watermark

def _canon(x):
    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_maxp(
    gmv_max_produk_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per (creds,sheet_name,toko) berdasarkan watermark (sheet_watermarks).
    Watermark grain is (creds, sheet_name, toko) — toko verbatim.
    Output: (df siap di-load ke BRONZE_DB.bronze_gmv_max_produk, sheet_max_dates: {(creds,sheet_name,toko):iso})
    """
    tiktok_maxp_clean = gmv_max_produk_raw.copy()
    # parse tanggal
    tiktok_maxp_clean["Tanggal"] = parse_mixed_dates(
        tiktok_maxp_clean["Tanggal"], return_date=False
    )
    tiktok_maxp_clean["Waktu posting"] = parse_mixed_dates(
        tiktok_maxp_clean["Waktu posting"], return_date=False
    )

    # copy & snake_case
    df = tiktok_maxp_clean.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang kolom dengan header kosong (sel header GSheet blank / hanya
    # karakter yang di-strip to_snake_case) supaya schema BigQuery valid.
    df = df.loc[:, df.columns != ""]

    # buang baris tanpa id_campaign
    df = df[df["id_campaign"].astype(str).str.strip() != ""]

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["tanggal","toko","id_campaign","id_produk","id_video","biaya","impresi_iklan_produk"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    columns_to_int_bq = [
        'pesanan_sku',
        'impresi_iklan_produk',
        'jumlah_klik_iklan_produk',
        'biaya_per_pesanan',
        'biaya' 
    ]

    for col in columns_to_int_bq:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(df[col], errors='coerce')
                .fillna(0)
                .apply(lambda x: int(round(x)))
                .astype(pd.Int64Dtype())
            )

    # Filter incremental per (creds,sheet_name,toko) — triple grain verbatim
    if "creds" in df.columns and "sheet_name" in df.columns and "toko" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "sheet_name", "toko", "tanggal", sheet_watermarks or {}
        )
    elif "creds" in df.columns and "sheet_name" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "creds", "sheet_name", "toko", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates