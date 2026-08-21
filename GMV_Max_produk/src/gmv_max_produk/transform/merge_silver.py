# src/gmv_max_produk/transform/merge_silver.py
from pathlib import Path
from src.gmv_max_produk.utils.bq_client import get_bq_client


def merge_to_silver():
    bq_client = get_bq_client()
    root_dir = Path(__file__).resolve().parents[3]  # etl-gmv-max/
    sql_path = root_dir / "sql" / "silver_merge_tt_ads_gmvmax.sql"

    merge_sql = sql_path.read_text(encoding="utf-8")
    job = bq_client.query(merge_sql)
    job.result()
    print("Silver MERGE OK.")