# src/gmv_max_produk/utils/gsheet_client.py
import os

import gspread
from dotenv import load_dotenv

load_dotenv()


def get_gspread_client() -> gspread.Client:
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")

    return gspread.service_account(filename=sa_path)
