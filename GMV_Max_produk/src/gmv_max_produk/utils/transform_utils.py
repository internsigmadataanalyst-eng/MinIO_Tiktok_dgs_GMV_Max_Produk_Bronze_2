# src/gmv_max_produk/utils/transform_utils.py
import numpy as np
import pandas as pd
from typing import Any, Dict, List, Union

EXCEL_EPOCH = pd.Timestamp("1899-12-30")


def to_snake_case(column_name: str) -> str:
    return (
        column_name.lower()
        .strip()
        .replace(" ", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "")
        .replace("/", "")
        .replace("-", "")
    )


def clean_numeric_columns(df: pd.DataFrame, cols, fillna_value=0) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col not in df.columns:
            print(f"Kolom '{col}' tidak ditemukan di DataFrame. Lewati Nggih.")
            continue

        df[col] = df[col].astype(str)
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].str.replace(r"[^\d,\.]", "", regex=True)
        df[col] = df[col].str.replace(".", "", regex=False)
        df[col] = df[col].str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(fillna_value)

        if (df[col] % 1 == 0).all():
            df[col] = df[col].astype(int)

    return df


def parse_mixed_dates(series: pd.Series, return_date=True) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": np.nan, "-": np.nan, "nan": np.nan, "None": np.nan})

    s_norm = s.str.replace(r"[-\.]", "/", regex=True)

    mask_ymd = s_norm.str.match(r"^\s*\d{4}/\d{1,2}/\d{1,2}\s*$", na=False)
    ymd = pd.to_datetime(s_norm.where(mask_ymd), format="%Y/%m/%d", errors="coerce")

    mask_dmy4 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$", na=False)
    dmy4 = pd.to_datetime(s_norm.where(mask_dmy4), format="%d/%m/%Y", errors="coerce")

    mask_dmy2 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{2}\s*$", na=False)
    dmy2 = pd.to_datetime(s_norm.where(mask_dmy2), format="%d/%m/%y", errors="coerce")

    iso_generic = pd.to_datetime(s, errors="coerce", format=None)

    mask_serial = s.str.match(r"^\d{3,6}$", na=False)
    serial_vals = pd.to_numeric(s.where(mask_serial), errors="coerce")
    serial = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    serial.loc[mask_serial] = EXCEL_EPOCH + pd.to_timedelta(
        serial_vals.loc[mask_serial], unit="D"
    )

    parsed = (
        ymd.combine_first(dmy4)
        .combine_first(dmy2)
        .combine_first(serial)
        .combine_first(iso_generic)
    )

    if return_date:
        return parsed.dt.date

    return parsed
