# src/gmv_max_produk/pipelines/run_daily_etl.py

import io
import os 
import traceback
from google.oauth2 import service_account

import pandas as pd

from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

from src.gmv_max_produk.utils.gsheet_client import get_gspread_client
from src.gmv_max_produk.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    write_quarantine,
    sync_error_manifest,
    filter_already_quarantined,
)
from src.gmv_max_produk.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    validate_and_normalize_raw,
)
from src.gmv_max_produk.ingestion.fetch_gmv_max_produk_gsheet import (
    fetch_gmv_max_produk,
)
from src.gmv_max_produk.transform.clean_bronze import build_bronze_maxp
from src.gmv_max_produk.transform.merge_silver import merge_to_silver
from src.gmv_max_produk.load.load_to_bigquery import load_df
from src.gmv_max_produk.utils.bronze_compare import (
    finish,
    effective_watermark_changes,
)
from src.gmv_max_produk.utils.log import (
    get_log_folder,
    write_section_log,
    setup_event_logging,
    is_event_logging_enabled,
    emit,
)
from src.gmv_max_produk.utils.watermark_monitor import gmv_max_produk_watermark_check

WATERMARK_PATH = "watermarks/gmv_max.json"
PROJECT_ID = "database-sigma"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "GMV MAX Produk"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "gmv/max"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine/gmv_max/"}
TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.bronze_maxp"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_produk"}
COMPARE_TARGET = {"system": "BigQuery", "entity": f"{PROJECT_ID}.Testing.bronze_maxp"}

# Compare original (BRONZE_DB) vs testing (Testing) bronze at the end of the run.
COMPARE_CONFIG = {
    "original_tables": ("BRONZE_DB.bronze_maxp",),
    "testing_tables": ("Testing.bronze_maxp",),
    "group_col": "toko",
    "date_col": "tanggal",
}

def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def _fetch_existing_bronze_hashes(
    creds, table_id="Testing.bronze_maxp", project_id=PROJECT_ID
) -> set:
    """Returns the set of row_hash_raw already present in Bronze.

    Used as an append-time idempotency gate: boundary-day rows (tanggal ==
    watermark) and re-loaded recovery rows are legitimately re-selected by the
    watermark filter every run; this drops the ones whose content is unchanged,
    so Bronze stops accumulating duplicates while still accepting edits (a
    changed row produces a NEW hash and flows through).
    """
    from pandas_gbq import read_gbq

    df_hashes = read_gbq(
        f"SELECT DISTINCT row_hash_raw FROM `{project_id}.{table_id}`",
        project_id=project_id,
        credentials=creds,
        dialect="standard",
    )
    return set(df_hashes["row_hash_raw"].dropna().astype(str))


def _select_recovered(
    df_valid: pd.DataFrame, resolved: list, report: dict
) -> pd.DataFrame:
    """PATH A: select rows from df_valid that were recovered from a resolved error.

    Grain is (sheet_name, creds, toko, error_date) — toko verbatim. A resolved
    entry means the key was in the error manifest last run but is NO LONGER
    in df_error this run (the data got fixed). Those rows bypass the watermark
    filter downstream.

    Full recovery only: we include the key's rows ONLY when the number of
    valid rows now equals the manifest n_rows. Otherwise the group is either
    only partially fixed (some rows still bad -> entry stays open) or extra
    rows appeared on that historical date. Skipping avoids duplicates and
    partial/incorrect recovery; the data is never silently lost because the
    entry remains "open" and will be retried on a later run.

    Counters are added to `report`:
      recovery_resolved        : resolved keys considered
      recovery_recovered_rows  : rows selected for Path A
      recovery_count_mismatch  : keys fixed but row_count != n_rows (skipped)
      recovery_absent          : resolved keys with no matching rows (deleted)
    """
    df = df_valid.copy()

    if df.empty or not resolved:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_recovered_rows", 0)
        report.setdefault("recovery_count_mismatch", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    # Toko verbatim — column is "Toko" raw, fallback "toko" or ""
    if "Toko" in df.columns:
        toko_series = df["Toko"].astype(str)
    elif "toko" in df.columns:
        toko_series = df["toko"].astype(str)
    else:
        toko_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = df["Tanggal"].dt.date.astype(str)
    except Exception:
        tanggal_str = pd.to_datetime(df["Tanggal"]).dt.date.astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    count_mismatch = 0
    absent = 0

    for r in resolved:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
        grp = df.index[key_series == key]
        n_expected = int(r.get("n_rows") or 0)

        if len(grp) == 0:
            absent += 1                      # rows removed from sheet
        elif len(grp) == n_expected:
            match.loc[grp] = True            # fully recovered -> Path A
        else:
            count_mismatch += 1              # FIXED but count mismatch -> skip

    report["recovery_resolved"] = len(resolved)
    report["recovery_recovered_rows"] = int(match.sum())
    report["recovery_count_mismatch"] = count_mismatch
    report["recovery_absent"] = absent

    return df[match]


def _write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict_msg):
    """Write the watermark drift check log file."""
    wm_log_lines = []
    wm_log_lines.append(
        f"=== WATERMARK DRIFT CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n"
    )

    wm_log_lines.append("-" * 50)
    wm_log_lines.append("DATASET: GMV MAX PRODUK (toko grain)")
    wm_log_lines.append("-" * 50)
    wm_log_lines.append(f"  {'sheet':<10} {'grain':<12} {'gsheet':<12} {'wm':<12} {'status'}")
    for _, row in status_df.iterrows():
        wm_log_lines.append(
            f"  {str(row['sheet_name']):<10} {str(row['grain']):<12} "
            f"{str(row['sheet_max_tanggal']):<12} {str(row['last_processed_date']):<12} "
            f"{'BEHIND' if row['is_behind'] else 'ok'}"
        )

    wm_log_lines.append(f"\nGate verdict: {verdict_msg}")
    pass_count = int(sheet_passes.sum()) if len(sheet_passes) else 0
    total = len(sheet_passes)
    wm_log_lines.append(f"  {pass_count}/{total} sheets have >=1 toko behind")

    write_section_log(log_folder, f"wm_monitor_logs_{run_key}.log", "\n".join(wm_log_lines) + "\n")


def _write_failure_log(log_folder, run_key, reason):
    """Write a dedicated ETL failure log file (etl_failed_<run_key>.log)."""
    lines = [
        f"=== ETL FAILED - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
        f"Dataset: gmv_max_produk",
        f"  Reason: {reason}",
    ]
    write_section_log(log_folder, f"etl_failed_{run_key}.log", "\n".join(lines) + "\n")


def run_daily_etl(dry_run: bool | None = None):
    print("== Start ETL GMV Max Produk ==")

    if dry_run is None:
        dry_run = os.getenv("ETL_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}

    if dry_run:
        print("[DRY-RUN] Mode aktif: TIDAK ada data yang ditulis ke MinIO/BigQuery/Silver.")

    # 1) Client
    gc = get_gspread_client()
    minio_client, minio_bucket = get_minio_client()
    creds = _get_credentials()

    # 2) Date key: partition pakai YYYYMMDD, nama file pakai YYYYMMDDHHMM
    #    (jam+menit agar 2 run di hari yang sama menghasilkan file terpisah, tanpa overwrite).
    now_obj = datetime.now()
    today_key = now_obj.strftime("%Y%m%d")
    run_key = now_obj.strftime("%Y%m%d%H%M")
    log_folder = get_log_folder(run_key) if not dry_run else None

    # Structured JSON events: enabled for production AND dry-run. When invoked
    # standalone (not via main.py), set it up with this run's own dry_run flag
    # and emit the pipeline-start event here (main.py emits it when running via
    # that entrypoint, so we avoid a duplicate on that path).
    if not is_event_logging_enabled():
        setup_event_logging(run_key, dry_run)
        emit("PIPELINE", "etl_pipeline", "Start ETL GMV Max Produk",
             metrics={"run_key": run_key, "today_key": today_key})

    # 2B) PRE-FLIGHT: Watermark drift gate
    print("\n" + "=" * 70)
    print("--- PRE-FLIGHT: Watermark Drift Check ---")
    print("=" * 70)
    status_df = gmv_max_produk_watermark_check()

    view = pd.DataFrame({
        "sheet_name": status_df["sheet_name"],
        "toko": status_df["grain"],
        "gsheet": status_df["sheet_max_tanggal"],
        "wm": status_df["last_processed_date"],
        "flag": status_df["is_behind"].map({True: "BEHIND", False: "ok"}),
    })
    print("-" * 70)
    print("DATASET: GMV MAX PRODUK (toko grain)")
    print("-" * 70)
    print(view.sort_values(["sheet_name", "toko"]).to_string(
        index=False,
        formatters={
            "gsheet": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "wm": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
        },
    ))
    print()
    emit(
        "PRE_FLIGHT", "watermark_monitor",
        f"Watermark drift check: {len(status_df)} group(s), "
        f"{int(status_df['is_behind'].sum())} behind",
        metrics={
            "total_groups": int(len(status_df)),
            "groups_behind": int(status_df["is_behind"].sum()),
        },
        source=SRC_GSHEET,
        target=SRC_MINIO,
    )

    # Gate 1: abort on access errors (enforced in dry-run too).
    # Empty status_df (no watermark yet / first run) -> no errors, let gate 2 pass.
    has_errors = bool(len(status_df)) and status_df["status"].str.startswith("error").any()
    if has_errors:
        error_sheets = status_df[status_df["status"].str.startswith("error")]["sheet_name"].unique().tolist()
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] {mode} - access errors on sheets: {error_sheets}")
        print("[GATE] Fix the sheet access issue and re-run.")
        emit(
            "GATE", "etl_gate",
            f"Access errors on sheets: {error_sheets} - fix the sheet access issue and re-run",
            level="ERROR",
            metrics={"error_sheets": error_sheets, "n_error_sheets": len(error_sheets)},
            source=SRC_GSHEET,
            target=SRC_MINIO,
        )
        if log_folder:
            _write_wm_log(log_folder, run_key, status_df, pd.Series(dtype=bool),
                          f"ABORT - access errors on sheets: {error_sheets}")
            _write_failure_log(log_folder, run_key,
                               f"access errors on sheets: {error_sheets}")
        return

    # Gate 2: every sheet must have >=1 toko behind (enforced in dry-run too).
    sheet_passes = status_df.groupby("sheet_name")["is_behind"].any()
    caught_up = sheet_passes[~sheet_passes].index.tolist()
    behind_sheets = sheet_passes[sheet_passes].index.tolist()
    if caught_up:
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] Sheets already up-to-date (skipped): {caught_up}")
        print(f"[GATE] Sheets with new data: {behind_sheets}")
        print(f"[GATE] {mode} - sheets with no new data are required before continuing.")
        emit(
            "GATE", "etl_gate",
            f"Sheets with no new data are required before continuing: {caught_up}",
            level="ERROR",
            metrics={
                "caught_up": caught_up,
                "behind": behind_sheets,
                "total_groups": int(len(status_df)),
                "sheets_up_to_date": len(caught_up),
                "sheets_with_new_data": len(behind_sheets),
            },
            source=SRC_GSHEET,
            target=SRC_MINIO,
        )
        if log_folder:
            _write_wm_log(log_folder, run_key, status_df, sheet_passes,
                          f"ABORT - sheets with no new data: {caught_up}")
            _write_failure_log(log_folder, run_key,
                               f"sheets with no new data (already up-to-date): {caught_up}")
        return

    print("--- PRE-FLIGHT PASSED ---\n")

    # Write wm_monitor log
    if log_folder:
        pass_count = int(sheet_passes.sum())
        total = len(sheet_passes)
        verdict = f"PASS - {pass_count}/{total} sheets have >=1 toko behind"
        _write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict)

    # 3) Per-sheet watermark check
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH
    )

    # 4) Ingest from GSheet (each sheet tagged with sheet_name)
    df_raw = fetch_gmv_max_produk(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")
    emit("INGEST", "gsheet_ingester", f"Fetched {len(df_raw)} raw rows from GSheet",
         metrics={"rows_raw": int(len(df_raw))},
         source=SRC_GSHEET)

    # 4b) STEP 2: validate & normalize as early as possible (mixed-column
    #     detection + date-error capture). Runs exactly once, before anything else.
    
    # buang baris tanpa id_campaign
    df_raw = df_raw[df_raw["ID Campaign"].astype(str).str.strip() != ""]
    df_valid, df_error, v_report = validate_and_normalize_raw(
        df_raw, NUMERIC_COLS, percent_cols=PERCENT_COLS
    )
    print(
        f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
        f"(date errors: {v_report['n_date_errors']} | future date errors: {v_report.get('n_date_future',0)} | toko_blank: {v_report.get('n_toko_blank',0)}) | blank rows dropped: {v_report['n_blank_rows']}"
    )
    emit(
        "VALIDATE", "validator",
        f"Validated: {len(df_valid)} valid, {v_report['n_bad_rows']} bad",
        level="WARN" if v_report["n_bad_rows"] else "INFO",
        metrics={
            "rows_raw": int(len(df_raw)),
            "rows_valid": int(len(df_valid)),
            "n_bad_rows": int(v_report["n_bad_rows"]),
            "n_date_errors": int(v_report["n_date_errors"]),
            "n_date_future": int(v_report.get("n_date_future", 0)),
            "n_toko_blank": int(v_report.get("n_toko_blank", 0)),
            "n_blank_rows": int(v_report["n_blank_rows"]),
        },
        source=SRC_GSHEET,
        target=TGT_MINIO_QUARANTINE,
    )
    if v_report["has_changes"]:
        print(f"[VALIDATE] Corrupted/Shifted columns: {v_report['affected_columns']}")
        print(
            f"[VALIDATE] Affected date range: {v_report['first_affected_date']} "
            f"---> {v_report['last_affected_date']}"
        )

    # STEP 3Q/6: sync error manifest EVERY run (append new open entries +
    # resolve entries whose format has been fixed since the last run).
    # Resolved entries feed PATH A (error recovery) below.
    resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, df_valid=df_valid, dry_run=dry_run)

    df_error_new = (
        filter_already_quarantined(minio_client, minio_bucket, df_error)
        if not df_error.empty
        else df_error
    )
    if not df_error_new.empty:
        if dry_run:
            print(f"[DRY-RUN] Akan quarantine {len(df_error_new)} bad row(s)")
        else:
            write_quarantine(minio_client, minio_bucket, df_error_new, today_key, run_key)

            # Write quarantine log with summary + sample bad rows
            if log_folder:
                import re as _re
                q_lines = []
                q_lines.append(f"=== QUARANTINE REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                q_lines.append("Dataset: gmv_max_produk")
                q_lines.append(f"  Total quarantined rows: {len(df_error_new)}\n")

                if "error_reason" in df_error_new.columns:
                    all_reasons = df_error_new["error_reason"].str.split("|").explode()
                    reason_counts = all_reasons.value_counts()
                    q_lines.append("  Error reasons breakdown:")
                    for reason, count in reason_counts.items():
                        q_lines.append(f"    {reason} : {count} rows")
                    q_lines.append("")

                    col_pattern = df_error_new["error_reason"].str.findall(r"date_unparsable\((\w+)=")
                    affected_from_dates = set()
                    for cols in col_pattern:
                        affected_from_dates.update(cols)
                    affected_cols = sorted(affected_from_dates | set(v_report.get("affected_columns", [])))
                    if affected_cols:
                        q_lines.append(f"  Affected columns: {affected_cols}\n")

                sample = df_error_new.head(5)
                q_lines.append(f"  Sample bad rows (first {len(sample)}):")
                display_cols = [c for c in ["Tanggal", "tanggal", "Toko", "toko",
                                             "ID Campaign", "id_campaign", "error_reason"] if c in sample.columns]
                if display_cols:
                    header = " | ".join(f"{c:<15}" for c in display_cols)
                    q_lines.append(f"    | {header} |")
                    q_lines.append(f"    | {'-' * len(header)} |")
                    for _, r in sample.iterrows():
                        vals = " | ".join(f"{str(r.get(c, '')):<15}" for c in display_cols)
                        q_lines.append(f"    | {vals} |")

                q_lines.append("")
                write_section_log(log_folder, f"quarantine_errors_{run_key}.log", "\n".join(q_lines) + "\n")

        emit(
            "QUARANTINE", "validator",
            f"{len(df_error_new)} bad row(s) quarantined",
            level="WARN",
            metrics={"n_quarantined": int(len(df_error_new))},
            source=SRC_GSHEET,
            target=TGT_MINIO_QUARANTINE,
        )

    # PATH A: recovered rows (fixed since last run) bypass the watermark.
    df_recovered = _select_recovered(df_valid, resolved, v_report)
    print(
        f"[RECOVERY] resolved={v_report.get('recovery_resolved', 0)} "
        f"| recovered_rows={v_report.get('recovery_recovered_rows', 0)} "
        f"| absent={v_report.get('recovery_absent', 0)} "
        f"| count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}"
    )
    emit(
        "RECOVERY", "error_recovery",
        f"Recovery: resolved={v_report.get('recovery_resolved', 0)}, "
        f"recovered_rows={v_report.get('recovery_recovered_rows', 0)}, "
        f"absent={v_report.get('recovery_absent', 0)}, "
        f"count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}",
        level="WARN" if v_report.get("recovery_count_mismatch", 0) or v_report.get("recovery_absent", 0) else "INFO",
        metrics={
            "resolved": int(v_report.get("recovery_resolved", 0)),
            "recovered_rows": int(v_report.get("recovery_recovered_rows", 0)),
            "absent": int(v_report.get("recovery_absent", 0)),
            "count_mismatch_skipped": int(v_report.get("recovery_count_mismatch", 0)),
        },
        source=SRC_GSHEET,
        target=TGT_BQ_BRONZE,
    )

    # PATH B: remaining rows use the standard per-sheet watermark filter.
    df_regular = df_valid.drop(df_recovered.index)
    df_bronze_regular, sheet_max_dates = build_bronze_maxp(
        df_regular, sheet_watermarks=watermark_map
    )

    # PATH A transform: empty watermarks = full load, max dates fed into the
    # per-sheet watermark so recovered rows are not re-selected every run.
    if df_recovered.empty:
        df_bronze_recovered = df_bronze_regular.iloc[0:0]
    else:
        df_bronze_recovered, recovered_max_dates = build_bronze_maxp(
            df_recovered, sheet_watermarks={}
        )
        for key, max_date in recovered_max_dates.items():
            sheet_max_dates[key] = max(sheet_max_dates.get(key, max_date), max_date)

    # MERGE & DEDUPLICATE
    df_bronze = pd.concat(
        [df_bronze_regular, df_bronze_recovered], ignore_index=True
    ).drop_duplicates(subset=["row_hash_raw"])

    # Idempotency gate: drop rows whose content hash already exists in Bronze.
    # Boundary-day re-emissions and previously-recovered rows are re-selected by
    # the watermark each run; only genuinely new/changed rows should be appended.
    if not df_bronze.empty:
        existing_hashes = _fetch_existing_bronze_hashes(creds)
        if existing_hashes:
            before = len(df_bronze)
            df_bronze = df_bronze[
                ~df_bronze["row_hash_raw"].astype(str).isin(existing_hashes)
            ]
            skipped = before - len(df_bronze)
            if skipped:
                print(
                    f"[IDEMPOTENCY] Skipped {skipped} row(s) already present in bronze"
                )
                emit(
                    "BRONZE", "idempotency_gate",
                    f"Skipped {skipped} row(s) already present in bronze",
                    level="INFO",
                    metrics={"skipped": int(skipped)},
                    source=SRC_GSHEET,
                    target=TGT_BQ_BRONZE,
                )

    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")
    emit("BRONZE", "bronze_builder", f"Rows bronze to load: {len(df_bronze)}",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET,
         target=TGT_BQ_BRONZE)

    # Nothing new to append: if rows were still selected (boundary-day /
    # recovered re-emissions) but every one already exists in Bronze, advance
    # the watermark anyway so they stop being re-selected every run.
    if df_bronze.empty and sheet_max_dates:
        if dry_run:
            changes = effective_watermark_changes(watermark_records, sheet_max_dates)
            for (sheet_key, sheet_name, toko), max_date in changes.items():
                print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
            if changes:
                print("[DRY-RUN] Akan update watermark (tanpa upload parquet).")
            else:
                print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
            emit("FINISH", "etl_pipeline", "ETL DONE (DRY-RUN) - watermark only",
                 metrics={"watermark_changes": len(changes)},
                 source=SRC_GSHEET, target=SRC_MINIO)
            finish(creds, watermark_records, "== ETL GMV Max DONE (DRY-RUN) ==", **COMPARE_CONFIG)
            return
        update_sheet_watermarks(
            minio_client, minio_bucket, WATERMARK_PATH, watermark_records,
            sheet_max_dates,
        )
        print("[MINIO] Watermark advanced (no new rows to append).")
        emit("FINISH", "etl_pipeline", "ETL DONE - watermark advanced (no new rows)",
             metrics={"watermark_changes": len(sheet_max_dates)},
             source=SRC_GSHEET, target=SRC_MINIO)
        finish(creds, watermark_records, "== ETL GMV Max DONE ==", **COMPARE_CONFIG)
        return

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        emit("FINISH", "etl_pipeline", "ETL DONE - no new data to process",
             metrics={"rows_loaded": 0},
             source=SRC_GSHEET, target=SRC_MINIO)
        finish(creds, watermark_records, "== ETL GMV Max DONE ==", **COMPARE_CONFIG)
        return

    # 6) Parquet conversion & Load to MinIO
    file_path = f"gmv/max/date={today_key}/max_{run_key}.parquet"
    folder_path = f"gmv/max/date={today_key}/"

    if dry_run:
        print(f"[DRY-RUN] Akan upload {len(df_bronze)} baris ke '{file_path}'")
        changes = effective_watermark_changes(watermark_records, sheet_max_dates)
        for (sheet_key, sheet_name, toko), max_date in changes.items():
            print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
        if not changes:
            print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
        print("[DRY-RUN] Akan: append ke Testing.bronze_maxp + MERGE ke silver_tt_ads_gmvmax")
        print("[DRY-RUN] Selesai. TIDAK ada data yang ditulis (dry-run).")
        emit("FINISH", "etl_pipeline",
             f"ETL DONE (DRY-RUN) - would load {len(df_bronze)} rows",
             metrics={"rows_loaded": int(len(df_bronze)),
                      "watermark_changes": len(changes)},
             source=SRC_GSHEET, target=TGT_BQ_BRONZE)
        finish(creds, watermark_records, "== ETL GMV Max Produk DONE (DRY-RUN) ==", **COMPARE_CONFIG)
        return

    # Folder partition marker
    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    # Convert & Upload Parquet
    try:
        parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
        minio_client.put_object(
            minio_bucket,
            file_path,
            io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
    except Exception as e:
        emit(
            "LOAD", "minio_loader",
            f"Parquet upload failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"rows": int(len(df_bronze)), "target_path": file_path},
            source=SRC_GSHEET,
            target=TGT_MINIO,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: gmv_max_produk",
                f"  Stage: Parquet upload",
                f"  Target path: {file_path}",
                f"  Rows: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        raise
    print(f"[MINIO] Successfully uploaded Parquet file to: {file_path}")
    emit("LOAD", "minio_loader", f"Parquet uploaded to {file_path}",
         metrics={"rows": int(len(df_bronze)), "target_path": file_path},
         source=SRC_GSHEET, target=TGT_MINIO)

    # 7) Update per-(creds,sheet_name,toko) watermark — triple grain, flush again
    # sheet_name is part of the key, no creds_sheet_map needed (kept optional for compat)
    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
    )

    # 7) Load : Bronze
    try:
        load_df(
            df_bronze,
            table_id="Testing.bronze_maxp",
            project_id=PROJECT_ID,
            if_exists="append",
            credentials=creds,
        )
    except Exception as e:
        emit(
            "LOAD", "bigquery_loader",
            f"BigQuery load failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={
                "rows_loaded": int(len(df_bronze)),
                "table": f"{PROJECT_ID}.Testing.bronze_maxp",
                "if_exists": "append",
            },
            source=SRC_MINIO,
            target=TGT_BQ_BRONZE,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: gmv_max_produk",
                f"  Stage: BigQuery load",
                f"  Target table: Testing.bronze_maxp",
                f"  Rows being loaded: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        raise
    print("[BRONZE] Load to BRONZE_DB.bronze_maxp DONE")
    emit("LOAD", "bigquery_loader", "Bronze load to Testing.bronze_maxp DONE",
         metrics={"rows_loaded": int(len(df_bronze)),
                  "table": f"{PROJECT_ID}.Testing.bronze_maxp"},
         source=SRC_MINIO, target=TGT_BQ_BRONZE)

    # 8) Merge : Silver
    print("[SILVER] Running MERGE into Testing.silver_tt_ads_gmvmax_produk ...")
    try:
        merge_to_silver()
    except Exception as e:
        emit(
            "SILVER", "silver_merger",
            f"Silver MERGE failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_produk"},
            source=TGT_BQ_BRONZE,
            target=TGT_BQ_SILVER,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Silver MERGE",
                f"  Table: Testing.silver_tt_ads_gmvmax_produk",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        raise
    print("[SILVER] MERGE DONE")
    emit("SILVER", "silver_merger", "Silver MERGE into Testing.silver_tt_ads_gmvmax_produk DONE",
         metrics={"table": f"{PROJECT_ID}.Testing.silver_tt_ads_gmvmax_produk"},
         source=TGT_BQ_BRONZE, target=TGT_BQ_SILVER)

    emit("FINISH", "etl_pipeline", "ETL GMV Max Produk DONE",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET, target=TGT_BQ_SILVER)
    finish(creds, watermark_records, "== ETL GMV Max Produk DONE ==", **COMPARE_CONFIG)


if __name__ == "__main__":
    run_daily_etl()
