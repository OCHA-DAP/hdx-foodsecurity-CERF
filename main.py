import ocha_stratus as stratus
import logging
import coloredlogs
from datetime import datetime

from src.datasources import ipc
from src.config import LOG_LEVEL, PROJECT_PREFIX
from src.utils import date_utils

# References for identifying the peak hunger period
REF_YEAR = 2024
REF_SEVERITY = "3+"

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)

years = [REF_YEAR, REF_YEAR - 1, REF_YEAR - 2]

if __name__ == "__main__":
    now = datetime.now()
    now_formatted = now.strftime("%Y-%m-%d")

    # Get the raw data and find the peak hunger periods from the reference year
    df_raw = ipc.get_raw_ipc()
    df = ipc.process_raw_ipc(df_raw)
    logger.info(f"Identifying peak hunger periods based on {REF_YEAR}")
    df_peak = ipc.identify_peak_hunger_period(df, REF_YEAR, REF_SEVERITY)

    # Check the overlap in reference periods
    df_periods = stratus.load_csv_from_blob(
        blob_name=f"{PROJECT_PREFIX}/peak_lean_season_summary_w_acaps.csv"
    )
    df_peak = date_utils.apply_overlap(df_peak, df_periods)

    # Now calculate the values for each year
    for severity in ["3+", "4", "5"]:
        df_summary = df_peak
        df_summary["phase"] = severity
        fname = f"annualized_ipc_summary_{REF_YEAR}_{severity}_{now_formatted}.csv"
        for year in years:
            df_matched = ipc.match_peak_hunger_period(df, df_peak, year, severity)
            df_summary = df_summary.merge(df_matched, how="left")
            df_summary[f"{year}_report_period"] = df_summary[
                f"{year}_report_period"
            ].apply(date_utils.format_interval)
        df_summary = ipc.add_yoy_changes(df_summary, years)
        df_summary["reference_period"] = df_summary["reference_period"].apply(
            date_utils.format_interval
        )
        stratus.upload_csv_to_blob(
            df_summary, f"{PROJECT_PREFIX}/processed/ipc_updates/{fname}", stage="dev"
        )
        logger.info(f"Output file saved successfully to blob: {fname}")
