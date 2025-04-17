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


if __name__ == "__main__":
    now = datetime.now()
    now_formatted = now.strftime("%Y-%m-%d")

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
        fname = f"{REF_YEAR}_ipc_summary_{severity}_{now_formatted}.csv"
        for year in [REF_YEAR, REF_YEAR - 1, REF_YEAR - 2]:
            df_matched = ipc.match_peak_hunger_period(df, df_peak, year, severity)
            df_summary = df_summary.merge(df_matched, how="left")
        stratus.upload_csv_to_blob(df_summary, f"{PROJECT_PREFIX}/{fname}", stage="dev")
        df_summary.to_csv(fname)
        logger.info(f"Output file saved successfully to blob: {fname}")
