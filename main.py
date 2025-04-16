import ocha_stratus as stratus
import logging
import coloredlogs

from src.datasources import ipc
from src.config import LOG_LEVEL

SEVERITY = "3+"
PROJECT_PREFIX = "ds-ufe-food-security"
REF_YEAR = 2024

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    df_raw = ipc.get_raw_ipc()
    df = ipc.process_raw_ipc(df_raw)
    logger.info(f"Identifying peak hunger periods based on {REF_YEAR}")
    df_peak = ipc.identify_peak_hunger_period(df, REF_YEAR)

    df_summary = df_peak
    for year in [2024, 2023, 2022]:
        logger.info(f"Finding IPC data from {year}...")
        df_ = ipc.match_peak_hunger_period(df, df_peak, year)
        df_summary = df_summary.merge(df_, how="left")
        stratus.upload_csv_to_blob(
            df_summary,
            f"{PROJECT_PREFIX}/annualized_summary_{SEVERITY}.csv",
            stage="dev",
        )
        df_summary.to_csv("summary.csv")
        logger.info("Output file saved successfully to blob")
