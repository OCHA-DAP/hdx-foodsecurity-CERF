import ocha_stratus as stratus
import logging
import coloredlogs
from datetime import datetime, timedelta

from src.datasources import ipc
from src.config import LOG_LEVEL, PROJECT_PREFIX
from src.utils import date_utils, format_utils

logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


if __name__ == "__main__":
    now = datetime.now() - timedelta(days=1)
    now_formatted = now.strftime("%Y-%m-%d")
    ref_year = now.year
    years = [ref_year, ref_year - 1, ref_year - 2]
    ref_severity = "3+"

    # Get the raw data and find the peak hunger periods from the reference year
    logger.info("Identifying peak hunger periods...")
    df = ipc.get_all_ipc()
    df = ipc.combine_4_plus(df)
    df_peak = ipc.identify_peak_hunger_period(df, ref_year, ref_severity)

    # Check the overlap in reference periods
    df_periods = stratus.load_csv_from_blob(
        blob_name=f"{PROJECT_PREFIX}/processed/reference_periods/cleaned_reference_periods.csv"
    ).rename(columns={"Country": "location_code"})
    df_peak = date_utils.apply_overlap(
        df_peak, df_periods, "data_driven_period", "data_driven_period_overlap"
    )
    df_peak = date_utils.apply_overlap(
        df_peak, df_periods, "expert_period_1", "expert_period_1_overlap"
    )
    df_peak = date_utils.apply_overlap(
        df_peak, df_periods, "expert_period_2", "expert_period_2_overlap"
    )

    # Now calculate the values for each year
    for severity in ["3", "3+", "4", "4+", "5"]:
        df_summary = df_peak
        df_summary["phase"] = severity
        fname = f"annualized_ipc_summary_{severity}_{now_formatted}.csv"
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
        df_summary = format_utils.clean_columns(df_summary)
        df_summary = format_utils.add_country_names(df_summary)
        stratus.upload_csv_to_blob(
            df_summary, f"{PROJECT_PREFIX}/processed/ipc_updates/{fname}", stage="dev"
        )
        logger.info(f"Output file saved successfully to blob: {fname}")
