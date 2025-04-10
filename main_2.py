import pandas as pd
import ocha_stratus as stratus

from src.utils.data_wrangling import standardize_data

SEVERITY = "3+"
PROJECT_PREFIX = "ds-ufe-food-security"
REF_YEAR = 2024


def get_raw_ipc():
    return stratus.load_csv_from_blob(
        f"{PROJECT_PREFIX}/ipc_global_national_long.csv", stage="dev"
    )[1:]


def process_raw_ipc(df, severity="3+"):
    df = df.copy()
    df = standardize_data(df)
    df["year"] = df["To"].dt.year
    return df[df["Phase"] == severity]


def identify_peak_hunger_period(df, year):
    df = df.copy()
    # Filter to a specific year
    # Note that `year` is associated with the `To` date
    df_filtered = df[df["year"] == year]
    # Get the most recent report if there are duplicates for the same time period
    df_filtered = df_filtered.sort_values(["Date of analysis"], ascending=False)
    df_filtered = df_filtered.drop_duplicates(
        subset=["Country", "From", "To"], keep="first"
    )
    # Now pick the one that has the highest food insecurity
    df_filtered = df_filtered.sort_values("Percentage", ascending=False)
    df_filtered = df_filtered.drop_duplicates(subset=["Country"], keep="first")
    # And create the reference period
    df_filtered["reference_period"] = df_filtered.apply(
        lambda x: pd.Interval(left=x["From"], right=x["To"], closed="both"), axis=1
    )
    # Check for any missing countries
    if len(set(df["Country"] != set(df["Country"]))):
        missing_countries = list(set(df["Country"]) - set(df_filtered["Country"]))
        print(
            f"Warning! {len(missing_countries)} countries do not have reports from {year}:"
        )
        print(missing_countries)
    df_filtered["reference_year"] = year

    return (
        df_filtered[["Country", "reference_year", "reference_period"]]
        .sort_values("Country")
        .reset_index()
        .drop(columns=["index"])
    )


# TODO: Logging for cases where there aren't matches in the period
def match_peak_hunger_period(df, df_peak, year):
    ref_year = int(df_peak.reference_year[0])
    df = df.copy()
    df_ = df[df.year == year]
    # Convert From / To dates to a period
    df_[f"{year}_report_period"] = df_.apply(
        lambda x: pd.Interval(left=x["From"], right=x["To"], closed="both"), axis=1
    )

    # Now create a reference period
    # A bit annoying to do since we want to check overlap independent from the years
    df_["ref_period"] = df_.apply(
        lambda row: pd.Interval(
            pd.Timestamp(
                year=ref_year - (1 if row["From"].month > row["To"].month else 0),
                month=row["From"].month,
                day=row["From"].day,
            ),
            pd.Timestamp(year=ref_year, month=row["To"].month, day=row["To"].day),
            closed="both",
        ),
        axis=1,
    )

    df_merged = df_.merge(df_peak)
    df_merged["has_overlap"] = df_merged.apply(
        lambda row: row["ref_period"].overlaps(row["reference_period"]),
        axis=1,
    )
    # Get only the ones that have overlap
    df_merged = df_merged[df_merged.has_overlap]

    # Now drop duplicate countries and get the one with the worst conditions
    df_merged = df_merged.sort_values("Percentage", ascending=False)
    df_merged = df_merged.drop_duplicates(subset=["Country"], keep="first")

    # Do some basic cleaning of the columns
    df_clean = df_merged.rename(
        columns={
            "Total country population": f"{year}_total_pop",
            "Percentage": f"{year}_percentage",
            "Number": f"{year}_number",
        }
    )
    df_clean = df_clean[
        [
            "Country",
            f"{year}_report_period",
            f"{year}_total_pop",
            f"{year}_number",
            f"{year}_percentage",
        ]
    ].sort_values("Country")
    return df_clean


if __name__ == "__main__":
    df_raw = get_raw_ipc()
    df = process_raw_ipc(df_raw)
    df_peak = identify_peak_hunger_period(df, REF_YEAR)

    df_summary = df_peak
    for year in [2024, 2023, 2022]:
        df_ = match_peak_hunger_period(df, df_peak, year)
        df_summary = df_summary.merge(df_, how="left")
        stratus.upload_csv_to_blob(
            df_summary, f"{PROJECT_PREFIX}/annualized_summary.csv", stage="dev"
        )
