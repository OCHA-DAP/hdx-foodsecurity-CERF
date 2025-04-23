import ocha_stratus as stratus
import pandas as pd
import logging
import coloredlogs
from src.config import LOG_LEVEL, PROJECT_PREFIX
from src.utils import date_utils


logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def get_raw_ipc() -> pd.DataFrame:
    """
    Retrieve raw IPC (Integrated Food Security Phase Classification) data from blob storage.

    Returns
    -------
    pandas.DataFrame
        Raw IPC data in long format
    """
    return stratus.load_csv_from_blob(
        f"{PROJECT_PREFIX}/ipc_global_national_long.csv", stage="dev"
    )[1:]


def process_raw_ipc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw IPC data by standardizing columns.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw IPC data.

    Returns
    -------
    pandas.DataFrame
        Processed IPC data .
    """
    df = df.copy()
    # Standardize the data
    df["Number"] = pd.to_numeric(df["Number"])
    df["Percentage"] = pd.to_numeric(df["Percentage"])
    df["Total country population"] = pd.to_numeric(df["Total country population"])

    # Add more detailed date information
    df["Date of analysis"] = pd.to_datetime(df["Date of analysis"], format="%b %Y")
    df["From"] = pd.to_datetime(df["From"])
    df["To"] = pd.to_datetime(df["To"])
    df["year"] = df["To"].dt.year

    return df


def identify_peak_hunger_period(
    df: pd.DataFrame, year: int, severity: str
) -> pd.DataFrame:
    """
    Identify the peak hunger period for each country in a given year.

    For each country, finds the time period with the highest food insecurity
    percentage within the specified year.

    Parameters
    ----------
    df : pandas.DataFrame
        Processed IPC data.
    year : int
        Year to filter data for.
    severity : str, optional
        IPC phase severity to filter for.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing countries with their reference periods of peak hunger,
        sorted alphabetically by country name.
    """
    df = df.copy()
    # Filter to a specific year
    # Note that `year` is associated with the `To` date
    df_filtered = df[(df["year"] == year) & (df["Phase"] == severity)]
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
        logger.warning(
            f"Warning! {len(missing_countries)} countries do not have reports from {year}: {missing_countries}"
        )
    df_filtered["reference_year"] = year

    return (
        df_filtered[["Country", "reference_year", "reference_period"]]
        .sort_values("Country")
        .reset_index()
        .drop(columns=["index"])
    )


# TODO: Logging for cases where there aren't matches in the period
def match_peak_hunger_period(
    df: pd.DataFrame, df_peak: pd.DataFrame, year: int, severity: str
) -> pd.DataFrame:
    """
    Match data from a specific year to the peak hunger periods identified in a reference year.

    For each country, finds the data point in the specified year that overlaps with
    the identified peak hunger period from the reference year.

    Parameters
    ----------
    df : pandas.DataFrame
        Processed IPC data containing multiple years.
    df_peak : pandas.DataFrame
        DataFrame with peak hunger reference periods, as returned by identify_peak_hunger_period().
    year : int
        Year for which to extract matching data.
    severity : str
        IPC phase severity to filter for.


    Returns
    -------
    pandas.DataFrame
        DataFrame containing food insecurity data for the specified year that matches
        the peak hunger period, with columns renamed to include the year.
    """
    ref_year = int(df_peak.reference_year[0])

    # Create a new DataFrame explicitly instead of a view
    df_year = df[(df.year == year) & (df["Phase"] == severity)].copy()

    # Now use loc to set the new columns
    df_year.loc[:, f"{year}_report_period"] = df_year.apply(
        lambda x: pd.Interval(left=x["From"], right=x["To"], closed="both"), axis=1
    )

    # Set ref_period using loc
    df_year.loc[:, "ref_period"] = df_year.apply(
        lambda row: date_utils.get_ref_period(row, ref_year),
        axis=1,
    )

    df_merged = df_year.merge(df_peak)
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
