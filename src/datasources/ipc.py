import ocha_stratus as stratus
import pandas as pd
import logging
import coloredlogs
from src.config import LOG_LEVEL, PROJECT_PREFIX
from src.utils import date_utils
import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()


logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def get_ipc_from_hapi(iso3):
    endpoint = (
        "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security"
    )
    params = {
        "app_identifier": os.getenv("HAPI_APP_IDENTIFIER"),
        "location_code": iso3,
        "admin_level": 0,
        "output_format": "json",
        "limit": 10000,
        "offset": 0,
    }
    # Check if the request was successful
    response = requests.get(endpoint, params=params)
    json_data = response.json()
    # Extract the data list from the JSON
    data_list = json_data.get("data", [])
    df_response = pd.DataFrame(data_list)

    if df_response.empty:
        raise Exception(f"No data available for {iso3}")
    df_response["From"] = pd.to_datetime(df_response["reference_period_start"])
    df_response["To"] = pd.to_datetime(df_response["reference_period_end"])
    df_response["year"] = df_response["To"].dt.year
    return df_response.sort_values("reference_period_start", ascending=False)[
        [
            "location_code",
            "ipc_phase",
            "ipc_type",
            "population_in_phase",
            "population_fraction_in_phase",
            "From",
            "To",
            "year",
        ]
    ]


def get_all_ipc():
    df_raw = get_raw_ipc()
    iso3s = df_raw.Country.unique()
    dfs = []
    logger.info("Getting data for all IOS3s from HAPI...")
    for iso3 in iso3s:
        try:
            df = get_ipc_from_hapi(iso3)
            dfs.append(df)
        except Exception:
            pass
    logger.info("Data retrieved!")
    return pd.concat(dfs)


def get_raw_ipc() -> pd.DataFrame:
    """
    Retrieve raw IPC (Integrated Food Security Phase Classification) data from blob storage.

    Returns
    -------
    pandas.DataFrame
        Raw IPC data in long format
    """
    return stratus.load_csv_from_blob(
        f"{PROJECT_PREFIX}/raw/ipc_global_national_long.csv", stage="dev"
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
    last_year = datetime.now() - timedelta(days=365)
    # Note that `year` is associated with the `To` date
    df_filtered = df[(df["To"] >= last_year) & (df["ipc_phase"] == severity)]
    # Get the most recent report if there are duplicates for the same time period
    custom_order = ["current", "second_projection", "first_projection"]
    df_filtered["ipc_type"] = pd.Categorical(
        df_filtered["ipc_type"], categories=custom_order, ordered=True
    )
    df_filtered = df_filtered.sort_values("ipc_type")
    df_filtered = df_filtered.drop_duplicates(
        subset=["location_code", "From", "To"], keep="first"
    )
    # Now pick the one that has the highest food insecurity
    df_filtered = df_filtered.sort_values(
        "population_fraction_in_phase", ascending=False
    )
    df_filtered = df_filtered.drop_duplicates(subset=["location_code"], keep="first")
    # And create the reference period
    df_filtered["reference_period"] = df_filtered.apply(
        lambda x: pd.Interval(left=x["From"], right=x["To"], closed="both"), axis=1
    )
    # Check for any missing countries
    if len(set(df["location_code"])) != len(set(df_filtered["location_code"])):
        missing_countries = list(
            set(df["location_code"]) - set(df_filtered["location_code"])
        )
        logger.warning(
            f"Warning! {len(missing_countries)} countries do not have reports from {year}: {missing_countries}"
        )
    df_filtered["reference_year"] = df_filtered.reference_period.apply(
        lambda x: x.right.year
    )

    return (
        df_filtered[["location_code", "reference_year", "reference_period"]]
        .sort_values("location_code")
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
    # Create a new DataFrame explicitly instead of a view
    df_year = df[(df.year == year) & (df["ipc_phase"] == severity)].copy()

    # Now use loc to set the new columns
    df_year.loc[:, f"{year}_report_period"] = df_year.apply(
        lambda x: pd.Interval(left=x["From"], right=x["To"], closed="both"), axis=1
    )
    df_merged = df_year.merge(df_peak)

    # Set ref_period using loc
    df_merged.loc[:, "ref_period"] = df_merged.apply(
        lambda row: date_utils.get_ref_period(row),
        axis=1,
    )

    df_merged["has_overlap"] = df_merged.apply(
        lambda row: row["ref_period"].overlaps(row["reference_period"]),
        axis=1,
    )
    # Get only the ones that have overlap
    df_merged = df_merged[df_merged.has_overlap]

    # Now drop duplicate countries and get the one with the worst conditions
    df_merged = df_merged.sort_values("population_fraction_in_phase", ascending=False)
    df_merged = df_merged.drop_duplicates(subset=["location_code"], keep="first")

    # Do some basic cleaning of the columns
    df_clean = df_merged.rename(
        columns={
            "population_fraction_in_phase": f"{year}_percentage",
            "population_in_phase": f"{year}_number",
        }
    )
    df_clean = df_clean[
        [
            "location_code",
            f"{year}_report_period",
            f"{year}_number",
            f"{year}_percentage",
        ]
    ].sort_values("location_code")
    return df_clean


def add_yoy_changes(df, years):
    """
    Add year-over-year point change columns to the dataframe.

    Parameters:
    -----------
    df : pandas DataFrame
        DataFrame containing food insecurity data
    years : list
        List of three consecutive years in descending order, e.g. [2025, 2024, 2023]

    Returns:
    --------
    pandas DataFrame
        DataFrame with added YoY change columns
    """
    # Convert years to strings
    years_str = [str(year) for year in years]

    # Generate column names for YoY changes
    newer_older_pairs = [(years_str[1], years_str[2]), (years_str[0], years_str[1])]

    for newer, older in newer_older_pairs:
        col_name = f"{older}_to_{newer}_change"
        df[col_name] = round(df[f"{newer}_percentage"] - df[f"{older}_percentage"], 2)

    return df


def combine_4_plus(df_all):
    df = df_all.copy()
    mapping = {"4": "4+", "5": "4+"}
    df["ipc_phase"] = df["ipc_phase"].map(mapping).fillna(df["ipc_phase"])
    dff = df[df.ipc_phase == "4+"]
    dff = (
        dff.groupby(["From", "To", "location_code", "ipc_type", "year", "ipc_phase"])
        .agg({"population_in_phase": "sum", "population_fraction_in_phase": "sum"})
        .reset_index()
    )
    return pd.concat([df_all, dff])
