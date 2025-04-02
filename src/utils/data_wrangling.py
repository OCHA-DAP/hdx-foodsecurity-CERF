import pandas as pd

def standardize_data(df):
    """

    Args:
        df:

    Returns:

    """
    df['Number'] = pd.to_numeric(df['Number'])
    df['Percentage'] = pd.to_numeric(df['Percentage'])
    df['Total country population'] = pd.to_numeric(df['Total country population'])

    # Add more detailed date information
    df['Date of analysis'] = pd.to_datetime(df['Date of analysis'], format='%b %Y')
    df['From'] = pd.to_datetime(df['From'])
    df['To'] = pd.to_datetime(df['To'])
    return df


def print_info_single_country(df: pd.DataFrame, iso3: str):
    row=df[df['Country'] == iso3]
    print(
        f"In the last year, for {iso3}, the highest percentage of people in IPC 3+ phase was {row['Percentage']} \n"
               f"which corresponds to {row['Number' ]} people. This figure is {row['Validity period' ]} for the period from {row['From' ]} to {row['To' ]}." )
    print(f"The previous year the corresponding number was  {row['Number_1ago' ]}")
    if row["Difference_num_people_previous_year"]>0:
         print(f"The number of people in IPC 3+ phase increased of { row["Difference_num_people_previous_year"]} people")
    else:
         print(f"The number of people in IPC 3+ phase decreased of {row["Difference_num_people_previous_year"]} people")


# Function to calculate overlap in days
def calculate_overlap(row):
    """

    Args:
        row:

    Returns:

    """
    if not pd.isna(row["From_historical"]):
        # Extract periods
        start_date = row["From"]
        end_date = row["To"]

        # Historical period months (fixed within the same year)
        historical_start_date = pd.Timestamp(year=start_date.year, month=int(row["From_historical"]), day=1)
        historical_end_date = pd.Timestamp(year=start_date.year, month=int(row["To_historical"]), day=1) + pd.offsets.MonthEnd(0)

        # Adjust historical range to wrap the year if necessary
        if historical_start_date > historical_end_date:
            historical_end_date = historical_end_date + pd.offsets.DateOffset(years=1)

        # Calculate intersection of periods
        overlap_start = max(start_date, historical_start_date)
        overlap_end = min(end_date, historical_end_date)

        # Calculate overlap days
        overlap_days = max(0, (overlap_end - overlap_start).days)
    else:
        overlap_days = 0
    return overlap_days