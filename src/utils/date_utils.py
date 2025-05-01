import pandas as pd
from calendar import month_name
import numpy as np


def get_period_name(reference_interval):
    start_date = reference_interval.left
    end_date = reference_interval.right

    months = set()
    current = start_date

    while current <= end_date:
        months.add(current.month)
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = pd.Timestamp(year=year, month=month, day=1)

    return [month_name[m] for m in months]


# sx is dynamic and sy is the one that we reference
def get_overlap_fraction(sx, sy):
    # remove the difference between the sets
    # and get the fraction of months that are covered
    return len(sx - (sx - sy)) / len(sy)


def apply_overlap(df, df_periods, target_period_column, output_column):
    df_summary = df.copy()
    df_summary[output_column] = 0.0
    df_summary["reference_period_months"] = df_summary["reference_period"].apply(
        lambda x: get_period_name(x)
    )
    for idx, row in df_summary.iterrows():
        iso3 = row["location_code"]
        sx = set(row["reference_period_months"])
        y = df_periods[df_periods.location_code == iso3][target_period_column].iloc[0]
        if pd.notna(y):
            sy = {month.strip() for month in y.split(",")}
            overlap = get_overlap_fraction(sx, sy)
        else:
            overlap = float("nan")  # Set to NaN if y is NaN
        df_summary.at[idx, output_column] = overlap
    return df_summary.drop(columns=["reference_period_months"])


def get_ref_period(row, ref_year):
    # Account for the Jan - Dec cross
    from_year = ref_year - (1 if row["From"].month > row["To"].month else 0)
    from_date = pd.Timestamp(
        year=from_year, month=row["From"].month, day=row["From"].day
    )
    to_days = pd.Timestamp(year=ref_year, month=row["To"].month, day=1).days_in_month
    to_date = pd.Timestamp(year=ref_year, month=row["To"].month, day=to_days)

    return pd.Interval(from_date, to_date, closed="both")


def format_interval(interval):
    if pd.isna(interval):
        return np.nan
    start_date = interval.left.strftime("%b %Y")
    end_date = interval.right.strftime("%b %Y")
    return f"{start_date} to {end_date}"
