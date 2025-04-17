import pandas as pd
from calendar import month_name


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


def apply_overlap(df, df_periods):
    df_summary = df.copy()
    df_summary["reference_period_overlap"] = 0.0
    df_summary["reference_period_months"] = df_summary["reference_period"].apply(
        lambda x: get_period_name(x)
    )
    for idx, row in df_summary.iterrows():
        iso3 = row["Country"]
        sx = set(row["reference_period_months"])
        y = df_periods[df_periods.Country == iso3]["period_long"].iloc[0]
        sy = {month.strip() for month in y.split(",")}
        overlap = get_overlap_fraction(sx, sy)
        df_summary.at[idx, "reference_period_overlap"] = overlap
    return df_summary.drop(columns=["reference_period_months"])
