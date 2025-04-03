---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.3
  kernelspec:
    display_name: nga-flooding
    language: python
    name: nga-flooding
---

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

## Comparing with the ACAPS Seasonal Calendar

This notebook outputs an updated version of the estimated peak lean season per country. This version of the file includes reference to the seasonal calendar from ACAPS.

Downloaded from: https://www.acaps.org/en/thematics/all-topics/seasonal-calendar

```python
import pandas as pd
import ocha_stratus as stratus
import ast
from calendar import month_abbr, month_name

PROJECT_PREFIX = "ds-ufe-food-security"

df_acaps = stratus.load_csv_from_blob(
    f"{PROJECT_PREFIX}/seasonal-events-calendar-acaps.csv", stage="dev", sep=";"
)
df_peak_hunger = stratus.load_csv_from_blob(
    f"{PROJECT_PREFIX}/peak_lean_season_summary.csv", stage="dev"
)
```

```python
df_acaps_filt = (
    df_acaps[
        (df_acaps["country_wide"] == True) & (df_acaps["event"] == "['Lean season']")
    ]
    .assign(
        iso=lambda df: df["iso"].str.strip("[]'\""),
        months=lambda df: df["months"].apply(
            lambda x: ", ".join(ast.literal_eval(x) if isinstance(x, str) else x)
        ),
    )
    .drop(
        columns=[
            "id",
            "country",
            "adm1_eng_name",
            "adm1",
            "event",
            "event_type",
            "label",
            "source_link",
            "source_date",
            "country_wide",
            "source",
        ]
    )
    .sort_values("iso")
)
```

```python
# Step 1: Create a period_long column in df_peak_hunger
def expand_period_to_full_months(row):
    start_month = row["Start_Month"]
    num_months = row["Num_Months"]

    # Get month index (1-12)
    month_indices = {}
    for i, name in enumerate(month_name):
        if i > 0:  # Skip the empty string at index 0
            month_indices[name] = i

    start_idx = month_indices[start_month]

    # Generate list of consecutive months
    months = []
    current_idx = start_idx
    for _ in range(num_months):
        months.append(month_name[current_idx])
        current_idx = current_idx % 12 + 1  # Wrap around to January after December

    return ", ".join(months)


# Apply the function to create period_long
df_peak_hunger["period_long"] = df_peak_hunger.apply(
    expand_period_to_full_months, axis=1
)

# Step 2: Handle multiple rows in df_acaps_filt and join with df_peak_hunger
# First, create a suffix for duplicate iso3 entries
df_acaps_filt["row_num"] = df_acaps_filt.groupby("iso").cumcount() + 1

# Reshape the dataframe to have one row per iso3
df_acaps_pivot = pd.pivot_table(
    df_acaps_filt,
    index="iso",
    columns="row_num",
    values=["months", "comment"],
    aggfunc="first",
).reset_index()

# Flatten the column names
df_acaps_pivot.columns = [
    f"{col[0]}_{col[1]}" if col[1] != "" else col[0] for col in df_acaps_pivot.columns
]

# If there's only one entry for an iso3, rename columns without suffix
if "months_1" in df_acaps_pivot.columns and "months_2" not in df_acaps_pivot.columns:
    rename_cols = {
        col: col.replace("_1", "")
        for col in df_acaps_pivot.columns
        if col.endswith("_1")
    }
    df_acaps_pivot = df_acaps_pivot.rename(columns=rename_cols)

# Step 3: Merge the dataframes
df_merged = pd.merge(
    df_peak_hunger, df_acaps_pivot, left_on="Country", right_on="iso", how="left"
)

# Clean up by dropping the duplicate iso column
if "iso" in df_merged.columns:
    df_merged = df_merged.drop(columns=["iso"])
```

```python
def calculate_month_overlap(row):
    # Check if both months_1 and months_2 are NA
    if pd.isna(row["months_1"]) and pd.isna(row["months_2"]):
        return None

    # Get list of months from period_long
    period_months = (
        set(row["period_long"].split(", ")) if pd.notna(row["period_long"]) else set()
    )

    # Get months from months_1 and months_2
    months_1 = set(row["months_1"].split(", ")) if pd.notna(row["months_1"]) else set()
    months_2 = set(row["months_2"].split(", ")) if pd.notna(row["months_2"]) else set()

    # Calculate overlaps
    overlap_1 = len(period_months.intersection(months_1))
    overlap_2 = len(period_months.intersection(months_2))

    # Return maximum overlap
    return max(overlap_1, overlap_2, 0)  # Using 0 if there's data but no overlap
```

```python
df_merged["months_overlap"] = df_merged.apply(calculate_month_overlap, axis=1)
df_merged["overlap_pct"] = df_merged["months_overlap"] / df_merged["Num_Months"] * 100

df_merged = df_merged.rename(
    columns={
        "comment_1": "acaps_comment_p1",
        "comment_2": "acaps_comment_p2",
        "months_1": "p1",
        "months_2": "p2",
    }
)
```

```python
df_merged
```

```python
stratus.upload_csv_to_blob(
    df_merged, blob_name=f"{PROJECT_PREFIX}/peak_lean_season_summary_w_acaps.csv"
)
```
