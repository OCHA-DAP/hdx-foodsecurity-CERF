import pandas as pd
import ocha_stratus as ocha
from datetime import datetime, timedelta

from src.utils.data_wrangling import standardize_data, calculate_overlap, print_info_single_country

SEVERITY = '3+'
PROJECT_PREFIX = "ds-ufe-food-security"

# Set the current date
x = datetime.now()

# Find the date of a year ago
x_one_year = x - timedelta(days=365)

# Downloaded from https://data.humdata.org/dataset/global-acute-food-insecurity-country-data
df = ocha.load_csv_from_blob(f"{PROJECT_PREFIX}/ipc_global_national_long.csv", stage="dev")[1:]
df = standardize_data(df)
df = df[df['Phase'] == SEVERITY]

# Filter data for only values regarding the last year or future
filtered_df = df[(x_one_year <= df["From"]) | ((df["From"] <= x_one_year) & (x_one_year <= df["To"]))]

# If same period present from multiple report, get more recent
filtered_df = filtered_df.sort_values(['Date of analysis'], ascending=False)
filtered_df = filtered_df.drop_duplicates(subset=['Country', "From", "To"], keep='first')
# NOTE: which is the criteria here for selecting?
grouped_df = filtered_df.sort_values('Percentage', ascending=False)
grouped_df = grouped_df.drop_duplicates(subset=['Country'], keep='first')

grouped_df["From_one_year_ago"] = pd.to_datetime(grouped_df["From"] - pd.DateOffset(years=1))
grouped_df["To_one_year_ago"] = pd.to_datetime(grouped_df["To"] - pd.DateOffset(years=1))
grouped_df.reset_index(inplace=True, drop=True)

# Create empty dataframe
df_one_year_ago = pd.DataFrame(columns=df.columns.tolist())

# Iterate over each country and find all the relevant reports
for ii in range(0, len(grouped_df)):
    country = grouped_df["Country"][ii]
    date_from = grouped_df.loc[ii,"From_one_year_ago"]
    date_to = grouped_df.loc[ii, "To_one_year_ago"]
    time_condition =  (((df["From"] <= date_from) & (df["To"] >= date_to)) |
                       ((df["From"] >= date_from) & (df["To"] < date_to)) |
                       ((df["From"] >= date_from) & (df["To"] > date_to)& (df["From"] < date_to))|
                       ((df["From"] < date_from) & (df["To"] <= date_to)& (df["To"] > date_from)))
    to_append = df[((df["Country"]==country) & (time_condition))]
    df_one_year_ago = pd.concat([df_one_year_ago, to_append], ignore_index=True)

df_one_year_ago = df_one_year_ago.sort_values('Percentage', ascending=False)

# NOTE: which is the criteria here for filtering? and what hierarchy?
df_one_year_ago_filtered = df_one_year_ago.drop_duplicates(subset=['Country'], keep='first')

# Merge dfs for final results
final_df = grouped_df.merge(df_one_year_ago_filtered, how='left', on='Country', suffixes=("","_1ago"))
final_df["Difference_num_people_previous_year"] = final_df["Number"] - final_df["Number_1ago"]

calendar = ocha.load_csv_from_blob(f"{PROJECT_PREFIX}/peak_lean_season_summary.csv")[1:]
final_df = final_df.merge(calendar[["Country", "Start_Month", "End_Month"]], how='left', on='Country')
final_df = final_df.rename(columns={'Start_Month': 'From_historical', 'End_Month': 'To_historical'})
#
# # Print some info

#print_info_single_country(df=final_df, iso3="AFG")
# Convert dates to datetime
final_df["From"] = pd.to_datetime(final_df["From"])
final_df["To"] = pd.to_datetime(final_df["To"])

# Map month names to month numbers
month_name_to_number = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}

final_df["From_historical"] = final_df["From_historical"].map(month_name_to_number)
final_df["To_historical"] = final_df["To_historical"].map(month_name_to_number)

# Calculate total period in days
final_df["Total days"] = (final_df["To"] - final_df["From"]).dt.days

# Apply overlap calculation
final_df["Overlap days"] = final_df.apply(calculate_overlap, axis=1)

# Calculate percentage
final_df["Overlap months"] =  final_df["Overlap days"]// 30
final_df["Total months"] =  final_df["Total days"]// 30
final_df["Overlap percentage"] = (final_df["Overlap months"] / final_df["Total months"]) * 100
final_df.drop(["Overlap days", "Total days"], axis=1, inplace=True)
final_df.to_csv(f"peak_hunger_period_{x.strftime("%Y%m%d")}.csv")