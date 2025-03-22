import pandas as pd
import ocha_stratus as ocha
from datetime import datetime, timedelta

from src.utils.data_wrangling import standardize_data

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

# Filter data for only values regarding the last year or future (should future be included?)
filtered_df = df[(x_one_year <= df["From"]) | ((df["From"] <= x_one_year) & (x_one_year <= df["To"]))]

# Group by Country and Validity Period, get the row with the max Percentage
grouped_df = filtered_df.loc[filtered_df.groupby(["Country", "Validity period"])['Percentage'].idxmax()]
# NOTE: which is the criteria here for selecting?
grouped_df = grouped_df.sort_values('Percentage', ascending=False)
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
final_df["Difference"] = final_df["Number"] - final_df["Number_1ago"]


# Print some info

for ii in range(0, len(final_df)):
    print(f"In the last year, for {final_df.loc[ii, 'Country' ]}, the highest percentage of people in IPC 3+ phase was {final_df.loc[ii, 'Percentage' ]} \n"
          f"which corresponds to {final_df.loc[ii, 'Number' ]} people. This figure is {final_df.loc[ii, 'Validity period' ]} for the period from {final_df.loc[ii, 'From' ]} to {final_df.loc[ii, 'To' ]}." )
    print(f"The previous year the corresponding number was  {final_df.loc[ii, 'Number_1ago' ]}")
    if final_df.loc[ii, "Difference"]>0:
        print(f"The number of people in IPC 3+ phase increased of { final_df.loc[ii, "Difference"]} people")
    else:
        print(f"The number of people in IPC 3+ phase decreased of {-final_df.loc[ii, "Difference"]} people")
    print("-----")