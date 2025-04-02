import pandas as pd
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import ocha_stratus as ocha

plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 18,
    'axes.labelsize': 16,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 14,
})

SEVERITY = '3+'
PROJECT_PREFIX = "ds-ufe-food-security"


def create_heatmap_single_country(df):
    # Create the heatmap
    plt.figure(figsize=(20, len(df_plot) * 0.3))
    ax = sns.heatmap(heatmap_data, cmap='viridis',
                     linewidths=0, linecolor='white',
                     cbar_kws={'label': 'People'})

    # Set labels
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_positions = [day_df[day_df['Month'] == m]['Day_of_year'].min() for m in range(1, 13)]
    month_positions = [p - 0.5 for p in month_positions]  # Adjust for proper alignment

    plt.xticks(month_positions, months)
    plt.yticks(np.arange(len(df_plot)) + 0.5, [f"{d.strftime('%Y-%m-%d')}"
                                               for d in df_plot['Date of analysis']], rotation=0)

    plt.title(f"Percentage of IPC 3+ Affected Population in {iso3}")
    plt.xlabel('Date')
    plt.ylabel('Data Analyzed')

    # Add vertical lines to separate months
    for pos in month_positions[1:]:
        plt.axvline(x=pos, color='white', linestyle='-', linewidth=0.1)

    plt.savefig(f"{iso3}_heatmap_percentage_all.png", dpi=300, bbox_inches='tight')

# Downloaded from https://data.humdata.org/dataset/global-acute-food-insecurity-country-data
df = ocha.load_csv_from_blob(f"{PROJECT_PREFIX}/ipc_global_national_long.csv", stage="dev")[1:]
df['Number'] = pd.to_numeric(df['Number'])
df['Percentage'] = pd.to_numeric(df['Percentage'])
df['Total country population'] = pd.to_numeric(df['Total country population'])

# Add more detailed date information
df['Date of analysis'] = pd.to_datetime(df['Date of analysis'], format='%b %Y')
df['From'] = pd.to_datetime(df['From'])
df['To'] = pd.to_datetime(df['To'])
df['analysis_year'] = df['Date of analysis'].dt.year
df['from_year'] = df['From'].dt.year
df['to_year'] = df['To'].dt.year
df['period_crosses_years'] = df['to_year'] != df['from_year']

df = df[df['Phase'] == SEVERITY]

# Some cases where we have the same date of analysis and projection -- keep the one with the highest projection value
df = df.sort_values('Percentage', ascending=False)
df = df.drop_duplicates(subset=['Date of analysis', 'Country', 'From', 'To', 'Validity period'], keep='first')

# # Now what if we have additional duplicates of the validity period?
# # Keep the one with the more recent Date of analysis
df = df.sort_values('Date of analysis', ascending=False)
df = df.drop_duplicates(subset=['Country', 'From', 'To'], keep='first')

for iso3 in df.Country.unique():
    df_plot = df[['Country', 'Percentage', 'From', 'To', 'to_year', 'Date of analysis']]
    df_plot = df_plot[df_plot.Country == iso3].sort_values('Date of analysis').reset_index()

    df_plot['Row_ID'] = df_plot.index
    df_plot['From_normalized'] = pd.to_datetime({
        'year': 2000,  # Use leap year to handle Feb 29
        'month': df_plot['From'].dt.month,
        'day': df_plot['From'].dt.day
    })

    df_plot['To_normalized'] = pd.to_datetime({
        'year': 2000,
        'month': df_plot['To'].dt.month,
        'day': df_plot['To'].dt.day
    })

    # Handle cases where To date is before From date (crossing year boundary)
    mask = df_plot['To_normalized'] < df_plot['From_normalized']
    df_plot.loc[mask, 'To_normalized'] = df_plot.loc[mask, 'To_normalized'] + pd.DateOffset(years=1)

    # Generate all days of the year
    all_days = pd.date_range(start='2000-01-01', end='2000-12-31')
    day_df = pd.DataFrame({
        'Date': all_days,
        'Day_of_year': all_days.dayofyear,
        'Month': all_days.month,
        'Day': all_days.day
    })

    # Create empty heatmap data matrix
    num_rows = len(df_plot)
    num_days = len(day_df)
    heatmap_data = np.empty((num_rows, num_days))
    heatmap_data[:] = np.nan

    # Fill the heatmap data
    for i, row in df_plot.iterrows():
        from_date = row['From_normalized']
        to_date = row['To_normalized']
        number = row['Percentage']

        # Handle year crossing
        if to_date.year > from_date.year:
            # Add days from from_date to end of year
            days_mask = (day_df['Date'] >= from_date) & (day_df['Date'] <= datetime(2000, 12, 31))
            heatmap_data[i, days_mask] = number

            # Add days from beginning of year to to_date
            to_date_adjusted = pd.Timestamp(2000, to_date.month, to_date.day)
            days_mask = (day_df['Date'] >= datetime(2000, 1, 1)) & (day_df['Date'] <= to_date_adjusted)
            heatmap_data[i, days_mask] = number
        else:
            # Normal case
            days_mask = (day_df['Date'] >= from_date) & (day_df['Date'] <= to_date)
            heatmap_data[i, days_mask] = number

    create_heatmap_single_country(df)


df_all_mean = pd.DataFrame()
df_all_count = pd.DataFrame()
n_countries = len(df.Country.unique())

# Transform to the correct heatmap format for monthly data
for iso3 in df.Country.unique():
    df_sel = df[df.Country == iso3]
    expanded_rows = []

    # Iterate through each row in the original DataFrame
    for index, row in df_sel.iterrows():
        from_date = row['From']
        to_date = row['To']
        percentage = row['Percentage']

        # Generate a range of months instead of days
        # Extract the year and month to create month start dates
        current_date = from_date.replace(day=1)
        end_date = to_date.replace(day=1)

        while current_date <= end_date:
            # Create a new row for this month
            new_row = {
                'Month_Date': current_date,
                'Affected_Population': percentage
                # Add any other columns you want to keep from the original row
            }

            expanded_rows.append(new_row)

            # Move to the next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

    # Create a new DataFrame from the expanded rows
    df_monthly = pd.DataFrame(expanded_rows).sort_values('Month_Date')

    # Set a common year (2000) for all months to align them in the plot
    df_monthly['Year'] = 2000
    df_monthly['Month'] = df_monthly['Month_Date'].dt.month
    df_monthly['Month_plot'] = pd.to_datetime({'year': 2000, 'month': df_monthly['Month'], 'day': 1})

    # Combine the values for multiple months
    df_agg = df_monthly.groupby('Month_plot')['Affected_Population'].mean().reset_index()
    df_agg['Country'] = iso3

    # Now get the count per month
    df_count = df_monthly.groupby('Month_plot')['Affected_Population'].count().reset_index()
    df_count['Country'] = iso3

    df_all_mean = pd.concat([df_all_mean, df_agg])
    df_all_count = pd.concat([df_all_count, df_count])

# For months without data, we'll have NaN values
pivot_df_mean = df_all_mean.pivot_table(
    index='Country',
    columns='Month_plot',
    values='Affected_Population',
    aggfunc='mean'  # Use mean if multiple values exist for same country/month
)
pivot_df_count = df_all_count.pivot_table(
    index='Country',
    columns='Month_plot',
    values='Affected_Population',
    aggfunc='mean'  # Use mean if multiple values exist for same country/month
)

heatmap_data = pivot_df_mean
heatmap_data_count = pivot_df_count

# Now try computing the z-score
z_score_heatmap = heatmap_data.copy()
for iso3 in df.Country.unique():
    country_mask = z_score_heatmap.index == iso3
    country_mean = z_score_heatmap.loc[country_mask].mean(axis=1).values[0]
    country_std = z_score_heatmap.loc[country_mask].std(axis=1).values[0]
    if country_std > 0:  # Avoid division by zero
        z_score_heatmap.loc[country_mask] = (z_score_heatmap.loc[country_mask] - country_mean) / country_std

def create_heatmap(heatmap_data, color_map, colorbar_label, title, output_filename, vmin, vmax, annot=False, center=0):
    plt.figure(figsize=(18, len(df['Country'].unique()) * 0.5))

    # Create the heatmap with diverging colors
    ax = sns.heatmap(
        heatmap_data,
        cmap=color_map,
        vmin=vmin,
        vmax=vmax,
        center=center,
        cbar_kws={'label': colorbar_label},
        annot=annot
    )

    # Format x-axis with month names
    month_labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    plt.xticks(np.arange(len(month_labels)) + 0.5, month_labels)
    plt.yticks(rotation=0)

    # Add vertical lines between months (optional)
    for i in range(1, 12):
        plt.axvline(x=i, color='white', linestyle='-', linewidth=0.5)

    plt.title(title)
    plt.tight_layout()

    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.show()

# create_heatmap(
#     heatmap_data=z_score_heatmap,
#     color_map='RdBu_r',
#     colorbar_label='Z-Score (Standard Deviations from Mean)',
#     title='Z-Score of Average IPC3+ Proportion of Population by Country',
#     output_filename='heatmap_zscore_all_drop_dups.png',
#     vmin=-3,
#     vmax=4,
#     center=0,
#     annot=True
# )
THRESH = 0.5

# create_heatmap(
#     heatmap_data=z_score_heatmap[z_score_heatmap > THRESH],
#     color_map='RdBu_r',
#     colorbar_label='Z-Score (Standard Deviations from Mean)',
#     title=f"Z-Score of Average IPC3+ Proportion of Population by Country -- Above {THRESH}",
#     output_filename='heatmap_zscore_thresh_drop_dups.png',
#     vmin=-3,
#     vmax=4,
#     center=0,
#     annot=True
# )
#
# create_heatmap(
#     heatmap_data=heatmap_data_count,
#     color_map='Blues',
#     colorbar_label='Number of Reports',
#     title='Number of IPC Reports per Month, per Country',
#     output_filename='heatmap_report_count.png',
#     vmin=0,
#     vmax=15,
#     center=7.5,
#     annot=True
# )

threshold = THRESH
df = z_score_heatmap

results = []
dates = df.columns.tolist()


def get_period_abbrev(start_idx, end_idx, dates_list, is_circular):
    n = len(dates_list)
    month_letters = []

    if is_circular and end_idx < start_idx:
        # Handle circular period (e.g., November to February)
        indices = list(range(start_idx, n)) + list(range(0, end_idx + 1))
    else:
        # Handle normal period (e.g., June to August)
        indices = list(range(start_idx, end_idx + 1))

    for i in indices:
        month_letter = dates_list[i].strftime('%B')[0]
        if month_letter:
            month_letters.append(month_letter)

    return ''.join(month_letters)

for country in df.index:
    values = df.loc[country].values
    mask = np.where(np.isnan(values), 0, values > threshold).astype(int)

    if np.all(mask == 0):
        # No values above threshold
        results.append({
            'Country': country,
            'Max_Streak': 0,
            'Start_Date': None,
            'End_Date': None,
            'Max_Value': None,
            'Is_Circular': False
        })
        continue

    # For circular data, we duplicate the array to find streaks that wrap around
    extended_mask = np.concatenate([mask, mask])
    extended_values = np.concatenate([values, values])

    # Find all sequences of 1s
    extended_starts = np.where(np.concatenate(([0], extended_mask[:-1])) - extended_mask < 0)[0]
    extended_ends = np.where(extended_mask - np.concatenate((extended_mask[1:], [0])) > 0)[0]
    extended_lengths = extended_ends - extended_starts + 1
    max_length = np.max(extended_lengths)

    # Get all streaks that have the maximum length
    max_length_indices = np.where(extended_lengths == max_length)[0]

    # For each max-length streak, find the maximum z-score
    max_zscores = []
    for idx in max_length_indices:
        start_pos = extended_starts[idx]
        end_pos = extended_ends[idx]
        streak_values = extended_values[start_pos:end_pos+1]
        # Filter out NaN values when finding max
        valid_values = streak_values[~np.isnan(streak_values)]
        max_zscore = np.max(valid_values) if len(valid_values) > 0 else np.nan
        max_zscores.append(max_zscore)

    # Find the index of the streak with the highest max z-score
    if len(max_zscores) > 0:
        best_streak_idx = max_length_indices[np.nanargmax(max_zscores)]
        start_idx = extended_starts[best_streak_idx]
        end_idx = extended_ends[best_streak_idx]
        best_max_value = max_zscores[np.nanargmax(max_zscores)]

        # Adjust indices for the circular nature
        n = len(mask)
        start_date_idx = start_idx % n
        end_date_idx = end_idx % n
        is_circular = start_idx // n != end_idx // n

        period_abbrev = get_period_abbrev(start_date_idx, end_date_idx, dates, is_circular)

        results.append({
            'Country': country,
            'Num_Months': max_length,
            'Start_Month': dates[start_date_idx].strftime('%B'),
            'End_Month': dates[end_date_idx].strftime('%B'),
            'Period': period_abbrev,
        })

peak_lean_season_summary = pd.DataFrame(results)
ocha.upload_csv_to_blob(peak_lean_season_summary, f"{PROJECT_PREFIX}/peak_lean_season_summary.csv")
