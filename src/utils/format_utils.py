def clean_columns(df_summary):
    change_cols = [col for col in df_summary.columns if "_change" in col][::-1]
    percentage_cols = [col for col in df_summary.columns if "_percentage" in col]
    number_cols = [col for col in df_summary.columns if "_number" in col]
    overlap_cols = [col for col in df_summary.columns if "_overlap" in col]
    period_cols = [col for col in df_summary.columns if "report_period" in col]

    final_cols = (
        ["location_code", "reference_year", "phase", "reference_period"]
        + change_cols
        + percentage_cols
        + number_cols
        + period_cols
        + overlap_cols
    )
    df_formatted = df_summary[final_cols].rename(
        columns={
            "location_code": "Country",
            "reference_period": "Peak Hunger Period",
        }
    )
    df_formatted = df_formatted.rename(
        columns={col: format_column(col) for col in df_formatted.columns}
    )
    return df_formatted


def format_year_change(column_name):
    """
    Convert year change columns like '2023_2024_change' to '2023 to 2024 change'
    """
    if "_change" in column_name and "_" in column_name:
        parts = column_name.split("_")
        if len(parts) >= 3 and parts[-1] == "change":
            if (
                parts[0].isdigit()
                and len(parts[0]) == 4
                and parts[1].isdigit()
                and len(parts[1]) == 4
            ):
                return f"{parts[0]} to {parts[1]} Change"
    return column_name


def format_column(col):
    return " ".join(
        word.capitalize() if i > 0 or not word.isdigit() else word
        for i, word in enumerate(col.split("_"))
    )
