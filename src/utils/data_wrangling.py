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