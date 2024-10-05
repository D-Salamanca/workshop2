import pandas as pd
import re


def clean_and_fill_artist(df: pd.DataFrame) -> None:
    """
    Clean and fill the DataFrame 'df' based on the defined patterns.
    
    Parameters:
        df (DataFrame): The DataFrame containing Grammy nominations information.
        
    Returns:
        DataFrame: The cleaned and filled DataFrame.
    """
    pd.set_option('future.no_silent_downcasting', True)

    patterns = [
        r'songwriters? \((.*?)\)',
        r'([^,]+), soloist',
        r'composer \((.*?)\)',
        r'arrangers? \((.*?)\)',
        r'\((.*?)\)' 
    ]

    extracted_nominees = []

    for pattern in patterns:
        extracted_nominees.append(df['workers'].str.extract(pattern, flags=re.IGNORECASE))

    df['extracted_nominee'] = pd.concat(extracted_nominees, axis=1).ffill(axis=1).iloc[:, -1]

    df['artist'] = df['artist'].fillna(df['extracted_nominee'])

    df.drop(columns=['extracted_nominee'], inplace=True)


def clean_and_fill_workers(df : pd.DataFrame) -> None:
    """
    Fill missing values in 'workers' column with corresponding values from 'artist' column
    where 'artist' has a value and 'workers' is null.

    Parameters:
        df (DataFrame): The DataFrame containing Grammy nominations information.

    Returns:
        DataFrame: The DataFrame with missing values in 'workers' filled.
    """

    mask = df['artist'].notnull() & df['workers'].isnull()

    df.loc[mask, 'workers'] = df.loc[mask, 'artist']

def rename_columns(df: pd.DataFrame, columns: dict) -> None:
    """
    Renames the columns of the DataFrame.

    Parameters:
        columns (dict): Dictionary with columns to be renamed.
    """
    df.rename(columns=columns, inplace=True)