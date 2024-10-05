import pandas as pd  # Importar pandas
import re  # Importar expresiones regulares

def clean_and_fill_artist(df: pd.DataFrame) -> None:
    """
    Limpiar y completar el DataFrame 'df' basado en los patrones definidos.
    
    Parámetros:
        df (DataFrame): El DataFrame que contiene la información de las nominaciones de los Grammy.
        
    Returns:
        None
    """
    pd.set_option('future.no_silent_downcasting', True)  # Opción de pandas

    patterns = [
        r'songwriters? \((.*?)\)',  # Patrón para extraer compositores
        r'([^,]+), soloist',  # Patrón para solistas
        r'composer \((.*?)\)',  # Patrón para compositores
        r'arrangers? \((.*?)\)',  # Patrón para arreglistas
        r'\((.*?)\)'  # Patrón general
    ]

    extracted_nominees = []  # Lista para almacenar los nominados extraídos

    # Extraer nominados según patrones
    for pattern in patterns:
        extracted_nominees.append(df['workers'].str.extract(pattern, flags=re.IGNORECASE))

    df['extracted_nominee'] = pd.concat(extracted_nominees, axis=1).ffill(axis=1).iloc[:, -1]  # Combinar resultados

    # Completar los artistas
    df['artist'] = df['artist'].fillna(df['extracted_nominee'])

    df.drop(columns=['extracted_nominee'], inplace=True)  # Eliminar la columna temporal

def clean_and_fill_workers(df: pd.DataFrame) -> None:
    """
    Completar valores faltantes en la columna 'workers' con los valores correspondientes de la columna 'artist'
    donde 'artist' tiene un valor y 'workers' es nulo.

    Parámetros:
        df (DataFrame): El DataFrame que contiene la información de las nominaciones de los Grammy.

    Returns:
        None
    """
    mask = df['artist'].notnull() & df['workers'].isnull()  # Máscara para seleccionar filas

    df.loc[mask, 'workers'] = df.loc[mask, 'artist']  # Completar valores en 'workers'

def rename_columns(df: pd.DataFrame, columns: dict) -> None:
    """
    Renombrar las columnas del DataFrame.

    Parámetros:
        columns (dict): Diccionario con columnas a renombrar.
    
    Returns:
        None
    """
    df.rename(columns=columns, inplace=True)  # Renombrar columnas