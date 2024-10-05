from transformations.transformations import *
from src.models.database_models import GrammyAwards
from src.models.database_models import SongsData
from src.database.db_connection import get_engine
from sqlalchemy import inspect, Table, MetaData, insert, select
import logging as log
import json
import pandas as pd  # Asegúrate de importar pandas

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

log.basicConfig(level=log.INFO)

def grammy_process() -> json:
    """
    Process the Grammy nominations data.

    Returns:
        JSON: The JSON representation of the DataFrame containing the Grammy nominations data.
    """

    connection = get_engine()

    try:
        if inspect(connection).has_table('grammy_awards'):
            GrammyAwards.__table__.drop(connection)
            log.info("Table dropped successfully.")
        
        GrammyAwards.__table__.create(connection)
        log.info("Table created successfully.")

        transformations = transformations('D-Salamanca/workshop2/data/spotify_dataset.csv')
        transformations.insert_id()
        log.info("Data transformed successfully.")

        df = transformations.df

        metadata = MetaData()
        table = Table('grammy_awards', metadata, autoload=True, autoload_with=connection)

        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]

            conn.execute(insert(table), values)
        
        log.info('Data loaded successfully')

        log.info('Starting query')

        select_stmt = select([table])

        result_proxy = connection.execute(select_stmt)
        results = result_proxy.fetchall()

        column_names = table.columns.keys()
        df_2 = pd.DataFrame(results, columns=column_names)
        return df_2.to_json(orient='records')

    except Exception as e:
        log.error(f"Error processing data: {e}")

def transform_grammys_data(json_data: json) -> json:
    """
    Transform the Grammy nominations data.
    
    Parameters:
        json_data (JSON): The JSON representation of the DataFrame containing the Grammy nominations data.
        
    Returns:
        json_data (JSON): The JSON representation of the transformed DataFrame.
    """
    
    # Cargar los datos JSON a DataFrames
    json_data = json.loads(json_data)
    df_grammy = pd.DataFrame(json_data)

    log.info('Starting Grammy transformations...')
    
    # 1. Eliminar las columnas innecesarias en el dataset de los Grammy
    columns_to_drop_grammy = ['published_at', 'updated_at', 'img']
    df_grammy_cleaned = df_grammy.drop(columns=columns_to_drop_grammy)

    # 2. Limpiar los nombres de los artistas y nominados
    df_grammy_cleaned['artist'] = df_grammy_cleaned['artist'].str.lower().str.strip()
    df_grammy_cleaned['nominee'] = df_grammy_cleaned['nominee'].str.lower().str.strip()

    log.info('Grammy_data limpio.')

    return df_grammy_cleaned.to_json(orient='records')


def read_spotify_data(file_path: str) -> json:
    """
    Read the Spotify data from the given file path.
    
    Parameters:
        file_path (str): The file path to the Spotify data.
        
    Returns:
        json (JSON): The JSON representation of the DataFrame containing the Spotify data.
    """
    df = pd.read_csv(file_path)

    log.info('Spotify data read successfully!')
    return df.to_json(orient='records')


def transform_spotify_data(json_data: json) -> json:
    """
    Transform the Spotify data.
    
    Parameters:
        json_data (JSON): The JSON representation of the DataFrame containing the Spotify data.      

    Returns:
        json (JSON): The JSON representation of the transformed DataFrame.
    """
    
    json_data = json.loads(json_data)
    df_spotify = pd.DataFrame(json_data)

    log.info('Starting Spotify transformations...')

    # 1. Eliminar las columnas innecesarias en el dataset de Spotify
    columns_to_drop_spotify = ['danceability', 'energy', 'key', 'loudness', 
                                'mode', 'acousticness', 'liveness', 'tempo', 'time_signature']
    df_spotify_cleaned = df_spotify.drop(columns=columns_to_drop_spotify)

    # 2. Limpiar los nombres de los artistas y las canciones
    df_spotify_cleaned['artists'] = df_spotify_cleaned['artists'].str.lower().str.strip()
    df_spotify_cleaned['track_name'] = df_spotify_cleaned['track_name'].str.lower().str.strip()

    log.info('Spotify data limpio.')
    
    return df_spotify_cleaned.to_json(orient='records')


def merge_datasets(json_data1: json, json_data2: json) -> json:
    """
    Merge the two datasets.
    
    Parameters:
        json_data1 (JSON): The first dataset. Grammy Awards   
        json_data2 (JSON): The second dataset. Spotify
        
    Returns:
        df_merged (JSON): The merged dataset.
    """

    json_data1 = json.loads(json_data1)
    grammy_df_cleaned = pd.DataFrame(json_data1)

    json_data2 = json.loads(json_data2)
    spotify_df_cleaned = pd.DataFrame(json_data2)

    log.info('Merging datasets...')

    # 4. Realizar el merge "right", manteniendo las filas de Spotify
    merged_df_right = pd.merge(grammy_df_cleaned, spotify_df_cleaned, 
                                left_on=['artist', 'nominee'], 
                                right_on=['artists', 'track_name'], 
                                how='right')

    # 5. Reemplazar los valores NaN en la columna 'winner' por False
    merged_df_right['winner'].fillna(False, inplace=True)

    # 6. Contar cuántas canciones tienen 'True' en la columna 'winner'
    num_true_winners = merged_df_right[merged_df_right['winner'] == True].shape[0]
    log.info(f'Number of true winners: {num_true_winners}')

    # 7. Verificar el número total de filas en el dataset combinado
    num_total_rows = merged_df_right.shape[0]
    log.info(f'Total rows in merged dataset: {num_total_rows}')

    log.info('Datasets merged realizado!')
    
    return merged_df_right.to_json(orient='records')


def load_merge(json_data: json) -> json:
    """
    Load the merged dataset to the database.

    Parameters:
        json_data (JSON): The JSON representation of the merged dataset.
    
    Returns:
        json (JSON): The JSON representation of the DataFrame containing the merged dataset.
    """

    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    df.insert(0, 'id', df.index + 1)

    connection = get_engine()

    try:
        if inspect(connection).has_table('songs_data'):
            SongsData.__table__.drop(connection)
            log.info("Tabla dropped correctamente.")
        
        SongsData.__table__.create(connection)
        log.info("Tabla created correctamente.")

        metadata = MetaData()
        table = Table('songs_data', metadata, autoload=True, autoload_with=connection)

        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]

            conn.execute(insert(table), values)

        log.info('Data subida correctamente')

        return df.to_json(orient='records')
    
    except Exception as e:
        log.error(f"Error processing data: {e}")

CREDENTIALS_PATH = 'credentials_module.json'

def login() -> GoogleDrive:
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(CREDENTIALS_PATH)

    if gauth.credentials is None:
        gauth.Refresh()
        gauth.SaveCredentialsFile(CREDENTIALS_PATH)
    else:
        gauth.Authorize()
    return GoogleDrive(gauth)

def load_dataset_to_drive(json_data: json, title: str, folder_id: str) -> None:
    """
    Load the dataset to Google Drive.
    
    Parameters:
        json_data (JSON): The DataFrame to be uploaded.
        title (str): The title of the file.
        folder_id (str): The folder ID to load the dataset to.
        
    Returns:
        None
    """

    json_data = json.loads(json_data)
    df = pd.DataFrame(json_data)

    drive = login()

    csv_string = df.to_csv(index=False)

    file = drive.CreateFile({'title': title,
                             'parents': [{'kind': 'drive#fileLink', 'id': folder_id}],
                             'mimeType': 'text/csv'})
    
    file.SetContentString(csv_string)
    file.Upload()

    log.info('Dataset subido correctamente a Google Drive!')
