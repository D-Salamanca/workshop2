from task.transformation_csv import Transformations  # Importar la clase Transformations
from task.transformation_db import clean_and_fill_artist, clean_and_fill_workers, rename_columns  # Importar funciones necesarias
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
    Procesa los datos de las nominaciones de los Grammy.

    Returns:
        JSON: La representación JSON del DataFrame que contiene los datos de nominaciones de Grammy.
    """
    connection = get_engine()

    try:
        # Eliminar la tabla si existe y crearla nuevamente
        if inspect(connection).has_table('grammy_awards'):
            GrammyAwards.__table__.drop(connection)
            log.info("Tabla 'grammy_awards' eliminada correctamente.")
        
        GrammyAwards.__table__.create(connection)
        log.info("Tabla 'grammy_awards' creada correctamente.")

        # Transformar los datos de Grammy
        transformations = Transformations('data/the_grammy_awards.csv')  # Ruta del archivo CSV
        transformations.insert_id()  # Insertar ID en el DataFrame
        log.info("Datos transformados correctamente.")

        df = transformations.df  # Obtener el DataFrame transformado

        # Limpiar los datos
        clean_and_fill_artist(df)
        clean_and_fill_workers(df)

        # Cargar datos en la base de datos
        metadata = MetaData()
        table = Table('grammy_awards', metadata, autoload=True, autoload_with=connection)

        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]
            conn.execute(insert(table), values)
        
        log.info('Datos de Grammy cargados correctamente.')

        # Consultar los datos para verificar
        select_stmt = select([table])
        result_proxy = connection.execute(select_stmt)
        results = result_proxy.fetchall()

        column_names = table.columns.keys()
        df_2 = pd.DataFrame(results, columns=column_names)
        return df_2.to_json(orient='records')

    except Exception as e:
        log.error(f"Error al procesar los datos de Grammy: {e}")

def transform_grammys_data(json_data: json) -> json:
    """
    Transformar los datos de nominaciones de Grammy.
    
    Parameters:
        json_data (JSON): La representación JSON del DataFrame que contiene los datos de nominaciones de Grammy.
        
    Returns:
        json_data (JSON): La representación JSON del DataFrame transformado.
    """
    
    json_data = json.loads(json_data)  # Cargar los datos JSON a DataFrames
    df_grammy = pd.DataFrame(json_data)

    log.info('Iniciando transformaciones de Grammy...')
    
    # 1. Eliminar columnas innecesarias
    columns_to_drop_grammy = ['published_at', 'updated_at', 'img']
    df_grammy_cleaned = df_grammy.drop(columns=columns_to_drop_grammy)

    # 2. Limpiar los nombres de los artistas y nominados
    clean_and_fill_artist(df_grammy_cleaned)
    clean_and_fill_workers(df_grammy_cleaned)

    # 3. Eliminar valores nulos
    remove_nulls = ['nominee', 'workers', 'artist']
    drop_null_values(df_grammy_cleaned, remove_nulls)  # type: ignore # Asegúrate de que esta función esté implementada correctamente

    # 4. Renombrar columnas
    rename_columns(df_grammy_cleaned, {'winner': 'nominee_status'})

    log.info('Transformaciones de Grammy completadas.')
    return df_grammy_cleaned.to_json(orient='records')

def read_spotify_data(file_path: str = 'data/spotify_dataset.csv') -> json:
    """
    Leer los datos de Spotify desde la ruta dada.
    
    Parameters:
        file_path (str): La ruta del archivo de datos de Spotify (por defecto es 'data/spotify_dataset.csv').
        
    Returns:
        json (JSON): La representación JSON del DataFrame que contiene los datos de Spotify.
    """
    try:
        transformations = Transformations(file_path)  # Usar la clase Transformations para cargar los datos
        transformations.insert_id()  # Insertar ID
        
        log.info('¡Datos de Spotify leídos y transformados correctamente!')

        if transformations.df.empty:
            log.warning('El DataFrame de Spotify está vacío después de la carga.')
        
        return transformations.df.to_json(orient='records')  # Devolver JSON
    except Exception as e:
        log.error(f"Error al leer los datos de Spotify: {e}")
        return json.dumps([])  # Retornar un JSON vacío en caso de error


def transform_spotify_data(json_data: json) -> json:
    """
    Transformar los datos de Spotify.
    
    Parameters:
        json_data (JSON): La representación JSON del DataFrame que contiene los datos de Spotify.      

    Returns:
        json (JSON): La representación JSON del DataFrame transformado.
    """
    
    json_data = json.loads(json_data)
    df_spotify = pd.DataFrame(json_data)

    log.info('Iniciando transformaciones de Spotify...')

    # 1. Eliminar columnas innecesarias
    columns_to_drop_spotify = ['danceability', 'energy', 'key', 'loudness', 
                                'mode', 'acousticness', 'liveness', 'tempo', 'time_signature']
    df_spotify_cleaned = df_spotify.drop(columns=columns_to_drop_spotify)

    # 2. Limpiar los nombres de los artistas y canciones
    df_spotify_cleaned['artists'] = df_spotify_cleaned['artists'].str.lower().str.strip()
    df_spotify_cleaned['track_name'] = df_spotify_cleaned['track_name'].str.lower().str.strip()

    log.info('Datos de Spotify limpios.')
    
    return df_spotify_cleaned.to_json(orient='records')

def merge_datasets(json_data1: json, json_data2: json) -> json:
    """
    Mezclar los dos conjuntos de datos.
    
    Parameters:
        json_data1 (JSON): El primer conjunto de datos. Grammy Awards   
        json_data2 (JSON): El segundo conjunto de datos. Spotify
        
    Returns:
        df_merged (JSON): El conjunto de datos combinado.
    """
    
    json_data1 = json.loads(json_data1)
    grammy_df_cleaned = pd.DataFrame(json_data1)

    json_data2 = json.loads(json_data2)
    spotify_df_cleaned = pd.DataFrame(json_data2)

    log.info('Mezclando conjuntos de datos...')

    # 3. Realizar el merge "right", manteniendo las filas de Spotify
    merged_df_right = pd.merge(grammy_df_cleaned, spotify_df_cleaned, 
                                left_on=['artist', 'nominee'], 
                                right_on=['artists', 'track_name'], 
                                how='right')

    # 4. Reemplazar valores NaN en la columna 'winner' por False
    merged_df_right['winner'].fillna(False, inplace=True)

    # 5. Contar cuántas canciones tienen 'True' en la columna 'winner'
    num_true_winners = merged_df_right[merged_df_right['winner'] == True].shape[0]
    log.info(f'Número de ganadores: {num_true_winners}')

    # 6. Verificar el número total de filas en el conjunto de datos combinado
    num_total_rows = merged_df_right.shape[0]
    log.info(f'Total de filas en el conjunto de datos combinado: {num_total_rows}')

    log.info('Conjuntos de datos mezclados correctamente.')
    
    return merged_df_right.to_json(orient='records')

def load_merge(json_data: json) -> json:
    """
    Carga el conjunto de datos combinado en la base de datos.

    Parámetros:
        json_data (JSON): La representación JSON del conjunto de datos combinado.
    
    Retorna:
        json (JSON): La representación JSON del DataFrame que contiene el conjunto de datos cargado.
    """

    json_data = json.loads(json_data)  # Cargar datos JSON a un DataFrame
    df = pd.DataFrame(json_data)

    df.insert(0, 'id', df.index + 1)  # Insertar una columna 'id' en el DataFrame

    connection = get_engine()  # Obtener la conexión a la base de datos

    try:
        # Verificar si la tabla 'songs_data' existe, y si es así, eliminarla
        if inspect(connection).has_table('songs_data'):
            SongsData.__table__.drop(connection)
            log.info("Tabla 'songs_data' eliminada correctamente.")
        
        SongsData.__table__.create(connection)  # Crear la tabla 'songs_data'
        log.info("Tabla 'songs_data' creada correctamente.")

        metadata = MetaData()
        table = Table('songs_data', metadata, autoload=True, autoload_with=connection)

        # Cargar datos en la base de datos
        with connection.connect() as conn:
            values = [{col: row[col] for col in df.columns} for _, row in df.iterrows()]
            conn.execute(insert(table), values)

        log.info('Datos cargados correctamente en la base de datos.')

        return df.to_json(orient='records')  # Retornar la representación JSON del DataFrame

    except Exception as e:
        log.error(f"Error al cargar los datos: {e}")  # Registrar error en caso de fallo

CREDENTIALS_PATH = 'credentials_module.json'  # Ruta al archivo de credenciales

def authenticate_drive():
    """
    Autentica al usuario con Google Drive usando la autenticación local.

    Returns:
        GoogleAuth: Instancia de GoogleAuth autenticada.
    """
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Realiza la autenticación local
    return gauth

def load_dataset_to_drive(json_data: json, title: str, folder_id: str) -> None:
    """
    Carga el conjunto de datos en Google Drive.
    
    Parámetros:
        json_data (JSON): El DataFrame que se va a subir.
        title (str): El título del archivo.
        folder_id (str): El ID de la carpeta en la que se cargará el conjunto de datos.
        
    Retorna:
        Ninguno.
    """

    json_data = json.loads(json_data)  # Cargar datos JSON a un DataFrame
    df = pd.DataFrame(json_data)

    drive = authenticate_drive()  # Llamar a la función de autenticación

    csv_string = df.to_csv(index=False)  # Convertir el DataFrame a CSV

    # Crear el archivo en Google Drive
    file = drive.CreateFile({'title': title,
                             'parents': [{'kind': 'drive#fileLink', 'id': folder_id}],
                             'mimeType': 'text/csv'})
    
    file.SetContentString(csv_string)  # Establecer el contenido del archivo
    file.Upload()  # Subir el archivo a Google Drive

    log.info('Dataset subido correctamente a Google Drive!')  # Log de éxito