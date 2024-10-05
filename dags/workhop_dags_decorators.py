from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
import os

# Agregar el directorio donde se encuentra el archivo etl.py
sys.path.append(os.path.abspath('./dags'))

from dags.etl import grammy_process, load_dataset_to_drive, load_merge, merge_datasets, read_spotify_data, transform_grammys_data, transform_spotify_data
from etl import *  # Asegúrate de que este archivo contenga las funciones necesarias

# Definir las configuraciones predeterminadas para el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='An ETL workflow for integrating Grammy Awards data with Spotify datasets, performing transformations, merging, and loading the resulting data into Google Drive',
    schedule_interval='@daily',
)
def etl_workshop():

    @task
    def db_grammy_task():
        """Fetches Grammy data and returns it."""
        return grammy_process()

    @task
    def grammy_transformations_task(json_data):
        """Transforms the Grammy data."""
        return transform_grammys_data(json_data)

    @task
    def read_spotify_data_task():
        """Reads the Spotify dataset."""
        return read_spotify_data('data/spotify_dataset.csv')

    @task
    def transform_spotify_data_task(json_data):
        """Transforms the Spotify dataset."""
        return transform_spotify_data(json_data)

    @task
    def merge_data_task(grammy_data, spotify_data):
        """Merges Grammy and Spotify data."""
        return merge_datasets(grammy_data, spotify_data)

    @task
    def load_data_task(merged_data):
        """Loads merged data into the destination."""
        return load_merge(merged_data)

    @task
    def store_task(final_data):
        """Stores the final dataset in Google Drive."""
        return load_dataset_to_drive(final_data, 'songs_data.csv', '1LxynhSi5b4IBvddJTey9RrTQDfk_Cq_b')

    # Definición de las dependencias entre las tareas
    grammy_data = db_grammy_task()
    transformed_grammy_data = grammy_transformations_task(grammy_data)

    spotify_data = read_spotify_data_task()
    transformed_spotify_data = transform_spotify_data_task(spotify_data)

    merged_data = merge_data_task(transformed_grammy_data, transformed_spotify_data)

    loaded_data = load_data_task(merged_data)

    store_task(loaded_data)

# Instancia del DAG
workflow_api_etl_dag = etl_workshop()
