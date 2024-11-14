from airflow.decorators import dag, task
import logging
logger = logging.getLogger(__name__)
logger.info("Starting DAG")

from datetime import datetime, timedelta
import os
import gdown
import tempfile
from typing import List
import duckdb

logger.info("Done import")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# /path/to/repo/folder/jaffle_shop.duckdb
DUCKDB_CONN_ID = "//local-data-eng-workshop/jaffle_shop.duckdb"

@dag(default_args=default_args, schedule_interval=timedelta(days=1))
def gcs_to_duck_db():

    @task
    def download_csv_files(**kwargs):
        logger.info("Starting download task")
        folder_url = 'https://drive.google.com/drive/folders/12W1Phx6T3dv0QK4j90TI0tjZ0EINB2Zh?usp=drive_link'


        # Download all CSV files from the folder into temp file
        temp_dir = "/tmp/jaffle_shop"
        logger.info("Downloading CSV files from Google Drive")
        gdown.download_folder(folder_url, output=temp_dir)

        logger.info("Done downloading CSV files")
        downloaded_files: List[str] = []
        for filename in os.listdir(temp_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join(temp_dir, filename)
                print(f"Downloaded: {filename}")
                downloaded_files.append(file_path)
        
        logger.info("Finished task")
        return downloaded_files
    
    @task
    def upload_to_duckdb(downloaded_files):
        # Connect to DuckDB/create a new DB if it doesn't exist
        conn = duckdb.connect(DUCKDB_CONN_ID)

        for file_path in downloaded_files:
            # Get the file name without extension
            file_name =  file_path.split('/')[-1]
            table_name = file_name.split('.')[0]

            # Drop existing table (if any) and create new one
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}');")

        # Close the connection
        conn.close()

    downloaded_files = download_csv_files()
    upload_to_duckdb(downloaded_files)

dag = gcs_to_duck_db()