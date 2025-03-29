from datetime import datetime, timedelta
import os
import zipfile
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import requests
from io import BytesIO

# Configuration
KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/martj42/international-football-results-from-1872-to-2017"
DATA_DIR = "/opt/airflow/data"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/google/credentials.json'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15)
}

def download_and_extract_dataset(**context):
    """Download and extract the Kaggle dataset."""
    try:
        # Create data directory if not exists
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Download the dataset
        response = requests.get(KAGGLE_URL, stream=True)
        response.raise_for_status()
        
        # Extract ZIP file
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(DATA_DIR)
            
        print(f"Dataset extracted to {DATA_DIR}")
        return True
        
    except Exception as e:
        raise AirflowException(f"Download/extract failed: {str(e)}")

def preprocess_goalscorers(file_path):
    """Clean goalscorers.csv to fix dates, delimiters, and missing minutes."""
    df = pd.read_csv(file_path, delimiter=';')
    
    # Convert date from DD.MM.YYYY to YYYY-MM-DD
    df['date'] = pd.to_datetime(
        df['date'], 
        format='%d.%m.%Y',
        errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    
    # Handle missing/invalid minutes
    df['minute'] = pd.to_numeric(df['minute'], errors='coerce').fillna(0).astype(int)
    
    # Save with semicolon delimiter
    df.to_csv(file_path, index=False, sep=';')

def upload_csv_to_bigquery(**context):
    """Upload CSV to BigQuery with dynamic delimiters and preprocessing."""
    try:
        file_path = context['file_path']
        dataset_id = context['dataset_id']
        table_id = context['table_id']
        schema = context['schema']
        delimiter = context['delimiter']

        # Preprocess only goalscorers.csv
        if table_id == 'goalscorers':
            preprocess_goalscorers(file_path)

        credentials = service_account.Credentials.from_service_account_file(
            os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        )
        client = bigquery.Client(credentials=credentials)

        converted_schema = [
            bigquery.SchemaField(field["name"], field["type"], mode='NULLABLE')
            for field in schema
        ]

        job_config = bigquery.LoadJobConfig(
            schema=converted_schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            allow_jagged_rows=True,
            ignore_unknown_values=True,
            null_marker='',
            field_delimiter=delimiter
        )

        with open(file_path, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file,
                client.dataset(dataset_id).table(table_id),
                job_config=job_config
            )
            job.result()

        if job.errors:
            print(f"Job errors for table {table_id}: {job.errors}")

        context['ti'].set_state(State.SUCCESS)
        print(f"Loaded {job.output_rows} rows to {dataset_id}.{table_id}")
        return True

    except Exception as e:
        context['ti'].set_state(State.FAILED)
        raise AirflowException(f"Upload failed: {str(e)}")

# Schema configurations
schemas = {
    'former_names': {
        'schema': [
            {'name': 'current', 'type': 'STRING'},
            {'name': 'former', 'type': 'STRING'}, 
            {'name': 'start_date', 'type': 'DATE'},
            {'name': 'end_date', 'type': 'DATE'}
        ],
        'delimiter': ','
    },
    'goalscorers': {
        'schema': [
            {'name': 'date', 'type': 'DATE'},
            {'name': 'home_team', 'type': 'STRING'},
            {'name': 'away_team', 'type': 'STRING'},
            {'name': 'team', 'type': 'STRING'},
            {'name': 'scorer', 'type': 'STRING'},
            {'name': 'minute', 'type': 'INTEGER'},
            {'name': 'own_goal', 'type': 'BOOLEAN'},
            {'name': 'penalty', 'type': 'BOOLEAN'}
        ],
        'delimiter': ','
    },
    'results': {
        'schema': [
            {'name': 'date', 'type': 'DATE'},
            {'name': 'home_team', 'type': 'STRING'},
            {'name': 'away_team', 'type': 'STRING'},
            {'name': 'home_score', 'type': 'INTEGER'},
            {'name': 'away_score', 'type': 'INTEGER'},
            {'name': 'tournament', 'type': 'STRING'},
            {'name': 'city', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'neutral', 'type': 'BOOLEAN'}
        ],
        'delimiter': ','
    },
    'shootouts': {
        'schema': [
            {'name': 'date', 'type': 'DATE'},
            {'name': 'home_team', 'type': 'STRING'},
            {'name': 'away_team', 'type': 'STRING'},
            {'name': 'winner', 'type': 'STRING'},
            {'name': 'first_shooter', 'type': 'STRING'}
        ],
        'delimiter': ','
    }
}

with DAG(
    'kaggle_football_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gcp', 'bigquery', 'kaggle']
) as dag:

    download_task = PythonOperator(
        task_id='download_and_extract_dataset',
        python_callable=download_and_extract_dataset,
        provide_context=True
    )

    for table_name, config in schemas.items():
        upload_task = PythonOperator(
            task_id=f'upload_{table_name}',
            python_callable=upload_csv_to_bigquery,
            provide_context=True,
            op_kwargs={
                'file_path': f'{DATA_DIR}/{table_name}.csv',
                'dataset_id': 'soccer',
                'table_id': table_name,
                'schema': config['schema'],
                'delimiter': config['delimiter']
            }
        )
        download_task >> upload_task