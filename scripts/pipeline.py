from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import requests
import pandas as pd
import os
import json
from pathlib import Path
from schemas import BronzeSchema, GoldSchema

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_brewery_data():
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        BronzeSchema.validate(df)
        
        os.makedirs('/opt/airflow/data/bronze', exist_ok=True)
        with open(f'/opt/airflow/data/bronze/breweries_{datetime.now().strftime("%Y%m%d%H%M%S")}.json', 'w') as f:
            json.dump(data, f)
    else:
        raise AirflowFailException(f"API request failed: {response.status_code}")

def transform_to_silver():
    bronze_path = '/opt/airflow/data/bronze'
    silver_path = '/opt/airflow/data/silver'
    os.makedirs(silver_path, exist_ok=True)
    
    for filename in Path(bronze_path).glob('*.json'):
        with open(filename) as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        df['address'] = df.apply(
            lambda x: f"{x['street']}, {x['city']}, {x['state']} {x['postal_code']}", 
            axis=1
        )
        
        for state in df['state'].unique():
            state_df = df[df['state'] == state]
            os.makedirs(f'{silver_path}/state={state}', exist_ok=True)
            state_df.to_parquet(
                f'{silver_path}/state={state}/breweries_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet',
                index=False
            )

def aggregate_to_gold():
    silver_path = '/opt/airflow/data/silver'
    gold_path = '/opt/airflow/data/gold'
    os.makedirs(gold_path, exist_ok=True)
    all_data = []
    
    for state_dir in Path(silver_path).glob('state=*'):
        for parquet_file in state_dir.glob('*.parquet'):
            df = pd.read_parquet(parquet_file)
            all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data)
        agg_df = combined_df.groupby(['brewery_type', 'state']).size().reset_index(name='count')
        GoldSchema.validate(agg_df)
        
        agg_df.to_parquet(
            f'{gold_path}/breweries_aggregated_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet',
            index=False
        )

def data_quality_check():
    gold_path = Path('/opt/airflow/data/gold')
    parquet_files = list(gold_path.glob('*.parquet'))
    
    if not parquet_files:
        raise AirflowFailException("Nenhum arquivo Parquet encontrado na camada Gold")

    df = pd.read_parquet(parquet_files[0])
    checks = [
        (df.empty, "DataFrame vazio na camada Gold"),
        (df['count'].isnull().any(), "Valores nulos na coluna 'count'"),
        ((df['count'] <= 0).any(), "Contagem inválida (<= 0)")
    ]

    for condition, error_msg in checks:
        if condition:
            raise AirflowFailException(f"Falha na qualidade dos dados: {error_msg}")

with DAG(
    'brewery_data_pipeline',
    default_args=default_args,
    description='Pipeline para processar dados de cervejarias',
    schedule_interval='0 0 * * *',
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_brewery_data',
        python_callable=extract_brewery_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_to_silver,
    )
    
    aggregate_task = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_to_gold,
    )
    
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )
    
    extract_task >> transform_task >> aggregate_task >> quality_check