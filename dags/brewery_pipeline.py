from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os
import json

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
        os.makedirs('/opt/airflow/data/bronze', exist_ok=True)
        with open(f'/opt/airflow/data/bronze/breweries_{datetime.now().strftime("%Y%m%d%H%M%S")}.json', 'w') as f:
            json.dump(data, f)
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def transform_to_silver():
    # Process all bronze files
    bronze_path = '/opt/airflow/data/bronze'
    silver_path = '/opt/airflow/data/silver'
    os.makedirs(silver_path, exist_ok=True)
    
    for filename in os.listdir(bronze_path):
        if filename.endswith('.json'):
            with open(f'{bronze_path}/{filename}') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            
            # Basic transformations
            df['address'] = df.apply(
                lambda x: f"{x['street']}, {x['city']}, {x['state']} {x['postal_code']}", 
                axis=1
            )
            
            # Save partitioned by state
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
    for state in os.listdir(silver_path):
        if state.startswith('state='):
            state_name = state.replace('state=', '')
            for filename in os.listdir(f'{silver_path}/{state}'):
                if filename.endswith('.parquet'):
                    df = pd.read_parquet(f'{silver_path}/{state}/{filename}')
                    all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data)
        
        # Aggregate by brewery type and state
        agg_df = combined_df.groupby(['brewery_type', 'state']).size().reset_index(name='count')
        
        agg_df.to_parquet(
            f'{gold_path}/breweries_aggregated_{datetime.now().strftime("%Y%m%d%H%M%S")}.parquet',
            index=False
        )

with DAG(
    'brewery_data_pipeline',
    default_args=default_args,
    description='A pipeline to process brewery data',
    schedule_interval='@daily',
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
    
    extract_task >> transform_task >> aggregate_task