import json
import os
import pandas as pd
from pathlib import Path

def transform_data(bronze_path, silver_path):
    """Transforma dados da camada bronze para silver"""
    os.makedirs(silver_path, exist_ok=True)
    
    for filename in Path(bronze_path).glob('*.json'):
        with open(filename) as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        
        # Transformações básicas
        df['address'] = df.apply(
            lambda x: f"{x['street']}, {x['city']}, {x['state']} {x['postal_code']}", 
            axis=1
        )
        
        # Particiona por estado
        for state in df['state'].unique():
            state_df = df[df['state'] == state]
            os.makedirs(f'{silver_path}/state={state}', exist_ok=True)
            state_df.to_parquet(
                f'{silver_path}/state={state}/{filename.stem}.parquet',
                index=False
            )

def aggregate_data(silver_path, gold_path):
    """Agrega dados da camada silver para gold"""
    os.makedirs(gold_path, exist_ok=True)
    all_data = []
    
    for state_dir in Path(silver_path).glob('state=*'):
        state = state_dir.name.split('=')[1]
        for parquet_file in state_dir.glob('*.parquet'):
            df = pd.read_parquet(parquet_file)
            all_data.append(df)
    
    if all_data:
        combined_df = pd.concat(all_data)
        agg_df = combined_df.groupby(['brewery_type', 'state']).size().reset_index(name='count')
        agg_df.to_parquet(
            f'{gold_path}/breweries_aggregated.parquet',
            index=False
        )