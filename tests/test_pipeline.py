import pytest
import os
import json
import pandas as pd
from scripts.pipeline import transform_data, aggregate_data

@pytest.fixture
def sample_data():
    return [
        {
            "id": "1",
            "name": "Test Brewery",
            "brewery_type": "micro",
            "street": "123 Main St",
            "city": "Denver",
            "state": "Colorado",
            "postal_code": "80202",
            "country": "United States"
        }
    ]

def test_transform_data(tmp_path, sample_data):
    # Test data transformation
    bronze_path = tmp_path / "bronze"
    bronze_path.mkdir()
    silver_path = tmp_path / "silver"
    
    # Save sample data
    with open(bronze_path / "test.json", "w") as f:
        json.dump(sample_data, f)
    
    # Transform
    transform_data(str(bronze_path), str(silver_path))
    
    # Check if file was created
    state_dir = silver_path / "state=Colorado"
    assert state_dir.exists()
    
    parquet_files = list(state_dir.glob("*.parquet"))
    assert len(parquet_files) == 1
    
    # Check data
    df = pd.read_parquet(parquet_files[0])
    assert not df.empty
    assert "address" in df.columns

def test_aggregate_data(tmp_path, sample_data):
    # Test aggregation
    silver_path = tmp_path / "silver"
    state_dir = silver_path / "state=Colorado"
    state_dir.mkdir(parents=True)
    gold_path = tmp_path / "gold"
    
    # Create sample silver data
    df = pd.DataFrame(sample_data)
    df['address'] = df.apply(
        lambda x: f"{x['street']}, {x['city']}, {x['state']} {x['postal_code']}", 
        axis=1
    )
    df.to_parquet(state_dir / "test.parquet", index=False)
    
    # Aggregate
    aggregate_data(str(silver_path), str(gold_path))
    
    # Check if file was created
    gold_files = list(gold_path.glob("*.parquet"))
    assert len(gold_files) == 1
    
    # Check aggregation
    agg_df = pd.read_parquet(gold_files[0])
    assert not agg_df.empty
    assert agg_df.iloc[0]['count'] == 1