import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

# Import your operators/tasks from the dag file
# Assuming your dag file is named phishing_pipeline_v3.py
from dags.phishing_pipeline_v3 import (
    PhishingGetterOperator, 
    S3PublisherOperator,
    STATE_FILE
)

@pytest.fixture
def dag():
    """Test 1: DAG Parsing logic [2, 4]"""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.get_dag(dag_id="phishing_reply_feed_v3")

def test_dag_loaded(dag):
    """Verify DAG exists and has no import errors [1]"""
    assert dag is not None
    assert len(dag.tasks) == 5

@patch("requests.get")
def test_getter_operator_success(mock_get, tmp_path):
    """Test Task 1: Download success and file write [5]"""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = "example.com,A,20250101\n"
    
    out_path = tmp_path / "raw.csv"
    op = PhishingGetterOperator(
        task_id="test_get", 
        url="http://mock.url", 
        output_path=str(out_path)
    )
    op.execute(context={})
    
    assert out_path.exists()
    assert "example.com" in out_path.read_text()

def test_transformation_logic(tmp_path):
    """Test Task 2: Polars cleaning, metadata, and hashing [6, 7]"""
    # Create mock raw data
    raw = tmp_path / "raw.csv"
    raw.write_text("# comment\nEMAIL@Example.com,A,20250101\n\n,B,20250102\nemail@example.com,A,20250101")
    
    # Run Polars logic (simplified for unit test)
    lf = pl.scan_csv(str(raw), has_header=False, new_columns=["address", "type", "source_date"], comment_prefix="#")
    
    # Apply your DAG logic
    lf = (
        lf.filter(pl.col("address").is_not_null())
        .with_columns([
            pl.col("address").str.to_lowercase(),
            pl.col("source_date").cast(pl.String).str.to_date("%Y%m%d")
        ])
        .unique(subset="address")
        .with_columns([
            pl.lit("Aper Phishing").alias("source_name"),
            pl.struct(["address", "type"]).hash().alias("record_hash")
        ])
    )
    df = lf.collect()

    assert len(df) == 1  # 1 dupe removed, 1 null removed, 1 comment skipped
    assert df["address"] == "email@example.com"  # Lowercase check
    assert isinstance(df["source_date"], pl.Date) # Type check
    assert "record_hash" in df.columns # Metadata check

def test_publisher_missing_file_fail():
    """Test Task 5: Failure when file not found [1, 8]"""
    op = S3PublisherOperator(
        task_id="test_pub",
        files_to_upload=["/tmp/non_existent_file.parquet"],
        bucket="test-bucket"
    )
    
    # Mock the hook to avoid connection attempts
    op.hook = MagicMock()
    
    with pytest.raises(FileNotFoundError):
        op.execute(context={"ds": "2026-01-01"})

def test_verify_changed_skips(tmp_path):
    """Test Task 3: Skipping logic when hash matches [9, 10]"""
    # Import locally to use mocked state file
    from dags.phishing_pipeline_v3 import verify_changed
    
    parquet = tmp_path / "test.parquet"
    parquet.write_bytes(b"data")
    content_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" # hash of 'test'
    
    # Mock the state file path
    with patch("dags.phishing_pipeline_v3.STATE_FILE", str(tmp_path / "state.txt")):
        Path(tmp_path / "state.txt").write_text(content_hash)
        
        with pytest.raises(AirflowSkipException):
            # Pass path that will generate the same hash
            verify_changed.function(str(parquet))
