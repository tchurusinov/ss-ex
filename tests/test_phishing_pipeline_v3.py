import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

# Requirements: Unit tests cover parsing, transform, and failures [5]
from dags.phishing_pipeline_v3 import (
    PhishingGetterOperator, 
    S3PublisherOperator
)

@pytest.fixture
def dag():
    """Verify DAG structure exists without syntax errors."""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.get_dag(dag_id="phishing_reply_feed_v3")

def test_dag_loaded(dag):
    """Confirm 5 tasks present in pipeline [5]."""
    assert dag is not None
    assert len(dag.tasks) == 5

@patch("requests.get")
def test_getter_operator_success(mock_get, tmp_path):
    """Verify raw download and local file write [6]."""
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
    """Verify lowercase, date parsing, and metadata [7]."""
    raw = tmp_path / "raw.csv"
    raw.write_text("# comment\nEMAIL@Example.com,A,20250101\n\n,B,20250102\nemail@example.com,A,20250101")
    
    # Simulate Task 2 logic
    lf = pl.scan_csv(str(raw), has_header=False, new_columns=["address", "type", "source_date"], comment_prefix="#")
    
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

    assert len(df) == 1
    assert (df["address"] == "email@example.com").all() 
    # FIX: Check .dtype instead of isinstance [1]
    assert df["source_date"].dtype == pl.Date
    assert "record_hash" in df.columns

def test_publisher_missing_file_fail():
    """Verify operator fails when file not found [4]."""
    op = S3PublisherOperator(
        task_id="test_pub",
        files_to_upload=["/tmp/non_existent.parquet"],
        bucket="test-bucket"
    )
    op.hook = MagicMock()
    with pytest.raises(FileNotFoundError):
        op.execute(context={"ds": "2026-01-01"})

def test_publisher_directory_behavior(tmp_path):
    """Task 5 Requirement: Support directory uploads [4]."""
    test_dir = tmp_path / "upload_me"
    test_dir.mkdir()
    (test_dir / "f1.parquet").write_text("data")
    
    op = S3PublisherOperator(
        task_id="test_dir",
        files_to_upload=[str(test_dir)],
        bucket="test-bucket"
    )
    op.hook = MagicMock()
    op.hook.check_for_bucket.return_value = True
    
    # If operator supports directories, this shouldn't crash
    op.execute(context={"ds": "2026-01-01"})
    assert op.hook.load_file.called

def test_verify_changed_skips(tmp_path):
    """Confirm AirflowSkipException on identical hash [8]."""
    # IMPORT FIX: requires verify_changed moved to module level in DAG file
    from dags.phishing_pipeline_v3 import verify_changed
    
    parquet = tmp_path / "test.parquet"
    parquet.write_bytes(b"data")
    content_hash = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    
    with patch("dags.phishing_pipeline_v3.STATE_FILE", str(tmp_path / "state.sha256")):
        Path(tmp_path / "state.sha256").write_text(content_hash)
        with pytest.raises(AirflowSkipException):
            # Call underlying function of decorated task
            verify_changed.function(str(parquet))