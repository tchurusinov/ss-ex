import pytest
import polars as pl
import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch
from airflow.models import DagBag
from airflow.exceptions import AirflowSkipException

from dags.phishing_pipeline_v4 import (
    PhishingGetterOperator, S3PublisherOperator, transform_to_parquet, verify_changed
)

@pytest.fixture
def dag():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.get_dag(dag_id="phishing_reply_feed_v4")

def test_dag_loaded(dag):
    assert dag is not None
    assert len(dag.tasks) == 5

@patch("requests.get")
def test_getter_operator_success(mock_get, tmp_path):
    mock_get.return_value.status_code, mock_get.return_value.text = 200, "ex.com,A,20250101\n"
    out = tmp_path / "raw.csv"
    op = PhishingGetterOperator(task_id="t", url="http://m.url", output_path=str(out))
    op.execute({})
    assert "ex.com" in out.read_text()

def test_transformation_logic(tmp_path):
    raw, proc = tmp_path / "raw.csv", tmp_path / "out.parquet"
    raw.write_text("EMAIL@Ex.com,A,20250101")
    transform_to_parquet.function(str(raw), str(proc), run_id="test")
    df = pl.read_parquet(str(proc))
    assert (df["address"] == "email@ex.com").all()
    assert df["source_date"].dtype == pl.Date # Fix: dtype check [History]

def test_publisher_directory_behavior(tmp_path):
    tdir = tmp_path / "up"
    tdir.mkdir(); (tdir / "f.parquet").write_text("d")
    op = S3PublisherOperator(task_id="t", files_to_upload=[str(tdir)], bucket="b")
    op.hook = MagicMock()
    op.execute({"ds": "2026-01-01"})
    assert op.hook.load_file.called

def test_verify_changed_skips(tmp_path):
    p = tmp_path / "t.parquet"
    p.write_bytes(b"data")
    h = hashlib.sha256(b"data").hexdigest()
    with patch("dags.phishing_pipeline_v4.STATE_FILE", str(tmp_path / "s.sha256")):
        Path(tmp_path / "s.sha256").write_text(h)
        with pytest.raises(AirflowSkipException):
            verify_changed.function(str(p))