import polars as pl
import requests
import hashlib
import tarfile
from pathlib import Path
from datetime import datetime
from functools import cached_property

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException

# Requirements: Templated paths [2, 4, 5]
RAW_BASE = "/opt/airflow/data/raw/phishing_reply_addresses_{{ ds_nodash }}.csv"
PROC_BASE = "/opt/airflow/data/processed/phishing_reply_addresses_{{ ds_nodash }}.parquet"
ARCH_BASE = "/opt/airflow/data/archive/phishing_reply_addresses_{{ ds_nodash }}.tar.gz"
STATE_FILE = "/opt/airflow/data/state/phishing_reply_addresses.sha256"

class PhishingGetterOperator(BaseOperator):
    """Task 1: Download feed. No network calls at parse time [2]."""
    template_fields = ("output_path",)

    def __init__(self, url, output_path, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.output_path = output_path

    def execute(self, context):
        self.log.info(f"Downloading from {self.url}")
        resp = requests.get(self.url, timeout=30)
        
        if resp.status_code != 200 or not resp.text.strip():
            raise ValueError(f"Download fail. Status: {resp.status_code}")
        
        # Ensure directory exists
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.output_path).write_text(resp.text)
        return self.output_path

class S3PublisherOperator(BaseOperator):
    """Task 5: Publish to S3. Lazy hook init [3]."""
    template_fields = ("files_to_upload",)

    def __init__(self, files_to_upload, bucket, aws_conn_id="aws_default", **kwargs):
        super().__init__(**kwargs)
        self.files_to_upload = files_to_upload # List of paths
        self.bucket = bucket
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self):
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        ds = context["ds"]
        uploaded_uris = []
        
        for local_path in self.files_to_upload:
            if not Path(local_path).exists():
                raise FileNotFoundError(f"Missing: {local_path}")
            
            # Target key logic from spec [3]
            filename = Path(local_path).name
            folder = "archive" if ".tar.gz" in filename else "feeds/phishing_reply_addresses"
            key = f"{folder}/dt={ds}/{filename}"
            
            self.hook.load_file(local_path, key, self.bucket, replace=True)
            uri = f"s3://{self.bucket}/{key}"
            self.log.info(f"Uploaded {local_path} -> {uri}")
            uploaded_uris.append(uri)
            
        return uploaded_uris # Return via XCom [3]



@dag(
    dag_id="phishing_reply_feed_v2",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
def phishing_pipeline_v2():

    # Task 1: Getter
    getter = PhishingGetterOperator(
        task_id="download_raw_feed",
        url="http://svn.code.sf.net/p/aper/code/phishing_reply_addresses",
        output_path=RAW_BASE
    )

    # Task 2: Polars Processing [8]
    @task
    def transform_to_parquet(raw_path: str):
        # Lazy Engine [6]
        lf = pl.scan_csv(
            raw_path,
            has_header=False,
            new_columns=["address", "type", "source_date"],
            comment_prefix="#" # Skip comment lines [8, 9]
        )

        # Transformation logic [8]
        lf = (
            lf.filter(pl.col("address").is_not_null())
            .with_columns([
                pl.col("address").str.to_lowercase(),
                pl.col("source_date").str.to_date("%Y%m%d")
            ])
            .unique(subset="address")
            .with_columns([
                pl.lit("Aper Phishing").alias("source_name"),
                pl.lit(datetime.now()).alias("ingested_at"),
                pl.lit("{{ run_id }}").alias("dag_run_id")
            ])
        )
        
        # Record hash per row [8]
        lf = lf.with_columns(
            pl.struct(["address", "type"]).cast(pl.String).str.hash().alias("record_hash")
        )

        # Sink to disk [6]
        out_path = raw_path.replace("raw", "processed").replace(".csv", ".parquet")
        lf.sink_parquet(out_path)
        return out_path

    # Task 3: Change Verifier [4]
    @task
    def verify_changed(parquet_path: str):
        current_hash = hashlib.sha256(Path(parquet_path).read_bytes()).hexdigest()
        state_path = Path(STATE_FILE)
        
        if state_path.exists() and state_path.read_text() == current_hash:
            raise AirflowSkipException("Data unchanged. Stopping.")
        
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(current_hash)
        return parquet_path

    # Task 4: Bash Archive [5]
    archive = BashOperator(
        task_id="archive_output",
        bash_command=f"tar -czf {ARCH_BASE} -C /opt/airflow/data/processed $(basename {PROC_BASE})"
    )

    # Task 5: S3 Publisher [5]
    publisher = S3PublisherOperator(
        task_id="publish_to_s3",
        files_to_upload=[PROC_BASE, ARCH_BASE],
        bucket="phishing-intel"
    )

    # Orchestration [1]
    parquet_file = transform_to_parquet(getter.output_path)
    verified_file = verify_changed(parquet_file)
    verified_file >> archive >> publisher

phishing_pipeline_v2()