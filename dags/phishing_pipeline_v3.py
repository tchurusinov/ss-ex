import polars as pl
import requests
import hashlib
import os
from pathlib import Path
from datetime import datetime
from functools import cached_property

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException

RAW_BASE = "/opt/airflow/data/raw/phishing_reply_addresses_{{ ds_nodash }}.csv"
PROC_BASE = "/opt/airflow/data/processed/phishing_reply_addresses_{{ ds_nodash }}.parquet"
ARCH_BASE = "/opt/airflow/data/archive/phishing_reply_addresses_{{ ds_nodash }}.tar.gz"
STATE_FILE = "/opt/airflow/data/state/phishing_reply_addresses.sha256"

class PhishingGetterOperator(BaseOperator):
    """Task 1: Download feed. No network calls during parse"""
    template_fields = ("output_path",)

    def __init__(self, url: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.url = url
        self.output_path = output_path

    def execute(self, context):
        self.log.info(f"Downloading from {self.url}")
        resp = requests.get(self.url, timeout=30)
        resp.raise_for_status() 
        
        if not resp.text.strip():
            raise ValueError("Empty body from source")
        
        output = Path(self.output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(resp.text)
        return str(output)

class S3PublisherOperator(BaseOperator):
    """Task 5: Publish to S3. Lazy initialization"""
    template_fields = ("files_to_upload", "bucket")

    def __init__(self, files_to_upload: list, bucket: str, aws_conn_id: str = "aws_default", **kwargs):
        super().__init__(**kwargs)
        self.files_to_upload = files_to_upload
        self.bucket = bucket
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self):
        return S3Hook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        ds = context["ds"]
        uploaded_uris = []

        if not self.hook.check_for_bucket(self.bucket):
            self.log.info(f"Bucket {self.bucket} missing. Creating it.")
            self.hook.create_bucket(self.bucket)

        for local_path in self.files_to_upload:
            p = Path(local_path)
            if not p.exists():
                raise FileNotFoundError(f"Missing file: {local_path}")
            
            filename = p.name
            folder = "archive" if ".tar.gz" in filename else "feeds/phishing_reply_addresses"
            key = f"{folder}/dt={ds}/{filename}"
            
            self.hook.load_file(local_path, key, self.bucket, replace=True)
            uploaded_uris.append(f"s3://{self.bucket}/{key}")
        return uploaded_uris

@dag(
    dag_id="phishing_reply_feed_v3",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False, 
)
def phishing_pipeline_v3():

    # Task 1: Getter
    getter_task = PhishingGetterOperator(
        task_id="download_raw_feed",
        url="http://svn.code.sf.net/p/aper/code/phishing_reply_addresses",
        output_path=RAW_BASE
    )

    # Task 2: Polars Process
    @task
    def transform_to_parquet(raw_file_path: str, **context):
        lf = pl.scan_csv(
            raw_file_path, 
            has_header=False, 
            new_columns=["address", "type", "source_date"],
            comment_prefix="#"
        )

        # Transformation logic
        lf = (
            lf.filter(pl.col("address").is_not_null())
            .with_columns([
                pl.col("address").str.to_lowercase(),
                pl.col("source_date").cast(pl.String).str.to_date("%Y%m%d")
            ])
            .unique(subset="address")
            .with_columns([
                pl.lit("Aper Phishing").alias("source_name"),
                pl.lit(datetime.now()).alias("ingested_at"),
                pl.lit(context['run_id']).alias("dag_run_id")
            ])
        )
        
        # Row-level record hash
        lf = lf.with_columns(
            pl.struct(["address", "type"]).cast(pl.String).hash().alias("record_hash")
        )

        # Resolve output path
        out_path = PROC_BASE.replace("{{ ds_nodash }}", context['ds_nodash'])
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Streaming sink to disk
        lf.sink_parquet(out_path)
        return out_path

    # Task 3: Change Verifier
    @task
    def verify_changed(parquet_path: str):
        curr_hash = hashlib.sha256(Path(parquet_path).read_bytes()).hexdigest()
        state = Path(STATE_FILE)
        
        if state.exists() and state.read_text() == curr_hash:
            raise AirflowSkipException("Data unchanged. Stopping.")
            
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(curr_hash)
        return parquet_path

    # Task 4: Bash Archive
    archive_task = BashOperator(
        task_id="archive_output",
        bash_command=f"mkdir -p /opt/airflow/data/archive && "
                     f"tar -czf {ARCH_BASE} -C /opt/airflow/data/processed $(basename {PROC_BASE})"
    )

    # Task 5: S3 Publisher
    publisher_task = S3PublisherOperator(
        task_id="publish_to_s3",
        files_to_upload=[PROC_BASE, ARCH_BASE],
        bucket="phishing-intel"
    )

    # ORCHESTRATION
    p_file = transform_to_parquet(getter_task.output)
    v_file = verify_changed(p_file)
    
    # dependencies for non-taskflow operators
    v_file >> archive_task >> publisher_task

phishing_pipeline_v3()