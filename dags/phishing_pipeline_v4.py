import polars as pl
import requests
import hashlib
from pathlib import Path
from datetime import datetime
from functools import cached_property

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException

# v4 Paths
RAW_BASE = "/opt/airflow/data/raw/phishing_reply_addresses_{{ ds_nodash }}.csv"
PROC_BASE = "/opt/airflow/data/processed/phishing_reply_addresses_{{ ds_nodash }}.parquet"
ARCH_BASE = "/opt/airflow/data/archive/phishing_reply_addresses_{{ ds_nodash }}.tar.gz"
STATE_FILE = "/opt/airflow/data/state/phishing_reply_addresses_v4.sha256"

class PhishingGetterOperator(BaseOperator):
    template_fields = ("output_path",)
    def __init__(self, url: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.url, self.output_path = url, output_path
    def execute(self, context):
        res = requests.get(self.url, timeout=30)
        if res.status_code != 200: raise Exception(f"Err: {res.status_code}")
        p = Path(self.output_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(res.text)
        return str(p)

class S3PublisherOperator(BaseOperator):
    template_fields = ("files_to_upload", "bucket")
    def __init__(self, files_to_upload: list, bucket: str, **kwargs):
        super().__init__(**kwargs)
        self.files_to_upload, self.bucket = files_to_upload, bucket
    @cached_property
    def hook(self): return S3Hook(aws_conn_id="aws_default")
    def execute(self, context):
        uris = []
        for p_str in self.files_to_upload:
            p = Path(p_str)
            if not p.exists(): raise FileNotFoundError(p_str)
            files = list(p.glob("**/*")) if p.is_dir() else [p]
            for f in files:
                if f.is_file():
                    key = f"feeds/v4/{context['ds']}/{f.name}"
                    self.hook.load_file(str(f), key, self.bucket, replace=True)
                    uris.append(f"s3://{self.bucket}/{key}")
        return uris

@task
def transform_to_parquet(raw_file_path: str, proc_path: str, **context):
    """Task 2: Direct path argument fix for NoneType error [History]."""
    lf = pl.scan_csv(raw_file_path, has_header=False, new_columns=["address", "type", "source_date"], comment_prefix="#")
    lf = (lf.filter(pl.col("address").is_not_null())
          .with_columns([pl.col("address").str.to_lowercase(), 
                         pl.col("source_date").cast(pl.String).str.to_date("%Y%m%d")])
          .unique(subset="address")
          .with_columns([pl.lit("Aper Phishing").alias("source_name"),
                         pl.lit(datetime.now().isoformat()).alias("ingested_at"),
                         pl.lit(context['run_id']).alias("dag_run_id"),
                         pl.struct(["address", "type"]).hash().alias("record_hash")]))
    Path(proc_path).parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(proc_path)
    return proc_path

@task
def verify_changed(parquet_path: str):
    """Task 3: Module level for pytest import [1]."""
    curr = hashlib.sha256(Path(parquet_path).read_bytes()).hexdigest()
    state = Path(STATE_FILE)
    if state.exists() and state.read_text() == curr:
        raise AirflowSkipException("Data unchanged. Stopping v4.")
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(curr)
    return parquet_path

@dag(dag_id="phishing_reply_feed_v4", start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def phishing_pipeline_v4():
    getter = PhishingGetterOperator(task_id="download_raw_feed", url="http://svn.code.sf.net/p/aper/code/phishing_reply_addresses", output_path=RAW_BASE)
    p_file = transform_to_parquet(getter.output, PROC_BASE)
    v_file = verify_changed(p_file)
    arch = BashOperator(task_id="archive_output", bash_command=f"tar -czf {ARCH_BASE} {PROC_BASE}")
    pub = S3PublisherOperator(task_id="publish_to_s3", files_to_upload=[PROC_BASE, ARCH_BASE], bucket="phishing-intel")
    v_file >> arch >> pub
phishing_pipeline_v4()