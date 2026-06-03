import hashlib
import polars as pl
from pathlib import Path
from datetime import datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_PATH = "/opt/airflow/data/raw/phishing_reply_addresses.txt"
PROCESSED_PATH = "/opt/airflow/data/processed/phishing_feed.parquet"
STATE_PATH = "/opt/airflow/data/state/last_hash.txt"
BUCKET = "phishing-intel"

@dag(
    dag_id="phishing_threat_feed",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def phishing_pipeline():

    @task
    def verify_changed():
        """Cryptographic check to skip redundant work [4]"""
        file_path = Path(RAW_PATH)
        if not file_path.exists():
            raise FileNotFoundError(f"Source file missing: {RAW_PATH}")

        # SHA-256 Fingerprint
        current_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()
        
        state_file = Path(STATE_PATH)
        if state_file.exists() and state_file.read_text() == current_hash:
            # Data same -> Stop DAG [4, 5]
            raise AirflowSkipException("No data change detected. Skipping.")
            
        state_file.write_text(current_hash)
        return current_hash

    @task
    def transform_data():
        """Polars Lazy Engine: Process 270MB+ in low RAM [6, 7]"""
        # scan_csv builds query plan, no load to RAM [7, 8]
        lf = pl.scan_csv(
            RAW_PATH, 
            has_header=False, 
            new_columns=["address", "type", "date"],
            # Skip comment lines starting with #
            skip_rows=25 
        )

        # Predicate Pushdown: filter early [8, 9]
        # Example: Keep only active phishing tactics (A, B, C)
        lf = lf.filter(pl.col("type").str.contains("A|B|C"))
        lf = lf.sort("date", descending=True)

        # Streaming Sink: Write row groups to disk [7, 8]
        lf.sink_parquet(PROCESSED_PATH, compression="snappy")
        return PROCESSED_PATH

    @task
    def upload_to_minio():
        """Push Parquet to local S3 mock [10, 11]"""
        s3 = S3Hook(aws_conn_id="aws_default")
        
        # Ensure bucket exists
        if not s3.check_for_bucket(BUCKET):
            s3.create_bucket(BUCKET)
            
        s3.load_file(
            filename=PROCESSED_PATH,
            key=f"feeds/phishing_{datetime.now().strftime('%Y%m%d')}.parquet",
            bucket_name=BUCKET,
            replace=True
        )

    verify_changed() >> transform_data() >> upload_to_minio()

phishing_pipeline()
