# Phishing Threat Intelligence Pipeline (Airflow 3.x)

Enterprise-grade orchestration for processing phishing reply-address feeds. Uses **Airflow 3.0**, **Polars** for memory-efficient lazy evaluation, and **MinIO** for local S3 simulation.

## 🏗 Architecture
- **Airflow 3.x**: Decoupled architecture with `dag-processor` and `api-server`.
- **Polars Engine**: Lazy CSV scanning and Parquet streaming to minimize RAM footprint.
- **SimpleAuthManager**: JSON-based authentication.
- **AIP-72**: Task execution via authenticated HTTP API.

## 🚀 Quickstart

### 1. Environment Setup
Create `.env` file in root:
```bash
AIRFLOW_UID=50000
AIRFLOW_GID=0
# Generate key: python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=YOUR_GENERATED_KEY
# ALL secrets below MUST be identical for service trust
AIRFLOW_SECRET=RANDOM_LONG_STRING_SAME_FOR_ALL
```

Initialize local directories:
```bash
mkdir -p ./data/{raw,processed,archive,state} ./logs ./config ./dags
```

### 2. Configuration
**Auth**: Create `./config/passwords.json` as a dictionary:
```json
{"admin": "admin"}
```

**S3 Mock**: After UI is up (`localhost:8383`), configure `aws_default` connection:
- **Conn Id**: `aws_default`
- **Conn Type**: `Amazon AWS`
- **Login**: `minioadmin`
- **Password**: `minioadmin`
- **Extra**:
```json
{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1"
}
```

### 3. Deploy
```bash
# Build custom image with Polars/AWS providers
docker compose build --no-cache

# Migrate DB (Required for Airflow 3)
docker compose run --rm airflow-init

# Start all services (api-server, scheduler, dag-processor, triggerer, postgres, minio)
docker compose up -d
```

## 🛠 Pipeline Logic (`phishing_reply_feed_v3`)
1. **Download**: `PhishingGetterOperator` pulls raw feed from SF.net.
2. **Transform**: Polars filters nulls, normalizes case, parses dates, and injects metadata (`record_hash`, `dag_run_id`).
3. **Verify**: SHA-256 state check. Skips downstream if data is unchanged.
4. **Archive**: Compresses Parquet to `.tar.gz` via `BashOperator`.
5. **Publish**: `S3PublisherOperator` pushes Parquet and Archive to MinIO.

## 🧪 Testing & Verification

### Unit Tests
Run the `pytest` suite inside the scheduler container:
```bash
docker compose exec airflow-scheduler pytest tests/test_phishing_pipeline_v3.py
docker compose exec -e PYTHONPATH=/opt/airflow airflow-scheduler pytest tests/test_phishing_pipeline_v4.py
```

### Forensic Inspection
Verify Parquet data integrity:
```bash
docker compose exec airflow-scheduler python3 -c "
import polars as pl
df = pl.read_parquet('/opt/airflow/data/processed/YOUR_FILE.parquet')
print(df.schema)
print(df.head())"
```

### S3 UI
Access MinIO Console at `http://localhost:9001` (Credentials: `minioadmin`/`minioadmin`).