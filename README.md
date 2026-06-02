# todo

- [ ] build
- [ ] run
- [ ] documentation
- [ ] unit tests






## quickstart

### Create directory structure
mkdir -p ./data/raw ./data/processed ./data/archive ./data/state ./dags ./logs ./plugins

### Install Python dependencies
pip install polars boto3 requests

### Initialize Airflow database
docker-compose run --rm airflow-webserver airflow db init

### Create Airflow admin user
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

### Start services
docker-compose up -d

### Verify setup
docker-compose logs airflow-webserver













































```
Practical Task: Phishing Reply Feed Pipeline
Source:
  •  URL: http://svn.code.sf.net/p/aper/code/phishing_reply_addresses
  •  Type: commented CSV
  •  Structure: ADDRESS,TYPE,DATE
  •  Example:

# ADDRESS:
exampleod.com,A,20080822
sampleod.org,E,20130129
Build this DAG:
download_raw_feed
    -> transform_to_parquet
    -> verify_changed
    -> archive_output
    -> publish_to_s3

Environment
Use the official Airflow 3.x Docker Compose setup, not puckel/docker-airflow.
Python dependencies:
polars
boto3
requests

For no AWS access, use LocalStack or MinIO as an S3-compatible target. For real environment access, use a real S3 bucket and credentials through Airflow connection/env config.

Task 1: Add PhishingGetterOperator
Requirements:
  •  Download the source URL.
  •  Write raw content to a configured local path.
  •  Support templated output path, for example: /opt/airflow/data/raw/phishing_reply_addresses_{{ ds_nodash }}.csv
  •  Fail clearly on non-200 HTTP response, timeout, or empty body.
  •  Do not create HTTP clients or perform network calls during DAG parse.
  •  Document the operator arguments and expected output.

Task 2: Add Polars Processing
Create a Python TaskFlow task or custom operator that:
  •  Reads the raw CSV with Polars.
  •  Skips comment lines starting with #.
  •  Uses columns: address, type, source_date.
  •  Normalizes addresses to lowercase.
  •  Removes empty rows.
  •  Removes duplicates by address.
  •  Parses source_date from yyyyMMdd.
  •  Adds metadata columns:
    •  source_name
    •  ingested_at
    •  dag_run_id
    •  record_hash
  •  Writes final output as Parquet.


Expected output:

/opt/airflow/data/processed/phishing_reply_addresses_{{ ds_nodash }}.parquet

Task 3: Add change_verifier

Requirements:

  •  Compute checksum of the produced Parquet file.
  •  Compare with checksum from the previous successful run.
  •  If unchanged, skip publishing or fail with a clear “data unchanged” message.
  •  If changed, persist the new checksum.
  •  For no-access practice, store checksum in local mounted state: /opt/airflow/data/state/phishing_reply_addresses.sha256
  •  For real S3 practice, store checksum under: s3://<bucket>/feeds/phishing_reply_addresses/_state/latest.sha256

Task 4: Add BashOperator Archive Step

Use BashOperator from the standard provider:
from airflow.providers.standard.operators.bash import BashOperator

Requirement:
  •  Archive the Parquet output into .tar.gz.
  •  Example output:

/opt/airflow/data/archive/phishing_reply_addresses_{{ ds_nodash }}.tar.gz

Task 5: Add S3PublisherOperator
Functional requirements:

  •  User can configure files/directories to publish from DAG.
  •  Supports both single files and directories.
  •  Fails if any configured file or directory is missing.
  •  Uploads:
    •  Parquet output
    •  archive output
    •  optional checksum/manifest file

Non-functional requirements:

  •  No S3 client creation during DAG parse.
  •  Use lazy initialization, for example cached_property, inside task execution.
  •  Log every uploaded local path and target S3 key.
  •  Return uploaded S3 URIs via XCom.

Example publish targets:

s3://<bucket>/feeds/phishing_reply_addresses/dt={{ ds }}/phishing_reply_addresses.parquet

s3://<bucket>/feeds/phishing_reply_addresses/archive/dt={{ ds }}/phishing_reply_addresses.tar.gz



Acceptance Criteria

  •  airflow dags list-import-errors shows no import errors.
  •  airflow dags test phishing_reply_feed 2026-01-01 succeeds.
  •  Raw file is downloaded.
  •  Parquet file is created with Polars.
  •  Re-running with unchanged data is detected.
  •  Archive file is created by BashOperator.
  •  Parquet and archive are uploaded to S3 or LocalStack/MinIO.
  •  Operator code has no network/client initialization at parse time.
  •  Unit tests cover parsing, transform, missing-file publish failure, and directory upload behavior.
```