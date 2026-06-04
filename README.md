# todo

- [ ] build
    - [x] infra
    - [ ] dag
- [ ] run
- [ ] documentation
- [ ] unit tests






# setup

docker compose down -v

## environment
echo '{"admin": "admin"}' > ./config/passwords.json
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

docker compose build --no-cache
docker image prune -f

docker compose up -d postgres
docker compose run --rm airflow-init


docker compose up -d

docker compose exec airflow-api-server cat simple_auth_manager_passwords.json.generated











```
Source Data: Place phishing_reply_addresses.txt in data/raw/.
DAG Implementation:

    verify_changed: Task to SHA-256 hash source file. Skip if match previous.
    transform_data: Use Polars Lazy API. scan_csv -> filter/sort -> sink_parquet to data/processed/.
    upload_s3: Use S3CreateObjectOperator or Hook to push Parquet to MinIO.

MinIO Setup: Access localhost:9001. Create bucket name phishing-intel.
Airflow Connection: In UI, create aws_default connection. Extra: {"endpoint_url": "http://minio:9000"}.
Unit Tests: Create tests/ folder. Use DagBag to check DAG integrity and Pytest for Polars logic.
```







```
tep 1: Setup Airflow Environment

    Install Airflow 3.x with Docker Compose
    Configure DAGs directory structure
    Set up LocalStack/MinIO for S3-compatible storage
    Create data directories: /opt/airflow/data/raw, /opt/airflow/data/processed, /opt/airflow/data/archive, /opt/airflow/data/state

Step 2: Implement PhishingGetterOperator

    Create custom operator inheriting from BaseOperator
    Add arguments: source_url, output_path
    Implement execute() method with HTTP download using requests
    Handle non-200 responses, timeouts, and empty bodies
    Use {{ ds_nodash }} templating for output path
    Ensure no network calls during DAG parse time 

Step 3: Implement Polars Processing Task

    Create Python TaskFlow task or custom operator
    Read raw CSV with Polars, skip comment lines starting with #
    Process columns: address, type, source_date
    Normalize addresses to lowercase
    Remove empty rows and duplicates by address
    Parse source_date from yyyymmdd format
    Add metadata columns: source_name, ingested_at, dag_run_id, record_hash
    Write output as Parquet to configured path 

Step 4: Implement change_verifier

    Compute SHA256 checksum of produced Parquet file
    Compare with previous successful run checksum
    Skip publishing if data unchanged with clear "data unchanged" message
    Persist new checksum locally to: /opt/airflow/data/state/phishing_reply_addresses.sha256
    For S3 practice, store under: s3://<bucket>/feeds/phishing_reply_addresses/_state/latest.sha256

Step 5: Implement BashOperator Archive Step

    Use BashOperator from standard provider
    Create archive command: tar -czf /opt/airflow/data/archive/phishing_reply_addresses_{{ ds_nodash }}.tar.gz -C /opt/airflow/data/processed phishing_reply_addresses_{{ ds_nodash }}.parquet
    Set up proper task dependencies

Step 6: Implement S3PublisherOperator

    Create custom operator using boto3 for S3 operations
    Upload Parquet and archive files to S3/LocalStack
    Handle S3-compatible storage configuration
    Implement proper error handling for upload failures

Step 7: Integrate All Components

    Build complete DAG with proper task dependencies
    Ensure all operators avoid network/client initialization at parse time
    Test DAG import without errors
    Verify each task executes correctly in sequence

Step 8: Add Unit Tests

    Test parsing functionality
    Test transform operations
    Test missing-file publish failure handling
    Test directory upload behavior
    Verify all acceptance criteria are met 

Step 9: Final Validation

    Run airflow dags list-import-errors to verify no import errors
    Execute airflow dags test phishing_reply_feed 2026-01-01
    Validate all acceptance criteria are satisfied 
```






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