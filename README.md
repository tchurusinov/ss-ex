# setup

## env
set AIRFLOW_UID(50000), AIRFLOW_GID(0), FERNET_KEY, JWT_SECRET in .env

echo '{"admin": "admin"}' > ./config/passwords.json

python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" - for fernet key

this is not necessary, conn issues were other
~~chmod -R 777 ./config ./dags ./logs ./data~~


## docker 
docker compose build --no-cache && docker image prune -f

~~docker compose run --rm airflow-init~~
~~docker compose down -v~~

docker compose up -d

docker compose exec airflow-api-server cmmnds to talk to container







```
Connection Fields

    Connection Id: aws_default (Standard ID tasks expect).
    Connection Type: Select Amazon AWS from list.
    Description: Caveman S3 mock (Optional).
    AWS Access Key ID: minioadmin.
    AWS Secret Access Key: minioadmin.

Where is "Extra"?
Scroll to bottom of form. Large text area named Extra exists for all AWS connections. Paste this JSON blob exactly:

{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1"
}
```
