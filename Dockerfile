FROM apache/airflow:3.0.0

USER airflow

RUN pip install --no-cache-dir \
    polars \
    apache-airflow-providers-amazon \
    boto3