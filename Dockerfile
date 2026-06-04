FROM apache/airflow:3.0.0

# If need system tools, use root. For pip, use airflow.
USER airflow

# Install dependencies into airflow user space
RUN pip install --no-cache-dir \
    polars \
    apache-airflow-providers-amazon \
    boto3


# FROM apache/airflow:3.0.0
# COPY requirements.txt .
# # Switch to airflow user BEFORE install to avoid ModuleNotFoundError at runtime
# USER airflow
# RUN pip install --no-cache-dir -r requirements.txt
