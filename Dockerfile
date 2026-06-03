FROM apache/airflow:3.0.0
COPY requirements.txt .
# Switch to airflow user BEFORE install to avoid ModuleNotFoundError at runtime
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
