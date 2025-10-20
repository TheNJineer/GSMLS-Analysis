# Extend the official Airflow image
FROM apache/airflow:latest-python3.12

# Copy the DAG requirements to the current directory
COPY requirements.txt /requirements.txt

# Install the requirements. Makes sure dependencies dont override what comes with Airflow
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt