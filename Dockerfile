# Extend the official Airflow image
FROM apache/airflow:latest-python3.12

# Copy the DAG requirements to the current directory
COPY requirements.txt /requirements.txt

# Install the requirements using the official Constraints file
# This ensures that no dependency in requirements.txt (or your GitHub repo)
# overrides the versions required to run Airflow safely.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"