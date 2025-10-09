# Extend the official Airflow image
FROM apache/airflow:3.0.4

# Switch to root to install the dependencies
USER root

# Copy the DAG requirements
COPY requirements.txt /requirements.txt

# Install the requirements
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to the Airflow user
USER airflow