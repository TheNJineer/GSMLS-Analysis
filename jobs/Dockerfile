FROM python:3.11-slim-bookworm

# 1. System deps (only if needed)
# -------------------------
# Install system dependencies
# -------------------------
RUN apt-get update && apt-get install -y \
    git \
    curl \
    gcc \
    openjdk-17-jre-headless \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR /app
RUN mkdir logs jobs downloads pipeline_metadata consumer_backup_data parquet_files

# -------------------------
# Spark local installation. Use if necessary
# Currently connecting to Spark Container on docker network
# -------------------------
#ENV SPARK_VERSION=3.5.1
#ENV HADOOP_VERSION=3
#
#RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
#    | tar -xz -C /opt/ \
#    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
#
#ENV SPARK_HOME=/opt/spark
#
#ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
#RUN cp /opt/spark/conf/spark-env.sh.template /opt/spark/conf/spark-env.sh && \
#    echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64" >> /opt/spark/conf/spark-env.sh

# -------------------------
# Environment variables
# -------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python

ENV PYTHONNOUSERSITE=1
ENV PYTHONPATH=/app

# -------------------------
# Python dependencies
# -------------------------

# 3. Copys requirements.txt and jobs script. Install Python deps (cache-friendly)
# Build context jobs/gsmls
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Entrypoint
ENTRYPOINT ["python"]