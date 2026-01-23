FROM python:3.11-slim

# 1. System deps (only if needed)
RUN apt-get update && apt-get install -y \
    git \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR /app
RUN mkdir logs downloads pipeline_metadata consumer_backup_data parquet_files

# 3. Copys requirements.txt and jobs script. Install Python deps (cache-friendly)
# Build context jobs/gsmls
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Entrypoint
ENTRYPOINT ["python"]