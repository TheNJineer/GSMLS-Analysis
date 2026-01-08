FROM python:3.11-slim

# 1. System deps (only if needed)
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR /app

# 3. Install Python deps (cache-friendly)
COPY jobs/gsmls_cleaning/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy job code + plugins
COPY jobs/gsmls_cleaning .

# 5. Entrypoint
ENTRYPOINT ["python", "/app/jobs/gsmls_cleaning/phased_cleaning.py"]