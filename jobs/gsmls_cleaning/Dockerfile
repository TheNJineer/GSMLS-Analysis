FROM python:3.11-slim

# 1. System deps (only if needed)
RUN apt-get update && apt-get install -y \
    git \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Set working directory
WORKDIR /app

# 3. Copys requirements.txt and jobs script. Install Python deps (cache-friendly)
# Build context jobs/gsmls_cleaning
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Entrypoint
ENTRYPOINT ["python", "/app/jobs/gsmls_cleaning/phased_cleaning.py"]