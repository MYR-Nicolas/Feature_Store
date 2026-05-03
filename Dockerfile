FROM python:3.13-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy project
COPY . .

# Install python deps
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt BigQuery
RUN pip install dbt-bigquery

ENV PYTHONUNBUFFERED=1

# Entrypoint
CMD ["python", "-m", "ELT.pipeline"]