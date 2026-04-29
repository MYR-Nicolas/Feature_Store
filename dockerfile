FROM python:3.13.9-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*
    
# Copy project
COPY . .

# Install python deps
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt
RUN pip install dbt-postgres

# Entrypoint
CMD ["python", "-m", "ELT.pipeline"]