# Use slim python; wheels for pandas/numpy install fine without full build chain
FROM python:3.12-slim

# Safer defaults
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ gnupg curl unixodbc unixodbc-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*


# 1) Install Python deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Install ODBC driver & dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    gnupg \
    curl \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2) Copy app code
COPY market_pipeline/ ./market_pipeline/
COPY scripts/ ./scripts/
COPY config/ ./config/
COPY app.py ./
COPY data/ ./data/

# 3) Create runtime directories instead of COPYing (they may not exist in repo)
RUN mkdir -p data logs && \
    adduser --disabled-password --gecos '' appuser && \
    chown -R appuser:appuser /app

# Expose Flask port
EXPOSE 5000

# Start app
CMD ["python", "app.py"]

# USER appuser
# EXPOSE 8080
#
# # gunicorn server for Railway
# CMD ["gunicorn", "app:app", "-w", "2", "-k", "gthread", "--threads", "4", "--timeout", "600", "-b", "0.0.0.0:8080"]
