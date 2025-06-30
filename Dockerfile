# Dockerfile

FROM python:3.11-slim

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

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose Flask port
EXPOSE 5000

# Start the Flask app
CMD ["python", "app.py"]
