FROM python:3.10-slim

# Install system dependencies for pyarrow, psycopg2, and compiling tools
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    curl \
    wget \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Set default command (optional, replace with your actual entrypoint if needed)
CMD ["python", "ingest_data.py"]