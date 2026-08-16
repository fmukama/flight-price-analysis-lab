# Official Apache Airflow base image (Python 3.11)
FROM apache/airflow:2.9.2-python3.11

# Switch to root to install OS-level build tools and database client libraries
USER root

# Install system dependencies needed for compiling psycopg2 and mysql client drivers
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow non-root user to install Python packages securely
USER airflow

# Copy requirements file into container
COPY requirements.txt /requirements.txt

# Install custom Python libraries without caching wheels to keep image slim
RUN pip install --no-cache-dir -r /requirements.txt