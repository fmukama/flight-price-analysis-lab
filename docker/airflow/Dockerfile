# Official Apache Airflow base image (Python 3.11)
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare after FROM: ARGs before FROM are not visible in the build stage
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Switch to root to install OS-level build tools and database client libraries
USER root

# Install system dependencies needed for compiling psycopg2 and mysql client
# drivers. postgresql-client supplies the `psql` binary that airflow-init uses
# to create the Airflow metadata database before running migrations.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow non-root user to install Python packages securely
USER airflow

# Copy requirements file into container
COPY requirements.txt /requirements.txt

# Install custom Python libraries under the official Airflow constraints file.
# The constraints are what stop a transitive dependency from dragging the
# Airflow core to an incompatible major version (see requirements.txt header).
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Fail the build loudly if the core was moved or the CLI entrypoint is missing,
# rather than shipping an image that crash-loops at runtime.
RUN airflow version \
    && python -c "import airflow, sqlalchemy, sys; \
assert airflow.__version__ == '2.9.2', f'airflow {airflow.__version__} != 2.9.2'; \
assert sqlalchemy.__version__.startswith('1.4'), f'sqlalchemy {sqlalchemy.__version__} must be 1.4.x'; \
print(f'verified airflow={airflow.__version__} sqlalchemy={sqlalchemy.__version__}')"
