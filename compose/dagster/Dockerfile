# Use a slim Python 3.12 image
FROM python:3.12.10-slim-bookworm

# Set Dagster home and working directory
ENV DAGSTER_HOME=/app/dagster/
WORKDIR $DAGSTER_HOME

ENV DBT_PROJECT_DIR=${DAGSTER_HOME}dbt
ENV DBT_PROFILES_DIR=${DBT_PROJECT_DIR}/config

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev git g++ cmake libssl-dev wget gettext \
    postgresql-client dos2unix && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN python -m pip install uv

# Install Python dependencies
COPY ./requirements .
RUN uv pip install --system -r dagster.txt

# Copy and prepare entrypoint and start scripts
COPY ./compose/dagster/entrypoint /entrypoint
COPY ./compose/dagster/start /start
RUN dos2unix /entrypoint /start && chmod +x /entrypoint /start

# Set entrypoint
ENTRYPOINT ["/entrypoint"]
