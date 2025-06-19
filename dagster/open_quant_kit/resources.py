import os

from dagster import resource
from sqlalchemy import create_engine


@resource
def dbt_postgres(init_context):
    """
    This resource provides a connection to the PostgreSQL database used by dbt.
    """
    db_url = os.getenv('POSTGRES_DB_URL').replace("postgres://", "postgresql://")
    engine = create_engine(db_url)
    return engine

