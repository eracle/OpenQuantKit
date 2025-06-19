# dagster/open_quant_kit/repository.py
from dagster import Definitions

from .jobs import jobs
from .resources import dbt_postgres

from .schedules import schedules


# List of assets
assets = [

]

# Define assets, jobs, schedules, etc.
defs = Definitions(
    assets=assets,
    jobs=jobs,
    schedules=schedules,
    resources={
        "dbt_postgres": dbt_postgres,
    },
)
