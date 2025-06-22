# dagster/open_quant_kit/repository.py
from dagster import Definitions
from .assets import assets
from .dbt import dbt_resource

from .jobs import jobs
from .resources import dbt_postgres

from .schedules import schedules



# Define assets, jobs, schedules, etc.
defs = Definitions(
    assets=assets,
    jobs=jobs,
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
        "dbt_postgres": dbt_postgres,
    },
)
