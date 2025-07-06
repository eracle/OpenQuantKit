-- models/dim_ticker_cik.sql

-- Normalize ticker symbols from both sources
with dim_ticker_clean as (
    select
        lower(trim(symbol)) as ticker
    from {{ ref('dim_ticker') }}
),

dim_cik_clean as (
    select
        lower(trim(ticker)) as ticker,
        cast(cik as text) as cik,  -- ensure CIK is text with leading zeros
        title
    from {{ source('sec_data', 'dim_cik') }}
)

-- Join the two on normalized ticker
select
    t.ticker,
    c.cik,
    c.title
from dim_ticker_clean t
inner join dim_cik_clean c
    on t.ticker = c.ticker
