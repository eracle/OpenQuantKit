{% set raw_filing_exists = if_table_exists('public', 'raw_filing') %}

with filings as (
  select *
  from {{ source('sec_data', 'raw_filing_index') }}
  where form_type in ('10-K', '10-Q')
),

ciks as (
  select *
  from {{ source('sec_data', 'dim_cik') }}
)

{% if raw_filing_exists %}
, raw as (
  select *
  from {{ source('sec_data', 'raw_filing') }}
)
{% endif %}

select
  c.ticker,
  f.cik,
  f.form_type,
  f.date_filed,
  f.year,
  f.quarter,
  f.filename,
  'https://www.sec.gov/Archives/' || f.filename as full_url
from filings f
join ciks c on f.cik = c.cik

{% if raw_filing_exists %}
left join raw r
  on c.ticker = r.ticker
  and f.form_type = r.form_type
  and f.date_filed = r.filing_date
where r.ticker is null
{% endif %}
