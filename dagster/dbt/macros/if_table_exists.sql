-- macros/if_table_exists.sql
{% macro if_table_exists(schema, table) %}
  {% set query %}
    select to_regclass('{{ schema }}.{{ table }}') is not null as exists
  {% endset %}
  {% set result = run_query(query) %}
  {% if execute %}
    {% set exists = result.columns[0].values()[0] %}
    {{ return(exists) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}
