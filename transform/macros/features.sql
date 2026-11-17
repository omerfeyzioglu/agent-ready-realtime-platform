{# Feature tables sit next to staging: .../lake/features/<table>/<key>=YYYY-MM/data.parquet #}
{% macro feature_location(table, key, month) %}
  {{ return(staging_root() | replace('/staging', '/features') ~ '/' ~ table ~ '/' ~ key ~ '=' ~ month ~ '/data.parquet') }}
{% endmacro %}
