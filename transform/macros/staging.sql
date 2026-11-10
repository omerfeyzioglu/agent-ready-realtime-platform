{# Where staged Parquet files live: s3://.../lake/staging in prod, a local
   folder in dev. run_staging.py always sets STAGING_ROOT. #}
{% macro staging_root() %}
  {{ return(env_var('STAGING_ROOT')) }}
{% endmacro %}

{# One file per monthly partition; rebuilding a month overwrites its file. #}
{% macro partition_location(table, key, month) %}
  {{ return(staging_root() ~ '/' ~ table ~ '/' ~ key ~ '=' ~ month ~ '/data.parquet') }}
{% endmacro %}

{# TLC flag columns hold 'Y' / 'N' (sometimes blank): blank is unknown, not false. #}
{% macro yn(expr) %}
  case upper(trim({{ expr }})) when 'Y' then true when 'N' then false end
{% endmacro %}

{# Half-open month window on a timestamptz column. #}
{% macro in_month(col, month) %}
  {{ col }} >= timestamptz '{{ month }}-01'
  and {{ col }} < timestamptz '{{ month }}-01' + interval 1 month
{% endmacro %}

{# Optional narrower window used for quick local test runs. #}
{% macro slice_filter(col) %}
  {% if var('slice_from') %}
    and {{ col }} >= timestamptz '{{ var("slice_from") }}'
    and {{ col }} < timestamptz '{{ var("slice_to") }}'
  {% endif %}
{% endmacro %}

{# NYC zone ids without a real place: 264 = Unknown, 265 = Outside of NYC. #}
{% macro zone_unknown(expr) %}coalesce({{ expr }} = 264, false){% endmacro %}
{% macro zone_outside_nyc(expr) %}coalesce({{ expr }} = 265, false){% endmacro %}

{# NYC fall-back dates (01:00-01:59 local happens twice) in the data range. #}
{% macro fall_back_dates() %}
  date '2023-11-05', date '2024-11-03', date '2025-11-02', date '2026-11-01'
{% endmacro %}
