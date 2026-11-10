{#
  Writes a model straight to one Parquet file with a streaming COPY, then
  exposes the file as a view so dbt tests can query it.

  dbt-duckdb's built-in `external` materialization first builds the whole
  result as a table inside DuckDB and copies it afterwards. For a month of
  events (~90M rows) that table spilled 20+ GB to disk. COPY (query) streams
  instead, so memory and disk stay small.
#}
{% materialization parquet_partition, adapter='duckdb' %}
  {%- set location = config.require('location') -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    copy ({{ sql }}) to '{{ location }}' (format parquet, compression zstd)
  {%- endcall %}

  {% call statement('expose') -%}
    create or replace view {{ target_relation }} as
    select * from read_parquet('{{ location }}')
  {%- endcall %}

  {{ run_hooks(post_hooks) }}
  {{ adapter.commit() }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
