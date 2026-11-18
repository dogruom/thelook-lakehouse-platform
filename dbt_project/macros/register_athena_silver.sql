{#
  run-operation (Athena target only, D-104): register the Spark-written Silver Delta tables for
  Athena. Spark/Glue registers its Delta tables in the Glue database `silver` as provider=delta
  with a placeholder schema, which Athena cannot read. Athena (engine v3) reads Delta natively
  only for tables created as
      CREATE EXTERNAL TABLE <db>.<t> LOCATION 's3://.../silver/<t>' TBLPROPERTIES ('table_type'='DELTA')
  so one such table per Silver source is created in the separate Glue database `athena_silver`
  (the schema the `silver` source resolves to on Athena). Athena derives columns from the Delta log,
  so run this after the Silver tables exist; it is idempotent (IF NOT EXISTS) and only reads S3
  metadata. The Glue database `athena_silver` is created by the Terraform catalog module.

    dbt run-operation register_athena_silver --target athena --profiles-dir profiles \
        --args '{}' --vars '{athena_silver_root: s3://crm-lakehouse-dev-<account_id>/silver}'

  Refuses to run on any other adapter or while var('athena_silver_root') is unset. Names and the
  S3 root are validated before any SQL is rendered, so a var cannot inject SQL.
#}
{% macro register_athena_silver() %}
  {%- if target.type != 'athena' -%}
    {{ exceptions.raise_compiler_error("register_athena_silver runs on the athena target only (target.type is '" ~ target.type ~ "')") }}
  {%- endif -%}
  {%- set root = (var('athena_silver_root', '') | string).rstrip('/') -%}
  {%- if root == '' -%}
    {{ exceptions.raise_compiler_error("register_athena_silver: set --vars '{athena_silver_root: s3://<data bucket>/silver}'") }}
  {%- endif -%}
  {%- if not modules.re.match('^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/[A-Za-z0-9._-]+)*\Z', root) -%}
    {{ exceptions.raise_compiler_error("register_athena_silver: invalid athena_silver_root '" ~ root ~ "'") }}
  {%- endif -%}
  {%- set tables = [] -%}
  {%- for node in graph.sources.values() | selectattr('source_name', 'equalto', 'silver') | sort(attribute='name') -%}
    {%- do tables.append(node) -%}
  {%- endfor -%}
  {%- if tables | length == 0 -%}
    {{ exceptions.raise_compiler_error("register_athena_silver: no tables found in source 'silver'") }}
  {%- endif -%}
  {%- for node in tables -%}
    {%- if not modules.re.match('^[a-z][a-z0-9_]{0,63}\Z', node.name) or not modules.re.match('^[a-z][a-z0-9_]{0,63}\Z', node.schema) -%}
      {{ exceptions.raise_compiler_error("register_athena_silver: invalid name '" ~ node.schema ~ "." ~ node.name ~ "'") }}
    {%- endif -%}
    {%- call statement('register_' ~ node.name, fetch_result=False, auto_begin=False) -%}
      create external table if not exists {{ node.schema }}.{{ node.name }}
      location '{{ root }}/{{ node.name }}'
      tblproperties ('table_type'='DELTA')
    {%- endcall -%}
    {{ log('registered ' ~ node.schema ~ '.' ~ node.name ~ ' -> ' ~ root ~ '/' ~ node.name, info=True) }}
  {%- endfor -%}
{% endmacro %}
