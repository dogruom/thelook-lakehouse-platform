{#
  Post-hook: one view per configured tenant over the model just built,
    <target.schema>_tenant.<model>__<brand_id> = select * from <model> where brand_id = '<brand_id>'
  (e.g. gold_tenant.obt_customer_360__brand_alpha). Brands come from var('tenant_brands') and are
  validated against the brand_id contract pattern before any SQL is rendered, so a var cannot
  inject SQL. The views are a consumer convenience (one tenant per BI connection), NOT access
  control: anyone who can read gold_marts reads every tenant (same stance as the Silver views,
  D-085). Idempotent (create or replace). Each statement runs on its own (adapter.execute): the Thrift
  Server takes one statement per call; the hook itself renders to an empty string.
#}
{% macro tenant_scoped_views() %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set schema = target.schema ~ '_tenant' -%}
  {%- set statements = ['create schema if not exists ' ~ schema] -%}
  {%- for brand in var('tenant_brands') -%}
    {%- if not modules.re.match('^brand_[a-z0-9_]+$', brand) -%}
      {{ exceptions.raise_compiler_error("tenant_scoped_views: invalid brand_id '" ~ brand ~ "'") }}
    {%- endif -%}
    {%- do statements.append(
      'create or replace view ' ~ schema ~ '.' ~ this.identifier ~ '__' ~ brand
      ~ ' as select * from ' ~ this ~ " where brand_id = '" ~ brand ~ "'"
    ) -%}
  {%- endfor -%}
  {%- for statement in statements -%}
    {%- do adapter.execute(statement, fetch=False) -%}
  {%- endfor -%}
  {{ return('') }}
{% endmacro %}
