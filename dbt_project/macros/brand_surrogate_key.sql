{#
  Deterministic tenant-safe surrogate key: hash of the given columns, each cast to string, NULL
  replaced by a sentinel, joined with a separator. Every Gold surrogate key must include `brand_id`
  (ids are unique only per brand); the only exception is the conformed non-tenant `dim_date`.
  Built from dbt cross-database macros (dbt.hash / dbt.concat / dbt.type_string), so it is portable
  across Spark and Athena. The result is a hex string. Keys are deterministic within one engine;
  cross-engine equality is unverified (timestamp-to-string formats differ) and is checked in
  W5.D28.4.
  Usage: {{ brand_surrogate_key(['brand_id', 'customer_id']) }}
#}
{% macro brand_surrogate_key(columns) %}
{%- set ns = namespace(has_brand=false) -%}
{%- for col in columns -%}
  {%- if col == 'brand_id' or col.endswith('.brand_id') -%}{%- set ns.has_brand = true -%}{%- endif -%}
{%- endfor -%}
{%- if not ns.has_brand -%}
  {{ exceptions.raise_compiler_error("brand_surrogate_key: the column list must include 'brand_id'") }}
{%- endif -%}
{%- set parts = [] -%}
{%- for col in columns -%}
  {%- do parts.append("coalesce(cast(" ~ col ~ " as " ~ dbt.type_string() ~ "), '_null_')") -%}
  {%- if not loop.last -%}{%- do parts.append("'|'") -%}{%- endif -%}
{%- endfor -%}
{{ dbt.hash(dbt.concat(parts)) }}
{% endmacro %}
