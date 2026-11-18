-- No staging view or Gold mart exposes a raw-PII column (Charter §3.7: raw PII never reaches
-- Gold; Silver keeps names raw, staging deliberately does not select them). Checked against the
-- built relations' real columns, so a `select *` or a new model cannot slip one in. Pseudonymised
-- forms (`email_token`, `phone_masked`) are allowed by design.
-- Fails with one row per offending column, and one row per staging/marts model that is missing
-- from the list below (so the guard cannot silently stop covering a new model).
{%- set checked = [
    'stg_clickstream', 'stg_customers', 'stg_customers_history', 'stg_order_items', 'stg_orders',
    'stg_products', 'dim_customers', 'dim_customers_history', 'dim_date', 'dim_products',
    'fct_order_items', 'fct_orders', 'fct_sessions', 'obt_customer_360', 'agg_rfm_segments',
] %}
{%- set forbidden = [
    'first_name', 'last_name', 'full_name', 'name_first', 'name_last', 'email', 'email_address',
    'phone', 'phone_number', 'street', 'street_address', 'address',
] %}
{%- for model_name in checked %}
-- depends_on: {{ ref(model_name) }}
{%- endfor %}
{%- set violations = [] %}
{%- if execute %}
  {%- for model_name in checked %}
    {%- for column in adapter.get_columns_in_relation(ref(model_name)) %}
      {%- if column.name | lower in forbidden %}
        {%- do violations.append((model_name, column.name | lower)) %}
      {%- endif %}
    {%- endfor %}
  {%- endfor %}
  {%- for node in graph.nodes.values() %}
    {%- if node.resource_type == 'model' and node.fqn[1] in ('staging', 'marts')
          and node.name not in checked %}
      {%- do violations.append((node.name, '<model not covered by this test>')) %}
    {%- endif %}
  {%- endfor %}
{%- endif %}
{%- if violations %}
  {%- for model_name, column_name in violations %}
select '{{ model_name }}' as model_name, '{{ column_name }}' as column_name
{%- if not loop.last %} union all {% endif %}
  {%- endfor %}
{%- else %}
select cast(null as string) as model_name, cast(null as string) as column_name
where 1 = 0
{%- endif %}
