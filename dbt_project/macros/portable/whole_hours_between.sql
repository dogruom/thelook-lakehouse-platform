{#
  Whole hours from `start_ts` to `end_ts`, truncated toward zero (floor for end >= start).
  dbt.datediff('hour') is NOT portable here: dbt-spark rounds UP (ceil) when start < end, while
  Trino/Athena date_diff truncates (W4.D21 review, D-097). The `default__` (Trino/Athena) variant
  is UNTESTED until the Athena track (D-093); only `spark__` runs today.
#}
{% macro whole_hours_between(start_ts, end_ts) %}
    {{ return(adapter.dispatch('whole_hours_between', 'crm_lakehouse')(start_ts, end_ts)) }}
{% endmacro %}

{% macro spark__whole_hours_between(start_ts, end_ts) %}
    cast(floor((unix_timestamp({{ end_ts }}) - unix_timestamp({{ start_ts }})) / 3600) as {{ dbt.type_bigint() }})
{% endmacro %}

{% macro default__whole_hours_between(start_ts, end_ts) %}
    cast(date_diff('hour', {{ start_ts }}, {{ end_ts }}) as {{ dbt.type_bigint() }})
{% endmacro %}
