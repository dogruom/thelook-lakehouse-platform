{#
  ISO day of week for a date expression: 1 = Monday ... 7 = Sunday.
  Engine-specific, so it goes through adapter.dispatch. The `default__` (Trino/Athena) variant is
  UNTESTED until the Athena track (D-093); only `spark__` runs today.
#}
{% macro iso_day_of_week(expr) %}
    {{ return(adapter.dispatch('iso_day_of_week', 'crm_lakehouse')(expr)) }}
{% endmacro %}

{% macro spark__iso_day_of_week(expr) %}
    cast(((dayofweek(cast({{ expr }} as date)) + 5) % 7) + 1 as {{ dbt.type_int() }})
{% endmacro %}

{% macro default__iso_day_of_week(expr) %}
    cast(day_of_week(cast({{ expr }} as date)) as {{ dbt.type_int() }})
{% endmacro %}
