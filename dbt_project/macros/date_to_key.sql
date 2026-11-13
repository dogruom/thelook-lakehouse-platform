{#
  Integer yyyymmdd key from a date/timestamp expression, using only ANSI `extract` (portable).
  Time zone: timestamps are cast to date in the session time zone, so date keys assume a UTC
  session (`spark.sql.session.timeZone` = UTC; pinned on the Thrift Server). A different session
  zone would shift keys for events near midnight.
#}
{% macro date_to_key(expr) %}
cast(
    extract(year from cast({{ expr }} as date)) * 10000
    + extract(month from cast({{ expr }} as date)) * 100
    + extract(day from cast({{ expr }} as date))
    as {{ dbt.type_int() }}
)
{% endmacro %}
