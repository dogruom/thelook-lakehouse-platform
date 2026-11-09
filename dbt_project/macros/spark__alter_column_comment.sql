{#
  Override of dbt-spark 1.11.0's macro (W2.D11 spike finding, ADR-0004, D-060).
  The upstream macro sends each ALTER TABLE ... CHANGE COLUMN ... COMMENT through run_query(),
  which fetches a result set; over the thrift method (PyHive) a DDL returns none and dbt fails
  with "'NoneType' object is not iterable", so persist_docs(columns=true) breaks every Delta
  model. Same SQL, executed as a statement without fetching. Remove when upstream is fixed.
#}
{% macro spark__alter_column_comment(relation, column_dict) %}
  {% if config.get('file_format', validator=validation.any[basestring]) in ['delta', 'hudi', 'iceberg'] %}
    {% for column_name in column_dict %}
      {% set comment = column_dict[column_name]['description'] %}
      {% set escaped_comment = comment | replace("'", "\\'") %}
      {% set column = adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name %}
      {% call statement('alter_column_comment_' ~ loop.index, fetch_result=False) %}
        {% if relation.is_iceberg %}
          alter table {{ relation }} alter column {{ column }} comment '{{ escaped_comment }}'
        {% else %}
          alter table {{ relation }} change column {{ column }} comment '{{ escaped_comment }}'
        {% endif %}
      {% endcall %}
    {% endfor %}
  {% endif %}
{% endmacro %}
