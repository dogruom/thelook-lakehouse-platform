{#
  RFM segment from three 1..5 scores (5 = best). fm = f + m (2..10). Rules, first match wins:
    champions    r >= 4 and fm >= 8
    loyal        r >= 3 and fm >= 6
    promising    r >= 4            (recent, low value so far)
    at_risk      r <= 2 and fm >= 6 (valuable, not seen lately)
    hibernating  everything else
  Portable CASE expression (Spark and Athena). Changing a threshold is a decision-log change.
#}
{% macro rfm_segment(r, f, m) -%}
case
    when {{ r }} >= 4 and ({{ f }} + {{ m }}) >= 8 then 'champions'
    when {{ r }} >= 3 and ({{ f }} + {{ m }}) >= 6 then 'loyal'
    when {{ r }} >= 4 then 'promising'
    when {{ r }} <= 2 and ({{ f }} + {{ m }}) >= 6 then 'at_risk'
    else 'hibernating'
end
{%- endmacro %}
