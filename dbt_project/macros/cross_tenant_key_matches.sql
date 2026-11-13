{#
  Child rows whose key has no parent in the SAME brand but does exist in the parent under ANOTHER
  brand: exactly what a join without brand_id would silently resolve to another tenant's row.
  child/parent are relations (ref/source results). `label` is required. Returns: relationship, brand_id, key_value.
#}
{% macro cross_tenant_key_matches(child, child_column, parent, parent_column, brand_column='brand_id', label=none) %}
{% if label is none %}
  {{ exceptions.raise_compiler_error('cross_tenant_key_matches requires a label') }}
{% endif %}
select
    '{{ label }}' as relationship,
    c.{{ brand_column }} as brand_id,
    cast(c.{{ child_column }} as string) as key_value
from {{ child }} as c
where c.{{ child_column }} is not null
  and not exists (
      select 1 from {{ parent }} as p
      where p.{{ brand_column }} = c.{{ brand_column }}
        and p.{{ parent_column }} = c.{{ child_column }}
  )
  and exists (
      select 1 from {{ parent }} as p
      where p.{{ brand_column }} <> c.{{ brand_column }}
        and p.{{ parent_column }} = c.{{ child_column }}
  )
{% endmacro %}
