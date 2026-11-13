{#
  Tenant-safe foreign-key check. The built-in `relationships` test matches on the key alone: in
  multi-tenant data ids are unique only per brand, so a child row could be "satisfied" by a parent
  with the same id in ANOTHER brand. This test anti-joins on brand AND key.
  Returns child rows (non-null key) without a parent in the same brand.
#}
{% test relationships_by_brand(model, column_name, to, field, brand_column='brand_id', to_brand_column='brand_id') %}
select c.{{ brand_column }} as brand_id, c.{{ column_name }} as key_value
from {{ model }} as c
left join {{ to }} as p
    on c.{{ brand_column }} = p.{{ to_brand_column }}
   and c.{{ column_name }} = p.{{ field }}
where c.{{ column_name }} is not null
  and p.{{ field }} is null
{% endtest %}
