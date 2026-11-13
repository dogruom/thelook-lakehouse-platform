select
    cast(brand_id as {{ dbt.type_string() }}) as brand_id,
    cast(customer_id as {{ dbt.type_string() }}) as customer_id
from (
    values
        ('brand_a', 'c1'),
        ('brand_a', 'c2'),
        ('brand_b', 'c2')
) as t(brand_id, customer_id)
