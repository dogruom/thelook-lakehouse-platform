select
    cast(brand_id as string) as brand_id,
    cast(order_id as string) as order_id,
    cast(customer_id as string) as customer_id
from values
    ('brand_a', 'o1', 'c1'),
    ('brand_b', 'o2', 'c2'),
    ('brand_b', 'o3', 'c1'),
    ('brand_a', 'o4', 'c9') as t(brand_id, order_id, customer_id)
