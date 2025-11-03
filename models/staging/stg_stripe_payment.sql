select id as payment_id
    ,orderid as order_id
    ,paymentmethod as payment_method
    ,status
    ,amount / 100 as amount  -- in cents convert to dollars
    ,created
    ,_batched_at
from raw.stripe.payment