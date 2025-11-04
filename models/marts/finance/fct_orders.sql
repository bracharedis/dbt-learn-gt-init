with orders as (
    select * from {{ ref('stg_jaffle_shop__orders')}}
),
payments as (
    select * from {{ ref('stg_stripe__payment')}}
),
order_payments as (
    select
        order_id,
        sum (case when status = 'success' then amount end) as amount
    from payments
    group by 1
),
final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status as order_status,
        p.payment_id,
        p.payment_method,
        p.status as payment_status,
        p.amount as payment_amount,
        p.created as payment_created
    from orders o
    left join payments p on o.order_id = p.order_id
)
select * from final
