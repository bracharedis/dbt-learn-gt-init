with orders as (

    select * from {{ ref('stg_jaffle_shop_orders')}}

),

payments as (

    select * from {{ ref('stg_stripe_payment')}}

),


final as (

    select
        orders.customer_id,
        sum(payments.amount) as lifetime_value
    
    from orders

    left join payments on orders.order_id = payments.order_id
    group by 1
)   
select * from final