with

    raw_customers as (
        select * from {{ source("jaffle_shop", "customers") }}
    ),

    raw_orders as (
        select * from {{ source("jaffle_shop", "orders") }}
    ),

    raw_payments as ( 
        select * from {{ source("stripe", "payment") }}
    ),

    orders as (
        select
            id as order_id,
            user_id as customer_id,
            order_date as order_placed_at,
            status as order_status
        from raw_orders
    ),
     
    customers as (
        select  
            id as customer_id,
            first_name as customer_first_name,
            last_name as customer_last_name
        from raw_customers
    ),

    -- Logical CTEs
    order_amount as (
        select
            orderid as order_id,
            max(created) as payment_finalized_date,
            sum(amount) / 100.0 as total_amount_paid
        from raw_payments
        where status <> 'fail'
        group by 1
    ),

    paid_orders as (
        select
            o.order_id,
            o.customer_id,
            o.order_placed_at,
            o.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.customer_first_name,
            c.customer_last_name
        from orders as o
        left join order_amount as p on o.order_id = p.order_id
        left join customers as c on o.customer_id = c.customer_id
    ),

    customer_orders as (
        select
            c.customer_id,
            min(o.order_placed_at) as first_order_date,
            max(o.order_placed_at) as most_recent_order_date,
            count(o.order_id) as number_of_orders
        from customers as c
        left join orders as o on o.customer_id = c.customer_id
        group by 1
    ),

    orders_clv_bad as (
        select 
            p.order_id, 
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join
            paid_orders t2
            on p.customer_id = t2.customer_id
            and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ),

    -- Final CTE
    final as (
        select
            p.*,
            row_number() over (order by p.order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by p.order_id
            ) as customer_sales_seq,
            case
                when c.first_order_date = p.order_placed_at then 'new' else 'return'
            end as nvsr,
            x.clv_bad as customer_lifetime_value,
            c.first_order_date as fdos
        from paid_orders p
        left join customer_orders as c using (customer_id)
        left outer join orders_clv_bad as x on x.order_id = p.order_id
        order by order_id
    )
-- Simple Select Statment
select *
from final
