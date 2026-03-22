{{ config(materialized='table') }}

with sales as (

    select * from {{ ref('stg_sales') }}

),

summary as (

    select
        region,
        category,
        sale_date,
        count(sale_id)               as total_transactions,
        sum(amount_usd)              as total_revenue_usd,
        avg(amount_usd)              as avg_order_value,
        min(amount_usd)              as min_sale,
        max(amount_usd)              as max_sale,
        sum(case when value_tier = 'High Value'
            then 1 else 0 end)       as high_value_count

    from sales
    group by region, category, sale_date

)

select * from summary
order by sale_date desc, total_revenue_usd desc
