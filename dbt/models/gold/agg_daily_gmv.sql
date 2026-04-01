{{ config(materialized='table', schema='gold') }}

select
    order_date,
    state,
    category,
    payment_method,

    count(*)                                            as order_count,
    sum(quantity)                                       as units_sold,
    round(sum(total_amount_inr), 2)                     as gmv_inr,
    round(avg(total_amount_inr), 2)                     as aov_inr,

    -- payment mix
    countif(is_cod)                                     as cod_orders,
    countif(is_upi)                                     as upi_orders,
    round(safe_divide(countif(is_cod), count(*)), 3)    as cod_ratio,

    -- risk signals
    countif(is_high_cod_risk)                           as high_cod_risk_orders,
    countif(is_high_return_risk)                        as high_return_risk_orders,
    countif(is_tier3_customer)                          as tier3_orders,

    -- day-over-day GMV change using window function
    round(sum(total_amount_inr), 2)
        - lag(round(sum(total_amount_inr), 2)) over (
            partition by state, category
            order by order_date
        )                                               as dod_gmv_change_inr,

    -- week-over-week
    round(sum(total_amount_inr), 2)
        - lag(round(sum(total_amount_inr), 2), 7) over (
            partition by state, category
            order by order_date
        )                                               as wow_gmv_change_inr

from {{ ref('fct_orders') }}
group by 1, 2, 3, 4