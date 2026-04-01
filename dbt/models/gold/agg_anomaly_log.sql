{{ config(materialized='table', schema='gold') }}

select
    order_id,
    order_date,
    event_timestamp,
    city,
    state,
    category,
    total_amount_inr,
    payment_method,
    anomaly_reason,

    case
        when total_amount_inr > 50000 then 'critical'
        when total_amount_inr > 15000 then 'high'
        else 'medium'
    end                 as severity,

    loaded_at

from {{ ref('fct_orders') }}
where is_anomalous = true
order by event_timestamp desc