{{ config(materialized='table', schema='bronze') }}

select
    order_id,
    event_timestamp,
    customer_id,
    customer_name,
    cast(customer_tier as int64)          as customer_tier,
    city,
    state,
    pincode,
    category,
    subcategory,
    cast(quantity as int64)               as quantity,
    cast(unit_price_inr as float64)       as unit_price_inr,
    cast(total_amount_inr as float64)     as total_amount_inr,
    payment_method,
    warehouse_id,
    cast(estimated_days as int64)         as estimated_days,
    cast(is_express as bool)              as is_express,
    cast(expected_return_rate as float64) as expected_return_rate,
    cast(is_anomalous as bool)            as is_anomalous,
    anomaly_reason,
    ingestion_id,
    consumed_at,
    date(timestamp(event_timestamp))      as event_date,
    current_timestamp()                   as loaded_at

from {{ source('raw', 'orders_raw') }}