{{ config(materialized='table', schema='bronze') }}

select
    return_id,
    order_id,
    event_timestamp,
    customer_id,
    city,
    state,
    category,
    return_reason,
    cast(refund_amount_inr as float64) as refund_amount_inr,
    refund_method,
    ingestion_id,
    consumed_at,
    current_timestamp()                as loaded_at

from {{ source('raw', 'returns_raw') }}