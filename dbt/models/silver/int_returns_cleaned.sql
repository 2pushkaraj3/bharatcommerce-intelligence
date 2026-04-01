{{ config(materialized='table', schema='silver') }}

with source as (
    select * from {{ ref('stg_returns_raw') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by return_id
        order by loaded_at desc
    ) = 1
)

select
    return_id,
    order_id,
    timestamp(event_timestamp)          as event_timestamp,
    date(timestamp(event_timestamp))    as return_date,
    customer_id,
    trim(city)                          as city,
    trim(state)                         as state,
    trim(category)                      as category,
    trim(return_reason)                 as return_reason,
    round(refund_amount_inr, 2)         as refund_amount_inr,
    trim(refund_method)                 as refund_method,
    ingestion_id,
    loaded_at

from deduped
where return_id is not null