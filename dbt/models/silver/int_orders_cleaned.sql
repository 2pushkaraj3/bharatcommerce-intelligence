{{ config(materialized='table', schema='silver') }}

with source as (
    select * from {{ ref('stg_orders_raw') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by order_id
        order by loaded_at desc
    ) = 1
),

cleaned as (
    select
        order_id,
        timestamp(event_timestamp)                          as event_timestamp,
        date(timestamp(event_timestamp))                    as order_date,
        customer_id,
        initcap(trim(customer_name))                        as customer_name,
        customer_tier,

        -- geography
        trim(city)                                          as city,
        trim(state)                                         as state,
        trim(pincode)                                       as pincode,

        -- product
        trim(category)                                      as category,
        trim(subcategory)                                   as subcategory,
        greatest(quantity, 1)                               as quantity,
        round(unit_price_inr, 2)                            as unit_price_inr,
        round(total_amount_inr, 2)                          as total_amount_inr,

        -- payment signals — core India ecom metrics
        payment_method,
        payment_method = 'COD'                              as is_cod,
        payment_method = 'UPI'                              as is_upi,

        -- COD above Rs3000 = elevated non-delivery risk
        (payment_method = 'COD'
            and total_amount_inr > 3000)                    as is_high_cod_risk,

        -- tier 3 city flag
        customer_tier = 3                                   as is_tier3_customer,

        -- fashion + tier3 + COD = highest return probability combo
        (category = 'Fashion'
            and customer_tier = 3
            and payment_method = 'COD')                     as is_high_return_risk,

        -- order value bucket
        case
            when total_amount_inr < 500   then 'low'
            when total_amount_inr < 2000  then 'mid'
            when total_amount_inr < 10000 then 'high'
            else 'premium'
        end                                                 as order_value_tier,

        -- risk / anomaly
        is_anomalous,
        trim(anomaly_reason)                                as anomaly_reason,
        expected_return_rate,

        -- fulfilment
        warehouse_id,
        estimated_days,
        is_express,

        ingestion_id,
        loaded_at

    from deduped
    where
        order_id           is not null
        and customer_id    is not null
        and total_amount_inr > 0
        and total_amount_inr < 500000
        and state          is not null
)

select * from cleaned