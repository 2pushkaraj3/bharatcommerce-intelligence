{{ config(materialized='table', schema='gold') }}

-- COD risk score: the flagship metric of this platform
-- Formula: return_risk(40%) + cod_ratio(30%) + tier3_ratio(30%)
-- Derived from real Meesho/Flipkart industry risk logic

select
    state,

    count(*)                                                        as total_orders,
    round(sum(total_amount_inr), 0)                                 as total_gmv_inr,
    round(avg(total_amount_inr), 0)                                 as aov_inr,

    round(safe_divide(countif(is_cod), count(*)) * 100, 1)         as cod_pct,
    round(safe_divide(countif(is_tier3_customer), count(*)) * 100, 1) as tier3_pct,
    round(safe_divide(countif(is_high_return_risk), count(*)) * 100, 1) as high_return_risk_pct,

    -- COD risk score (0 to 1, higher = riskier)
    round(
        (safe_divide(countif(is_high_return_risk), count(*)) * 0.4)
        + (safe_divide(countif(is_cod), count(*)) * 0.3)
        + (safe_divide(countif(is_tier3_customer), count(*)) * 0.3),
        3
    )                                                               as cod_risk_score,

    -- state profile label
    case
        when safe_divide(countif(is_tier3_customer), count(*)) > 0.6
            then 'Tier-3 dominated'
        when safe_divide(countif(is_tier3_customer), count(*)) > 0.3
            then 'Mixed'
        else 'Tier-1/2 dominated'
    end                                                             as state_profile

from {{ ref('fct_orders') }}
group by state
order by total_gmv_inr desc