-- mart_market_rates.sql
-- Daily FX rates with change, direction and fallback flag

{{ config(materialized='table', file_format='delta', schema='gold') }}

SELECT
    pair,
    rate_date,
    rate,
    ROUND(daily_change, 4)                  AS daily_change,
    direction,
    is_fallback
FROM {{ ref('stg_market_exchange_rates') }}