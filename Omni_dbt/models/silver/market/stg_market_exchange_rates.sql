-- stg_market_exchange_rates.sql
-- Cleaned FX rates with daily change and direction

{{ config(materialized='view', schema='silver') }}

SELECT
    from_currency,
    to_currency,
    CONCAT(from_currency, '/', to_currency)     AS pair,
    rate,
    is_fallback,
    TO_DATE(last_refreshed, 'EEE, dd MMM yyyy HH:mm:ss Z')  AS rate_date,
    rate - LAG(rate) OVER (
        PARTITION BY from_currency, to_currency
        ORDER BY last_refreshed
    )                                           AS daily_change,
    CASE
        WHEN rate > LAG(rate) OVER (
            PARTITION BY from_currency, to_currency
            ORDER BY last_refreshed
        ) THEN 'up'
        WHEN rate < LAG(rate) OVER (
            PARTITION BY from_currency, to_currency
            ORDER BY last_refreshed
        ) THEN 'down'
        ELSE 'flat'
    END                                         AS direction,
    ingested_at
FROM {{ source('bronze', 'market_exchange_rates_raw') }}
WHERE rate > 0