{{ config(materialized='table') }}

SELECT 
    symbol,
    CAST(DATE_TRUNC('hour', FROM_ISO8601_TIMESTAMP(timestamp)) AS VARCHAR) as hour,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(trade_value) as total_value,
    COUNT(*) as trade_count
FROM {{ ref('stg_trades') }}
GROUP BY symbol, DATE_TRUNC('hour', FROM_ISO8601_TIMESTAMP(timestamp))