{{ config(
    materialized='table'
) }}

SELECT 
    symbol,
    date_trunc('hour', trade_timestamp) as hour,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(quantity) as total_quantity,
    SUM(trade_value) as total_value,
    COUNT(*) as trade_count
FROM {{ ref('stg_trades') }}
GROUP BY symbol, date_trunc('hour', trade_timestamp)
ORDER BY hour DESC, symbol
