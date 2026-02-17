{{ config(materialized='view') }}

SELECT 
    symbol,
    price,
    quantity,
    timestamp,
    trade_time,
    processing_time,
    CAST(price * quantity AS DOUBLE) as trade_value
FROM {{ source('crypto', 'raw_trades') }}