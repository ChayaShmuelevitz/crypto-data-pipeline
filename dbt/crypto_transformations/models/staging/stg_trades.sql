{{ config(
    materialized='view',
    database='AwsDataCatalog'
) }}

SELECT 
    symbol,
    price,
    quantity,
    CAST(from_iso8601_timestamp(timestamp) AS timestamp) as trade_timestamp,
    trade_time,
    price * quantity as trade_value
FROM {{ source('crypto', 'raw_trades') }}
WHERE price > 0
  AND quantity > 0
