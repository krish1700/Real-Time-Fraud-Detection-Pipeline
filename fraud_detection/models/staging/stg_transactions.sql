-- Staging: Clean and standardize raw transaction data
{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        txn_id,
        user_id,
        amount,
        currency,
        merchant,
        merchant_category,
        timestamp,
        country,
        device_id,
        ip_address,
        fraud_score,
        risk_level,
        is_fraudulent,
        fraud_label
    FROM {{ source('public', 'scored_transactions') }}
)

SELECT
    txn_id,
    user_id,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    UPPER(currency) AS currency,
    merchant,
    LOWER(merchant_category) AS merchant_category,
    timestamp AS transaction_timestamp,
    UPPER(country) AS country_code,
    device_id,
    ip_address,
    fraud_score,
    UPPER(risk_level) AS risk_level,
    is_fraudulent,
    COALESCE(fraud_label, 'legitimate') AS fraud_label,
    -- Derived fields
    DATE_TRUNC('hour', timestamp) AS transaction_hour,
    DATE_TRUNC('day', timestamp) AS transaction_date,
    EXTRACT(HOUR FROM timestamp) AS hour_of_day,
    EXTRACT(DOW FROM timestamp) AS day_of_week,
    
    -- Risk categorization
    CASE 
        WHEN fraud_score >= 70 THEN 'HIGH'
        WHEN fraud_score >= 40 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS calculated_risk_level

FROM source_data
WHERE txn_id IS NOT NULL