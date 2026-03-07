-- Mart: Hourly fraud summary with key metrics
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['summary_hour'], 'type': 'btree'}
    ]
) }}

WITH hourly_stats AS (
    SELECT
        transaction_hour AS summary_hour,
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN is_fraudulent THEN 1 END) AS fraud_transactions,
        COUNT(CASE WHEN risk_level = 'HIGH' THEN 1 END) AS high_risk_count,
        COUNT(CASE WHEN risk_level = 'MEDIUM' THEN 1 END) AS medium_risk_count,
        COUNT(CASE WHEN risk_level = 'LOW' THEN 1 END) AS low_risk_count,
        
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraudulent THEN amount ELSE 0 END) AS fraud_amount,
        
        AVG(amount) AS avg_transaction_amount,
        AVG(fraud_score) AS avg_fraud_score,
        MAX(fraud_score) AS max_fraud_score,
        
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT merchant) AS unique_merchants,
        COUNT(DISTINCT country_code) AS unique_countries
        
    FROM {{ ref('stg_transactions') }}
    GROUP BY transaction_hour
)

SELECT
    summary_hour,
    total_transactions,
    fraud_transactions,
    high_risk_count,
    medium_risk_count,
    low_risk_count,
    
    -- Fraud rate calculation
    ROUND(
        CASE 
            WHEN total_transactions > 0 
            THEN (fraud_transactions::DECIMAL / total_transactions) * 100 
            ELSE 0 
        END, 
        2
    ) AS fraud_rate_pct,
    
    -- High risk rate
    ROUND(
        CASE 
            WHEN total_transactions > 0 
            THEN (high_risk_count::DECIMAL / total_transactions) * 100 
            ELSE 0 
        END, 
        2
    ) AS high_risk_rate_pct,
    
    total_amount,
    fraud_amount,
    
    -- Average fraud loss per transaction
    ROUND(
        CASE 
            WHEN fraud_transactions > 0 
            THEN fraud_amount / fraud_transactions 
            ELSE 0 
        END, 
        2
    ) AS avg_fraud_loss,
    
    ROUND(avg_transaction_amount, 2) AS avg_transaction_amount,
    ROUND(avg_fraud_score, 2) AS avg_fraud_score,
    max_fraud_score,
    
    unique_users,
    unique_merchants,
    unique_countries,
    
    CURRENT_TIMESTAMP AS created_at

FROM hourly_stats
ORDER BY summary_hour DESC