-- Mart: User-level risk profile and behavior analysis
{{ config(
    materialized='table',
    indexes=[
      {'columns': ['user_id'], 'type': 'btree'},
      {'columns': ['risk_tier'], 'type': 'btree'}
    ]
) }}

WITH user_stats AS (
    SELECT
        user_id,
        COUNT(*) AS total_transactions,
        COUNT(CASE WHEN is_fraudulent THEN 1 END) AS fraud_count,
        
        SUM(amount) AS total_spent,
        AVG(amount) AS avg_transaction_amount,
        MAX(amount) AS max_transaction_amount,
        MIN(amount) AS min_transaction_amount,
        STDDEV(amount) AS stddev_amount,
        
        AVG(fraud_score) AS avg_fraud_score,
        MAX(fraud_score) AS max_fraud_score,
        
        COUNT(DISTINCT merchant_category) AS unique_categories,
        COUNT(DISTINCT country_code) AS unique_countries,
        COUNT(DISTINCT device_id) AS unique_devices,
        COUNT(DISTINCT ip_address) AS unique_ips,
        
        MIN(transaction_timestamp) AS first_transaction,
        MAX(transaction_timestamp) AS last_transaction,
        
        -- Behavioral flags
        COUNT(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1 END) AS late_night_txns,
        COUNT(CASE WHEN amount > 5000 THEN 1 END) AS high_value_txns,
        COUNT(CASE WHEN merchant_category IN ('wire_transfer', 'atm_withdrawal') THEN 1 END) AS risky_category_txns
        
    FROM {{ ref('stg_transactions') }}
    GROUP BY user_id
)

SELECT
    user_id,
    total_transactions,
    fraud_count,
    
    -- Fraud rate
    ROUND(
        CASE 
            WHEN total_transactions > 0 
            THEN (fraud_count::DECIMAL / total_transactions) * 100 
            ELSE 0 
        END, 
        2
    ) AS fraud_rate_pct,
    
    ROUND(total_spent, 2) AS total_spent,
    ROUND(avg_transaction_amount, 2) AS avg_transaction_amount,
    ROUND(max_transaction_amount, 2) AS max_transaction_amount,
    ROUND(min_transaction_amount, 2) AS min_transaction_amount,
    ROUND(COALESCE(stddev_amount, 0), 2) AS stddev_amount,
    
    ROUND(avg_fraud_score, 2) AS avg_fraud_score,
    max_fraud_score,
    
    unique_categories,
    unique_countries,
    unique_devices,
    unique_ips,
    
    first_transaction,
    last_transaction,
    
    -- Account age in days
    EXTRACT(DAY FROM (last_transaction - first_transaction)) AS account_age_days,
    
    -- Behavioral metrics
    late_night_txns,
    high_value_txns,
    risky_category_txns,
    
    -- Risk scoring
    CASE
        WHEN avg_fraud_score >= 70 OR fraud_count >= 5 THEN 'CRITICAL'
        WHEN avg_fraud_score >= 50 OR fraud_count >= 3 THEN 'HIGH'
        WHEN avg_fraud_score >= 30 OR fraud_count >= 1 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,
    
    -- Anomaly flags
    CASE WHEN unique_countries > 3 THEN TRUE ELSE FALSE END AS multi_country_flag,
    CASE WHEN unique_devices > 2 THEN TRUE ELSE FALSE END AS multi_device_flag,
    CASE WHEN late_night_txns > 5 THEN TRUE ELSE FALSE END AS late_night_flag,
    CASE WHEN stddev_amount > avg_transaction_amount * 2 THEN TRUE ELSE FALSE END AS amount_volatility_flag,
    
    CURRENT_TIMESTAMP AS profile_updated_at

FROM user_stats
ORDER BY avg_fraud_score DESC, total_transactions DESC