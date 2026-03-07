from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import logging

# Default arguments
default_args = {
    'owner': 'fraud_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'fraud_detection_pipeline',
    default_args=default_args,
    description='Hourly fraud detection analytics pipeline',
    schedule_interval='@hourly',  # Run every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['fraud', 'dbt', 'analytics'],
)

# Task 1: Check data freshness
def check_data_freshness(**context):
    """Verify new transactions exist in last hour"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    query = """
        SELECT COUNT(*) as recent_count
        FROM public.scored_transactions
        WHERE timestamp > NOW() - INTERVAL '1 hour';
    """
    
    result = hook.get_first(query)
    count = result[0] if result else 0
    
    logging.info(f"Found {count} transactions in last hour")
    
    if count == 0:
        raise ValueError("No fresh data! Check Kafka/Spark pipeline.")
    
    # Push count to XCom for downstream tasks
    context['ti'].xcom_push(key='transaction_count', value=count)
    return count

freshness_check = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

# Task 2: Run dbt models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt/fraud_detection && dbt run --profiles-dir .',
    dag=dag,
)

# Task 3: Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt/fraud_detection && dbt test --profiles-dir .',
    dag=dag,
)

# Task 4: Generate fraud report
def generate_fraud_report(**context):
    """Query marts and log fraud metrics"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get latest hourly summary
    summary_query = """
        SELECT 
            summary_hour,
            total_transactions,
            fraud_transactions,
            fraud_rate_pct,
            high_risk_count,
            avg_fraud_score
        FROM public_marts.fraud_summary
        ORDER BY summary_hour DESC
        LIMIT 1;
    """
    
    summary = hook.get_first(summary_query)
    
    if summary:
        logging.info("=" * 60)
        logging.info("HOURLY FRAUD REPORT")
        logging.info("=" * 60)
        logging.info(f"Hour: {summary[0]}")
        logging.info(f"Total Transactions: {summary[1]:,}")
        logging.info(f"Fraud Transactions: {summary[2]:,}")
        logging.info(f"Fraud Rate: {summary[3]:.2f}%")
        logging.info(f"High Risk Count: {summary[4]:,}")
        logging.info(f"Avg Fraud Score: {summary[5]:.2f}")
        logging.info("=" * 60)
        
        # Alert if fraud rate exceeds threshold
        if summary[3] > 10.0:  # 10% threshold
            logging.warning(f"⚠️ HIGH FRAUD ALERT: {summary[3]:.2f}% fraud rate!")
    
    # Get top 5 riskiest accounts
    risk_query = """
        SELECT user_id, risk_tier, fraud_rate_pct, avg_fraud_score
        FROM public_marts.account_risk_profile
        WHERE risk_tier IN ('CRITICAL', 'HIGH')
        ORDER BY avg_fraud_score DESC
        LIMIT 5;
    """
    
    risky_accounts = hook.get_records(risk_query)
    
    if risky_accounts:
        logging.info("\nTOP 5 RISKY ACCOUNTS:")
        for acc in risky_accounts:
            logging.info(f"  {acc[0]} | {acc[1]} | Fraud Rate: {acc[2]:.1f}% | Score: {acc[3]:.2f}")
    
    return summary

report_task = PythonOperator(
    task_id='generate_fraud_report',
    python_callable=generate_fraud_report,
    dag=dag,
)

# Task 5: Cleanup old data (optional)
cleanup_old_data = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_default',
    sql="""
        DELETE FROM public.scored_transactions
        WHERE timestamp < NOW() - INTERVAL '7 days';
    """,
    dag=dag,
)

# Define task dependencies
freshness_check >> dbt_run >> dbt_test >> report_task >> cleanup_old_data