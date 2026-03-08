import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from faker import Faker
import uuid
import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server

fake = Faker()

# ============================================
# CONFIGURATION
# ============================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "txn_raw"
TOTAL_ACCOUNTS = 1000
FRAUDSTER_ACCOUNTS = 50
TARGET_TPS = 100  # Transactions per second

# ============================================
# PROMETHEUS METRICS
# ============================================
# Counter: Monotonically increasing values
transactions_total = Counter(
    'fraud_transactions_total', 
    'Total number of transactions generated',
    ['transaction_type']  # Labels: legitimate, fraudulent
)

# Histogram: Distribution of transaction amounts
transaction_amount_histogram = Histogram(
    'fraud_transaction_amount_dollars',
    'Distribution of transaction amounts',
    buckets=[10, 50, 100, 500, 1000, 5000, 10000]
)

# Histogram: Processing time per batch
batch_processing_time = Histogram(
    'fraud_batch_processing_seconds',
    'Time taken to process each batch of transactions'
)

# Gauge: Current transactions per second
current_tps = Gauge(
    'fraud_current_tps',
    'Current transactions per second'
)

# Gauge: Total fraud rate percentage
fraud_rate_gauge = Gauge(
    'fraud_rate_percentage',
    'Current fraud rate as percentage'
)

# Counter: Transactions by merchant category
transactions_by_category = Counter(
    'fraud_transactions_by_category_total',
    'Total transactions by merchant category',
    ['category']
)

# ============================================
# ACCOUNT GENERATION
# ============================================
class AccountGenerator:
    def __init__(self):
        self.legitimate_accounts = []
        self.fraudster_accounts = []
        self.account_metadata = {}
        
    def generate_accounts(self):
        """Generate 1000 legitimate + 50 fraudster accounts"""
        print(f"Generating {TOTAL_ACCOUNTS} legitimate accounts...")
        
        # Generate legitimate accounts
        for i in range(TOTAL_ACCOUNTS):
            user_id = f"U{i+1:04d}"
            account = {
                "user_id": user_id,
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "country": fake.country_code(),
                "account_age_days": random.randint(30, 3650),  # 1 month to 10 years
                "avg_transaction_amount": random.uniform(50, 500),
                "is_fraudster": False
            }
            self.legitimate_accounts.append(account)
            self.account_metadata[user_id] = account
        
        print(f"Generating {FRAUDSTER_ACCOUNTS} fraudster accounts...")
        
        # Generate fraudster accounts
        for i in range(FRAUDSTER_ACCOUNTS):
            user_id = f"F{i+1:04d}"
            account = {
                "user_id": user_id,
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "country": fake.country_code(),
                "account_age_days": random.randint(1, 90),  # New accounts (red flag)
                "avg_transaction_amount": random.uniform(500, 5000),  # Higher amounts
                "is_fraudster": True,
                "fraud_type": random.choice([
                    "velocity_attack",      # Many txns in short time
                    "amount_anomaly",       # Unusually large amounts
                    "geographic_impossible", # Multiple countries quickly
                    "round_amount",         # Suspicious round numbers
                    "late_night"            # Transactions at odd hours
                ])
            }
            self.fraudster_accounts.append(account)
            self.account_metadata[user_id] = account
        
        print(f"✓ Total accounts: {len(self.legitimate_accounts) + len(self.fraudster_accounts)}")
        return self.legitimate_accounts, self.fraudster_accounts

# ============================================
# TRANSACTION GENERATION
# ============================================
class TransactionGenerator:
    def __init__(self, accounts_metadata):
        self.accounts_metadata = accounts_metadata
        self.transaction_history = {}  # Track recent txns per user
        
    def generate_legitimate_transaction(self, account):
        """Generate normal transaction"""
        user_id = account["user_id"]
        
        # Normal transaction patterns
        amount = round(random.gauss(account["avg_transaction_amount"], 100), 2)
        amount = max(5.0, min(amount, 2000.0))  # Clamp between $5-$2000
        
        transaction = {
            "txn_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": amount,
            "currency": "USD",
            "merchant": fake.company(),
            "merchant_category": random.choice([
                "grocery", "restaurant", "gas_station", "retail", 
                "online_shopping", "entertainment", "utilities"
            ]),
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "country": account["country"],
            "device_id": f"device_{user_id}",
            "ip_address": fake.ipv4(),
            "is_fraudulent": False,
            "fraud_label": "legitimate"
        }
        
        return transaction
    
    def generate_fraudulent_transaction(self, account):
        """Generate fraudulent transaction based on fraud type"""
        user_id = account["user_id"]
        fraud_type = account["fraud_type"]
        
        transaction = {
            "txn_id": str(uuid.uuid4()),
            "user_id": user_id,
            "currency": "USD",
            "merchant": fake.company(),
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "device_id": f"device_{user_id}",
            "ip_address": fake.ipv4(),
            "is_fraudulent": True,
            "fraud_label": fraud_type
        }
        
        # Apply fraud-specific patterns
        if fraud_type == "velocity_attack":
            # Many transactions in short time
            transaction["amount"] = round(random.uniform(100, 500), 2)
            transaction["merchant_category"] = "online_shopping"
            transaction["country"] = account["country"]
            
        elif fraud_type == "amount_anomaly":
            # Unusually large amount
            transaction["amount"] = round(random.uniform(5000, 15000), 2)
            transaction["merchant_category"] = random.choice(["electronics", "jewelry", "luxury"])
            transaction["country"] = account["country"]
            
        elif fraud_type == "geographic_impossible":
            # Transaction from different country than usual
            transaction["amount"] = round(random.uniform(200, 1000), 2)
            transaction["country"] = random.choice(["RU", "CN", "NG", "BR"])  # High-risk countries
            transaction["merchant_category"] = "online_shopping"
            
        elif fraud_type == "round_amount":
            # Suspicious round numbers
            transaction["amount"] = float(random.choice([1000, 2000, 5000, 10000]))
            transaction["merchant_category"] = "wire_transfer"
            transaction["country"] = account["country"]
            
        elif fraud_type == "late_night":
            # Transactions at odd hours (2-5 AM)
            hour = random.randint(2, 5)
            transaction["timestamp"] = datetime.now(timezone.utc).replace(hour=hour).isoformat().replace('+00:00', 'Z')
            transaction["amount"] = round(random.uniform(500, 2000), 2)
            transaction["merchant_category"] = "atm_withdrawal"
            transaction["country"] = account["country"]
        
        return transaction
        
    def generate_transaction(self):
        """Generate a random transaction (legitimate or fraudulent)"""
        # Randomly select from legitimate or fraudulent accounts
        if random.random() < 0.95:  # 95% legitimate, 5% fraudulent
            account = random.choice(self.accounts_metadata.legitimate_accounts)
            return self.generate_legitimate_transaction(account)
        else:
            account = random.choice(self.accounts_metadata.fraudster_accounts)
            return self.generate_fraudulent_transaction(account)


# ============================================
# KAFKA PRODUCER
# ============================================
def create_producer():
    """Initialize Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        compression_type='gzip',
        linger_ms=10,
        batch_size=16384,
        acks=1,  # Wait for leader acknowledgment only (faster)
        max_in_flight_requests_per_connection=5  # Allow 5 unacknowledged requests
    )
    return producer

# ============================================
# MAIN STREAMING LOOP
# ============================================
def stream_transactions():
    """Generate and stream transactions to Kafka with Prometheus metrics"""
    
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    print("📊 Prometheus metrics server started on port 8000")
    
    account_gen = AccountGenerator()
    account_gen.generate_accounts()
    generator = TransactionGenerator(account_gen)
    producer = create_producer()
    
    total_transactions = 0
    total_fraud = 0
    start_time = time.time()
    
    print(f"\n🚀 Starting transaction stream at {TARGET_TPS} TPS")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            batch_start = time.time()
            
            for _ in range(TARGET_TPS):
                # Generate transaction
                transaction = generator.generate_transaction()
                
                # Send to Kafka
                producer.send(KAFKA_TOPIC, transaction)
                
                # Update Prometheus metrics
                is_fraud = transaction['is_fraudulent']
                txn_type = 'fraudulent' if is_fraud else 'legitimate'
                
                # Increment counters
                transactions_total.labels(transaction_type=txn_type).inc()
                transactions_by_category.labels(
                    category=transaction['merchant_category']
                ).inc()
                
                # Record amount distribution
                transaction_amount_histogram.observe(transaction['amount'])
                
                # Update totals
                total_transactions += 1
                if is_fraud:
                    total_fraud += 1
            
            # Calculate metrics
            batch_duration = time.time() - batch_start
            batch_processing_time.observe(batch_duration)
            
            runtime = time.time() - start_time
            actual_tps = total_transactions / runtime if runtime > 0 else 0
            fraud_rate = (total_fraud / total_transactions * 100) if total_transactions > 0 else 0
            
            # Update gauges
            current_tps.set(actual_tps)
            fraud_rate_gauge.set(fraud_rate)
            
            # Print stats every 1000 transactions
            if total_transactions % 1000 == 0:
                print(f"[{total_transactions:,} txns] Fraud: {total_fraud:,} ({fraud_rate:.1f}%) | "
                      f"TPS: {actual_tps:.1f} | Runtime: {int(runtime)}s")
            
            # Sleep to maintain target TPS
            sleep_time = max(0, 1.0 - batch_duration)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n\n⏹ Stopping transaction generator...")
        producer.flush()
        producer.close()
        
        # Final statistics
        runtime = time.time() - start_time
        print("\n" + "="*60)
        print("FINAL STATISTICS")
        print("="*60)
        print(f"Total Transactions: {total_transactions:,}")
        print(f"Fraudulent: {total_fraud:,} ({fraud_rate:.2f}%)")
        print(f"Legitimate: {total_transactions - total_fraud:,}")
        print(f"Runtime: {runtime:.1f} seconds")
        print(f"Average TPS: {total_transactions/runtime:.1f}")
        print("="*60)

# ============================================
# RUN
# ============================================
if __name__ == "__main__":
    stream_transactions()