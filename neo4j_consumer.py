"""
Neo4j Graph Consumer - Fraud Ring Detection
Reads high-risk transactions from Kafka and builds a graph database
"""

import json
import time
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "txn_raw"
KAFKA_GROUP_ID = "neo4j-consumer-group"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# ============================================
# NEO4J DRIVER
# ============================================
class Neo4jGraphBuilder:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print(f"✓ Connected to Neo4j at {uri}")
        
    def close(self):
        self.driver.close()
        
    def create_constraints(self):
        """Create uniqueness constraints for better performance"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.user_id IS UNIQUE",
                "CREATE CONSTRAINT merchant_name IF NOT EXISTS FOR (m:Merchant) REQUIRE m.name IS UNIQUE",
                "CREATE CONSTRAINT device_id IF NOT EXISTS FOR (d:Device) REQUIRE d.device_id IS UNIQUE",
                "CREATE CONSTRAINT ip_address IF NOT EXISTS FOR (i:IPAddress) REQUIRE i.ip IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    pass  # Constraint might already exist
                    
        print("✓ Neo4j constraints created")
    
    def process_transaction(self, txn_data):
        """
        Create graph nodes and relationships for a transaction
        
        Graph Schema:
        - (Account)-[:TRANSACTED_WITH]->(Merchant)
        - (Account)-[:USED_DEVICE]->(Device)
        - (Account)-[:FROM_IP]->(IPAddress)
        - (Device)-[:SHARED_BY]->(Account)
        """
        
        with self.driver.session() as session:
            session.execute_write(self._create_transaction_graph, txn_data)
    
    @staticmethod
    def _create_transaction_graph(tx, data):
        """Cypher query to create nodes and relationships"""
        
        query = """
        // Create or merge Account node
        MERGE (account:Account {user_id: $user_id})
        ON CREATE SET 
            account.created_at = datetime($timestamp),
            account.country = $country,
            account.total_transactions = 1,
            account.total_amount = $amount,
            account.is_fraudster = $is_fraudulent
        ON MATCH SET
            account.total_transactions = account.total_transactions + 1,
            account.total_amount = account.total_amount + $amount,
            account.last_transaction = datetime($timestamp)
        
        // Create or merge Merchant node
        MERGE (merchant:Merchant {name: $merchant})
        ON CREATE SET 
            merchant.category = $merchant_category,
            merchant.first_seen = datetime($timestamp),
            merchant.transaction_count = 1
        ON MATCH SET
            merchant.transaction_count = merchant.transaction_count + 1
        
        // Create or merge Device node
        MERGE (device:Device {device_id: $device_id})
        ON CREATE SET 
            device.first_seen = datetime($timestamp),
            device.user_count = 1
        
        // Create or merge IP Address node
        MERGE (ip:IPAddress {ip: $ip_address})
        ON CREATE SET 
            ip.first_seen = datetime($timestamp),
            ip.country = $country,
            ip.user_count = 1
        
        // Create relationships
        MERGE (account)-[t:TRANSACTED_WITH]->(merchant)
        ON CREATE SET 
            t.first_transaction = datetime($timestamp),
            t.transaction_count = 1,
            t.total_amount = $amount
        ON MATCH SET
            t.transaction_count = t.transaction_count + 1,
            t.total_amount = t.total_amount + $amount,
            t.last_transaction = datetime($timestamp)
        
        MERGE (account)-[ud:USED_DEVICE]->(device)
        ON CREATE SET 
            ud.first_used = datetime($timestamp),
            ud.usage_count = 1
        ON MATCH SET
            ud.usage_count = ud.usage_count + 1,
            ud.last_used = datetime($timestamp)
        
        MERGE (account)-[fi:FROM_IP]->(ip)
        ON CREATE SET 
            fi.first_used = datetime($timestamp),
            fi.usage_count = 1
        ON MATCH SET
            fi.usage_count = fi.usage_count + 1,
            fi.last_used = datetime($timestamp)
        
        // Create Device-Account sharing relationship
        MERGE (device)-[sb:SHARED_BY]->(account)
        ON CREATE SET sb.since = datetime($timestamp)
        
        // Create IP-Account sharing relationship
        MERGE (ip)-[ib:USED_BY]->(account)
        ON CREATE SET ib.since = datetime($timestamp)
        
        // Store transaction details
        CREATE (txn:Transaction {
            txn_id: $txn_id,
            amount: $amount,
            currency: $currency,
            timestamp: datetime($timestamp),
            is_fraudulent: $is_fraudulent,
            fraud_label: $fraud_label
        })
        
        CREATE (account)-[:MADE_TRANSACTION]->(txn)
        CREATE (txn)-[:AT_MERCHANT]->(merchant)
        """
        
        tx.run(query, 
               user_id=data['user_id'],
               merchant=data['merchant'],
               merchant_category=data['merchant_category'],
               device_id=data['device_id'],
               ip_address=data['ip_address'],
               country=data['country'],
               amount=data['amount'],
               currency=data['currency'],
               timestamp=data['timestamp'],
               txn_id=data['txn_id'],
               is_fraudulent=data['is_fraudulent'],
               fraud_label=data.get('fraud_label', 'legitimate'))

# ============================================
# KAFKA CONSUMER
# ============================================
def create_kafka_consumer():
    """Initialize Kafka consumer"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='latest',  # Start from latest messages
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"✓ Connected to Kafka topic: {KAFKA_TOPIC}")
    return consumer

# ============================================
# FRAUD SCORING (Simple Rule-Based)
# ============================================
def calculate_fraud_score(txn):
    """Calculate fraud score to filter high-risk transactions"""
    score = 0
    
    # Amount-based scoring
    if txn['amount'] > 5000:
        score += 40
    elif txn['amount'] > 2000:
        score += 20
    elif txn['amount'] > 1000:
        score += 10
    
    # Category-based scoring
    suspicious_categories = ["wire_transfer", "atm_withdrawal", "jewelry", "luxury", "electronics"]
    if txn['merchant_category'] in suspicious_categories:
        score += 30
    
    # Country-based scoring
    high_risk_countries = ["RU", "CN", "NG", "BR"]
    if txn['country'] in high_risk_countries:
        score += 20
    
    # Fraud label (if present)
    if txn.get('is_fraudulent', False):
        score += 50
    
    return min(score, 100)

# ============================================
# MAIN CONSUMER LOOP
# ============================================
def main():
    print("=" * 60)
    print("NEO4J GRAPH CONSUMER - FRAUD RING DETECTION")
    print("=" * 60)
    
    # Initialize Neo4j
    print("\n[1/3] Connecting to Neo4j...")
    graph_builder = Neo4jGraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    graph_builder.create_constraints()
    
    # Initialize Kafka consumer
    print("\n[2/3] Connecting to Kafka...")
    consumer = create_kafka_consumer()
    
    # Start consuming
    print("\n[3/3] Starting graph population...")
    print("=" * 60)
    print("CONSUMING HIGH-RISK TRANSACTIONS")
    print("Filtering: fraud_score >= 40 (MEDIUM/HIGH risk)")
    print("Press Ctrl+C to stop")
    print("=" * 60 + "\n")
    
    txn_count = 0
    high_risk_count = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            txn = message.value
            txn_count += 1
            
            # Calculate fraud score
            fraud_score = calculate_fraud_score(txn)
            
            # Only process HIGH and MEDIUM risk transactions (score >= 40)
            if fraud_score >= 40:
                high_risk_count += 1
                
                # Add to Neo4j graph
                graph_builder.process_transaction(txn)
                
                # Progress update every 10 high-risk transactions
                if high_risk_count % 10 == 0:
                    elapsed = time.time() - start_time
                    print(f"[{high_risk_count} high-risk] "
                          f"Total processed: {txn_count} | "
                          f"Filter rate: {(high_risk_count/txn_count)*100:.1f}% | "
                          f"Runtime: {elapsed:.0f}s")
    
    except KeyboardInterrupt:
        print("\n\n⏹ Stopping Neo4j consumer...")
        
        # Final stats
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("FINAL STATISTICS")
        print("=" * 60)
        print(f"Total Transactions Processed: {txn_count:,}")
        print(f"High-Risk Added to Graph: {high_risk_count:,}")
        print(f"Filter Rate: {(high_risk_count/txn_count)*100:.2f}%")
        print(f"Runtime: {elapsed:.1f} seconds")
        print("=" * 60)
    
    finally:
        consumer.close()
        graph_builder.close()
        print("✓ Connections closed")

if __name__ == "__main__":
    main()