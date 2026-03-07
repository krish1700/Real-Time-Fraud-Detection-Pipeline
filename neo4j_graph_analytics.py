from neo4j import GraphDatabase
import os

class Neo4jGraphAnalytics:
    def __init__(self):
        uri = "bolt://localhost:7687"
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def create_graph_projection(self):
        """Create in-memory graph projection for GDS algorithms"""
        with self.driver.session() as session:
            # Drop existing projection if exists
            try:
                session.run("CALL gds.graph.drop('fraud-network', false)")
                print("✓ Dropped existing graph projection")
            except:
                pass
            
            # Create new projection
            result = session.run("""
                CALL gds.graph.project(
                    'fraud-network',
                    'Account',
                    {
                        TRANSACTED_WITH: {
                            orientation: 'UNDIRECTED',
                            properties: ['amount', 'fraud_score']
                        }
                    }
                )
                YIELD graphName, nodeCount, relationshipCount
                RETURN graphName, nodeCount, relationshipCount
            """)
            
            record = result.single()
            print(f"✓ Graph projection created: {record['nodeCount']} nodes, {record['relationshipCount']} relationships")
    
    def run_pagerank(self, top_n=20):
        """
        Run PageRank to find central accounts in the fraud network
        High PageRank = many connections or connections to important nodes
        """
        with self.driver.session() as session:
            result = session.run("""
                CALL gds.pageRank.stream('fraud-network')
                YIELD nodeId, score
                WITH gds.util.asNode(nodeId) AS account, score
                WHERE account.is_fraudster = true
                RETURN account.user_id AS user_id, 
                       score AS pagerank_score,
                       account.total_transactions AS total_txns
                ORDER BY score DESC
                LIMIT $top_n
            """, top_n=top_n)
            
            central_accounts = []
            print(f"\n{'='*60}")
            print("TOP CENTRAL FRAUD ACCOUNTS (PageRank)")
            print(f"{'='*60}")
            
            for i, record in enumerate(result, 1):
                account = {
                    'user_id': record['user_id'],
                    'pagerank_score': record['pagerank_score'],
                    'total_txns': record['total_txns']
                }
                central_accounts.append(account)
                print(f"{i}. {account['user_id']} | "
                      f"PageRank: {account['pagerank_score']:.4f} | "
                      f"Txns: {account['total_txns']}")
            
            return central_accounts
    
    def run_louvain_community_detection(self):
        """
        Run Louvain algorithm to detect fraud rings (communities)
        Accounts in same community likely collaborating
        """
        with self.driver.session() as session:
            # Run Louvain and write community IDs
            session.run("""
                CALL gds.louvain.write('fraud-network', {
                    writeProperty: 'community_id'
                })
            """)
            print("✓ Louvain community detection complete")
            
            # Find fraud communities
            result = session.run("""
                MATCH (a:Account)
                WHERE a.is_fraudster = true AND a.community_id IS NOT NULL
                WITH a.community_id AS community, 
                     collect(a.user_id) AS members,
                     count(a) AS size,
                     sum(a.total_transactions) AS total_txns,
                     avg(a.avg_fraud_score) AS avg_score
                WHERE size >= 3
                RETURN community, members, size, total_txns, avg_score
                ORDER BY size DESC
                LIMIT 10
            """)
            
            fraud_rings = []
            print(f"\n{'='*60}")
            print("DETECTED FRAUD RINGS (Louvain Communities)")
            print(f"{'='*60}")
            
            for record in result:
                ring = {
                    'community_id': record['community'],
                    'members': record['members'],
                    'size': record['size'],
                    'total_txns': record['total_txns'],
                    'avg_fraud_score': record['avg_score']
                }
                fraud_rings.append(ring)
                
                print(f"\nCommunity {ring['community_id']}:")
                print(f"  Members: {', '.join(ring['members'][:5])}" + 
                      (f" + {ring['size']-5} more" if ring['size'] > 5 else ""))
                print(f"  Size: {ring['size']} accounts")
                print(f"  Total Transactions: {ring['total_txns']}")
                print(f"  Avg Fraud Score: {ring['avg_fraud_score']:.2f}")
            
            return fraud_rings
    
    def find_fraud_ring_connections(self, community_id):
        """Find all connections within a fraud ring"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a1:Account)-[r:TRANSACTED_WITH]-(a2:Account)
                WHERE a1.community_id = $community_id 
                  AND a2.community_id = $community_id
                  AND a1.is_fraudster = true
                  AND a2.is_fraudster = true
                RETURN a1.user_id AS from_user,
                       a2.user_id AS to_user,
                       r.amount AS amount,
                       r.fraud_score AS fraud_score,
                       r.timestamp AS timestamp
                ORDER BY r.timestamp DESC
                LIMIT 50
            """, community_id=community_id)
            
            connections = [dict(record) for record in result]
            return connections
    
    def close(self):
        self.driver.close()

# ============================================
# MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    print("="*60)
    print("NEO4J GRAPH ANALYTICS - FRAUD RING DETECTION")
    print("="*60)
    
    analytics = Neo4jGraphAnalytics()
    
    try:
        # Step 1: Create graph projection
        print("\n[1/3] Creating graph projection...")
        analytics.create_graph_projection()
        
        # Step 2: Run PageRank
        print("\n[2/3] Running PageRank algorithm...")
        central_accounts = analytics.run_pagerank(top_n=20)
        
        # Step 3: Run Louvain community detection
        print("\n[3/3] Running Louvain community detection...")
        fraud_rings = analytics.run_louvain_community_detection()
        
        # Investigate top fraud ring
        if fraud_rings:
            top_ring = fraud_rings[0]
            print(f"\n{'='*60}")
            print(f"INVESTIGATING TOP FRAUD RING (Community {top_ring['community_id']})")
            print(f"{'='*60}")
            connections = analytics.find_fraud_ring_connections(top_ring['community_id'])
            print(f"Found {len(connections)} internal connections")
            
            if connections:
                print("\nSample connections:")
                for i, conn in enumerate(connections[:5], 1):
                    print(f"  {i}. {conn['from_user']} → {conn['to_user']} | "
                          f"${conn['amount']:.2f} | Score: {conn['fraud_score']}")
        
        print(f"\n{'='*60}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*60}")
        
    finally:
        analytics.close()