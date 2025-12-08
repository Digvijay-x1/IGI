import os
import psycopg2
import numpy as np
from collections import defaultdict

# Try to import rocksdb, fallback to mock if failed (e.g. build issues)
try:
    import rocksdb
    ROCKSDB_AVAILABLE = True
except ImportError:
    ROCKSDB_AVAILABLE = False
    print("WARNING: python-rocksdb not available. Using Mock Index.")

class Ranker:
    def __init__(self):
        # 1. Connect to Postgres (Metadata)
        try:
            self.db_conn = psycopg2.connect(
                host=os.environ.get("DB_HOST", "postgres_service"),
                database=os.environ.get("DB_NAME", "search_engine"),
                user=os.environ.get("DB_USER", "admin"),
                password=os.environ.get("DB_PASS", "password123")
            )
            print("Connected to Postgres")
        except Exception as e:
            print(f"Failed to connect to Postgres: {e}")
            self.db_conn = None
        
        # 2. Open RocksDB (Inverted Index) - Read Only
        rocksdb_path = os.environ.get("ROCKSDB_PATH", "/shared_data/search_index.db")
        self.index_db = None
        
        if ROCKSDB_AVAILABLE:
            try:
                opts = rocksdb.Options()
                # We only need read access
                self.index_db = rocksdb.DB(rocksdb_path, opts, read_only=True)
                print(f"Opened RocksDB at {rocksdb_path}")
            except Exception as e:
                print(f"Failed to open RocksDB: {e}")
        
        # Mock Index for fallback
        self.mock_index = {
            "computer": "1,2",
            "cats": "3,4"
        }
        
        # 3. Load Global Stats (avgdl)
        self.avgdl = self._calculate_avgdl()
        print(f"Ranker initialized. AvgDL: {self.avgdl}")

    def _calculate_avgdl(self):
        if not self.db_conn:
            return 100.0 # Default if DB not connected
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("SELECT AVG(doc_length) FROM documents")
                avg = cur.fetchone()[0]
                return float(avg) if avg else 0.0
        except Exception as e:
            print(f"Error calculating avgdl: {e}")
            return 100.0

    def search(self, query, k=10):
        """
        Performs BM25 search for the given query.
        Returns top k results: [{'url': ..., 'title': ..., 'score': ...}]
        """
        tokens = query.lower().split() # Simple tokenization
        if not tokens:
            return []

        # BM25 Constants
        k1 = 1.5
        b = 0.75
        
        # Accumulate scores: doc_id -> score
        scores = defaultdict(float)
        
        for token in tokens:
            # A. Get Posting List from RocksDB or Mock
            postings_str = None
            
            if self.index_db:
                try:
                    val = self.index_db.get(token.encode('utf-8'))
                    if val:
                        postings_str = val.decode('utf-8')
                except Exception as e:
                    print(f"Error fetching token {token}: {e}")
            elif not ROCKSDB_AVAILABLE:
                # Fallback to mock
                postings_str = self.mock_index.get(token)

            if not postings_str:
                continue
                
            # Format: "doc_id1,doc_id2,..." (Simplified for now, ideally should have TF)
            # For this phase, we assume TF=1 for all occurrences in the simplified index
            if isinstance(postings_str, bytes):
                postings_str = postings_str.decode('utf-8')
            
            doc_ids = [int(d) for d in postings_str.split(',')]
            
            # Calculate IDF
            # IDF(q_i) = log( (N - n(q_i) + 0.5) / (n(q_i) + 0.5) + 1 )
            # For simplicity in this phase, we'll use a basic IDF or just count
            # We need N (total docs)
            N = 1000 # Placeholder or fetch from DB
            n_qi = len(doc_ids)
            idf = np.log((N - n_qi + 0.5) / (n_qi + 0.5) + 1)
            
            for doc_id in doc_ids:
                # In a real implementation, we'd fetch doc_length and TF from the index/DB
                # Here we do a simplified calculation
                tf = 1 # Simplified
                doc_len = 100 # Simplified placeholder
                
                # BM25 Score for this term
                numerator = idf * tf * (k1 + 1)
                denominator = tf + k1 * (1 - b + b * (doc_len / self.avgdl))
                scores[doc_id] += numerator / denominator

        # Sort by score
        sorted_docs = sorted(scores.items(), key=lambda item: item[1], reverse=True)[:k]
        
        # Fetch Metadata for top results
        results = []
        if self.db_conn:
            try:
                with self.db_conn.cursor() as cur:
                    for doc_id, score in sorted_docs:
                        cur.execute("SELECT url FROM documents WHERE id = %s", (doc_id,))
                        row = cur.fetchone()
                        if row:
                            results.append({
                                "id": doc_id,
                                "url": row[0],
                                "score": score,
                                "title": row[0] # Use URL as title for now
                            })
            except Exception as e:
                print(f"Error fetching metadata: {e}")
        else:
            # Fallback if DB is down
            for doc_id, score in sorted_docs:
                results.append({
                    "id": doc_id,
                    "url": f"http://mock-url.com/{doc_id}",
                    "score": score,
                    "title": f"Mock Document {doc_id}"
                })
                    
        return results
