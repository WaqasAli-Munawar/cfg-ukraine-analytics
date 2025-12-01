"""
Embedding Service for CFG Ukraine RAG
Creates and manages vector embeddings in Qdrant
WITH DEDUPLICATION AND BATCH UPLOAD
"""
import uuid
import hashlib
from typing import List, Dict, Any, Optional, Set
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct,
    VectorParams,
    Distance,
    Filter,
    FieldCondition,
    MatchValue,
)
import pandas as pd

from src.services.onelake_data_service import OneLakeDataService
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class EmbeddingService:
    """
    Creates and manages vector embeddings for CFG Ukraine data.
    Uses OpenAI embeddings and Qdrant for vector storage.
    
    FEATURES:
    - Deduplication: Won't re-embed existing data
    - ETag tracking: Detects OneLake data changes
    - Batch upload: Handles large datasets
    """
    
    EMBEDDING_MODEL = "text-embedding-3-small"
    EMBEDDING_DIMENSION = 1536
    
    # Collection names
    ACCOUNTS_COLLECTION = "cfg_accounts"
    ENTITIES_COLLECTION = "cfg_entities"
    DEPARTMENTS_COLLECTION = "cfg_departments"
    METADATA_COLLECTION = "cfg_metadata"
    
    # Batch sizes
    EMBEDDING_BATCH_SIZE = 100
    UPLOAD_BATCH_SIZE = 50  # Smaller batches for Qdrant upload
    
    def __init__(self):
        self.settings = get_settings()
        self.openai_client = OpenAI(api_key=self.settings.openai_api_key)
        self.qdrant_client = QdrantClient(
            host=self.settings.qdrant_host,
            port=self.settings.qdrant_port,
        )
        self.data_service = OneLakeDataService()
        logger.info("Embedding service initialized with deduplication and batch upload")
    
    def _generate_doc_id(self, collection: str, identifier: str) -> str:
        """Generate a deterministic document ID."""
        content = f"{collection}:{identifier}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_existing_ids(self, collection_name: str) -> Set[str]:
        """Get all existing document IDs from a collection."""
        existing_ids = set()
        
        try:
            collections = self.qdrant_client.get_collections().collections
            if collection_name not in [c.name for c in collections]:
                return existing_ids
            
            offset = None
            while True:
                result = self.qdrant_client.scroll(
                    collection_name=collection_name,
                    limit=100,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )
                
                points, offset = result
                
                for point in points:
                    if 'doc_id' in point.payload:
                        existing_ids.add(point.payload['doc_id'])
                
                if offset is None:
                    break
            
            logger.info(f"Found {len(existing_ids)} existing documents in {collection_name}")
            
        except Exception as e:
            logger.warning(f"Could not get existing IDs from {collection_name}: {e}")
        
        return existing_ids
    
    def _get_stored_etag(self, collection_name: str) -> Optional[str]:
        """Get the stored ETag for a collection from metadata."""
        try:
            self.ensure_collection(self.METADATA_COLLECTION)
            
            results = self.qdrant_client.scroll(
                collection_name=self.METADATA_COLLECTION,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="collection",
                            match=MatchValue(value=collection_name),
                        )
                    ]
                ),
                limit=1,
                with_payload=True,
                with_vectors=False,
            )
            
            points, _ = results
            if points:
                return points[0].payload.get('etag')
            
        except Exception as e:
            logger.warning(f"Could not get stored ETag for {collection_name}: {e}")
        
        return None
    
    def _store_etag(self, collection_name: str, etag: str):
        """Store the ETag for a collection in metadata."""
        try:
            self.ensure_collection(self.METADATA_COLLECTION)
            
            meta_id = self._generate_doc_id("metadata", collection_name)
            dummy_vector = [0.0] * self.EMBEDDING_DIMENSION
            
            point = PointStruct(
                id=meta_id,
                vector=dummy_vector,
                payload={
                    'collection': collection_name,
                    'etag': etag,
                    'doc_id': meta_id,
                },
            )
            
            self.qdrant_client.upsert(
                collection_name=self.METADATA_COLLECTION,
                points=[point],
            )
            
            logger.info(f"Stored ETag for {collection_name}: {etag[:20]}...")
            
        except Exception as e:
            logger.warning(f"Could not store ETag for {collection_name}: {e}")
    
    def _upload_points_batched(self, collection_name: str, points: List[PointStruct]):
        """Upload points to Qdrant in batches to avoid payload size limit."""
        total_uploaded = 0
        
        for i in range(0, len(points), self.UPLOAD_BATCH_SIZE):
            batch = points[i:i + self.UPLOAD_BATCH_SIZE]
            self.qdrant_client.upsert(
                collection_name=collection_name,
                points=batch,
            )
            total_uploaded += len(batch)
            logger.info(f"Uploaded batch {i//self.UPLOAD_BATCH_SIZE + 1}: {len(batch)} points (total: {total_uploaded}/{len(points)})")
        
        return total_uploaded
    
    def create_embedding(self, text: str) -> List[float]:
        """Create embedding for a single text."""
        response = self.openai_client.embeddings.create(
            model=self.EMBEDDING_MODEL,
            input=text,
        )
        return response.data[0].embedding
    
    def create_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Create embeddings for multiple texts in batch."""
        if not texts:
            return []
        
        all_embeddings = []
        
        for i in range(0, len(texts), self.EMBEDDING_BATCH_SIZE):
            batch = texts[i:i + self.EMBEDDING_BATCH_SIZE]
            response = self.openai_client.embeddings.create(
                model=self.EMBEDDING_MODEL,
                input=batch,
            )
            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)
            logger.info(f"Created embeddings for batch {i//self.EMBEDDING_BATCH_SIZE + 1} ({len(batch)} texts)")
        
        return all_embeddings
    
    def ensure_collection(self, collection_name: str):
        """Ensure a collection exists in Qdrant."""
        collections = self.qdrant_client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if collection_name not in collection_names:
            self.qdrant_client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=self.EMBEDDING_DIMENSION,
                    distance=Distance.COSINE,
                ),
            )
            logger.info(f"Created collection: {collection_name}")
    
    def embed_accounts(self, force_refresh: bool = False) -> Dict[str, int]:
        """Embed chart of accounts into Qdrant with deduplication and batching."""
        self.ensure_collection(self.ACCOUNTS_COLLECTION)
        
        # Check ETag
        file_path = f"{self.data_service.lakehouse_id}/Files/FCCS/FCC_ACCOUNT_BI.csv"
        has_changed, current_etag = self.data_service.connector.has_file_changed(file_path)
        stored_etag = self._get_stored_etag(self.ACCOUNTS_COLLECTION)
        
        if stored_etag == current_etag and not force_refresh:
            collection_info = self.qdrant_client.get_collection(self.ACCOUNTS_COLLECTION)
            logger.info(f"Accounts data unchanged (ETag match). Skipping embedding.")
            return {
                'total': collection_info.points_count,
                'new': 0,
                'skipped': collection_info.points_count,
                'reason': 'ETag unchanged - data not modified in OneLake'
            }
        
        # Get existing IDs
        existing_ids = self._get_existing_ids(self.ACCOUNTS_COLLECTION) if not force_refresh else set()
        
        # Load accounts from OneLake
        accounts_df = self.data_service.get_accounts()
        
        # Prepare new documents only
        new_texts = []
        new_payloads = []
        new_doc_ids = []
        skipped = 0
        
        for _, row in accounts_df.iterrows():
            account = str(row['Account'])
            parent = str(row['Parent']) if pd.notna(row['Parent']) else ""
            description = str(row['Description']) if pd.notna(row.get('Description')) else ""
            
            doc_id = self._generate_doc_id(self.ACCOUNTS_COLLECTION, account)
            
            if doc_id in existing_ids:
                skipped += 1
                continue
            
            text = f"Account: {account}. Parent: {parent}. {description}"
            new_texts.append(text)
            
            new_payloads.append({
                'account': account,
                'parent': parent,
                'description': description,
                'text': text,
                'doc_id': doc_id,
            })
            new_doc_ids.append(doc_id)
        
        if not new_texts:
            logger.info(f"No new accounts to embed. Skipped: {skipped}")
            self._store_etag(self.ACCOUNTS_COLLECTION, current_etag)
            return {
                'total': len(accounts_df),
                'new': 0,
                'skipped': skipped,
                'reason': 'All accounts already embedded'
            }
        
        # Create embeddings
        logger.info(f"Creating embeddings for {len(new_texts)} NEW accounts...")
        embeddings = self.create_embeddings_batch(new_texts)
        
        # Create points
        points = [
            PointStruct(
                id=doc_id,
                vector=embedding,
                payload=payload,
            )
            for doc_id, embedding, payload in zip(new_doc_ids, embeddings, new_payloads)
        ]
        
        # Upload in batches
        logger.info(f"Uploading {len(points)} points to Qdrant in batches...")
        self._upload_points_batched(self.ACCOUNTS_COLLECTION, points)
        
        # Store ETag
        self._store_etag(self.ACCOUNTS_COLLECTION, current_etag)
        
        logger.info(f"Embedded {len(points)} NEW accounts (skipped {skipped} existing)")
        return {
            'total': len(accounts_df),
            'new': len(points),
            'skipped': skipped,
        }
    
    def embed_entities(self, force_refresh: bool = False) -> Dict[str, int]:
        """Embed entity master data into Qdrant with deduplication and batching."""
        self.ensure_collection(self.ENTITIES_COLLECTION)
        
        file_path = f"{self.data_service.lakehouse_id}/Files/FCCS/FCC_ENTITY_BI.csv"
        has_changed, current_etag = self.data_service.connector.has_file_changed(file_path)
        stored_etag = self._get_stored_etag(self.ENTITIES_COLLECTION)
        
        if stored_etag == current_etag and not force_refresh:
            collection_info = self.qdrant_client.get_collection(self.ENTITIES_COLLECTION)
            logger.info(f"Entities data unchanged (ETag match). Skipping embedding.")
            return {
                'total': collection_info.points_count,
                'new': 0,
                'skipped': collection_info.points_count,
                'reason': 'ETag unchanged - data not modified in OneLake'
            }
        
        existing_ids = self._get_existing_ids(self.ENTITIES_COLLECTION) if not force_refresh else set()
        entities_df = self.data_service.get_entities()
        
        new_texts = []
        new_payloads = []
        new_doc_ids = []
        skipped = 0
        
        for _, row in entities_df.iterrows():
            entity = str(row['Entity'])
            parent = str(row['Parent']) if pd.notna(row['Parent']) else ""
            description = str(row['Description']) if pd.notna(row.get('Description')) else ""
            
            doc_id = self._generate_doc_id(self.ENTITIES_COLLECTION, entity)
            
            if doc_id in existing_ids:
                skipped += 1
                continue
            
            text = f"Entity: {entity}. Parent: {parent}. {description}"
            new_texts.append(text)
            
            new_payloads.append({
                'entity': entity,
                'parent': parent,
                'description': description,
                'text': text,
                'doc_id': doc_id,
            })
            new_doc_ids.append(doc_id)
        
        if not new_texts:
            logger.info(f"No new entities to embed. Skipped: {skipped}")
            self._store_etag(self.ENTITIES_COLLECTION, current_etag)
            return {
                'total': len(entities_df),
                'new': 0,
                'skipped': skipped,
                'reason': 'All entities already embedded'
            }
        
        logger.info(f"Creating embeddings for {len(new_texts)} NEW entities...")
        embeddings = self.create_embeddings_batch(new_texts)
        
        points = [
            PointStruct(
                id=doc_id,
                vector=embedding,
                payload=payload,
            )
            for doc_id, embedding, payload in zip(new_doc_ids, embeddings, new_payloads)
        ]
        
        logger.info(f"Uploading {len(points)} points to Qdrant in batches...")
        self._upload_points_batched(self.ENTITIES_COLLECTION, points)
        
        self._store_etag(self.ENTITIES_COLLECTION, current_etag)
        
        logger.info(f"Embedded {len(points)} NEW entities (skipped {skipped} existing)")
        return {
            'total': len(entities_df),
            'new': len(points),
            'skipped': skipped,
        }
    
    def embed_departments(self, force_refresh: bool = False) -> Dict[str, int]:
        """Embed department master data into Qdrant with deduplication and batching."""
        self.ensure_collection(self.DEPARTMENTS_COLLECTION)
        
        file_path = f"{self.data_service.lakehouse_id}/Files/FCCS/FCC_DEPARTMENT_BI.csv"
        has_changed, current_etag = self.data_service.connector.has_file_changed(file_path)
        stored_etag = self._get_stored_etag(self.DEPARTMENTS_COLLECTION)
        
        if stored_etag == current_etag and not force_refresh:
            collection_info = self.qdrant_client.get_collection(self.DEPARTMENTS_COLLECTION)
            logger.info(f"Departments data unchanged (ETag match). Skipping embedding.")
            return {
                'total': collection_info.points_count,
                'new': 0,
                'skipped': collection_info.points_count,
                'reason': 'ETag unchanged - data not modified in OneLake'
            }
        
        existing_ids = self._get_existing_ids(self.DEPARTMENTS_COLLECTION) if not force_refresh else set()
        departments_df = self.data_service.get_departments()
        
        new_texts = []
        new_payloads = []
        new_doc_ids = []
        skipped = 0
        
        for _, row in departments_df.iterrows():
            department = str(row['Department'])
            parent = str(row['Parent']) if pd.notna(row['Parent']) else ""
            description = str(row['Description']) if pd.notna(row.get('Description')) else ""
            
            doc_id = self._generate_doc_id(self.DEPARTMENTS_COLLECTION, department)
            
            if doc_id in existing_ids:
                skipped += 1
                continue
            
            text = f"Department: {department}. Parent: {parent}. {description}"
            new_texts.append(text)
            
            new_payloads.append({
                'department': department,
                'parent': parent,
                'description': description,
                'text': text,
                'doc_id': doc_id,
            })
            new_doc_ids.append(doc_id)
        
        if not new_texts:
            logger.info(f"No new departments to embed. Skipped: {skipped}")
            self._store_etag(self.DEPARTMENTS_COLLECTION, current_etag)
            return {
                'total': len(departments_df),
                'new': 0,
                'skipped': skipped,
                'reason': 'All departments already embedded'
            }
        
        logger.info(f"Creating embeddings for {len(new_texts)} NEW departments...")
        embeddings = self.create_embeddings_batch(new_texts)
        
        points = [
            PointStruct(
                id=doc_id,
                vector=embedding,
                payload=payload,
            )
            for doc_id, embedding, payload in zip(new_doc_ids, embeddings, new_payloads)
        ]
        
        logger.info(f"Uploading {len(points)} points to Qdrant in batches...")
        self._upload_points_batched(self.DEPARTMENTS_COLLECTION, points)
        
        self._store_etag(self.DEPARTMENTS_COLLECTION, current_etag)
        
        logger.info(f"Embedded {len(points)} NEW departments (skipped {skipped} existing)")
        return {
            'total': len(departments_df),
            'new': len(points),
            'skipped': skipped,
        }
    
    def embed_all(self, force_refresh: bool = False) -> Dict[str, Dict[str, int]]:
        """Embed all data sources into Qdrant."""
        results = {
            'accounts': self.embed_accounts(force_refresh),
            'entities': self.embed_entities(force_refresh),
            'departments': self.embed_departments(force_refresh),
        }
        
        total_new = sum(r.get('new', 0) for r in results.values())
        total_skipped = sum(r.get('skipped', 0) for r in results.values())
        
        logger.info(f"Embedding complete: {total_new} new, {total_skipped} skipped")
        
        return results
    
    def search_accounts(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search accounts by semantic similarity."""
        query_embedding = self.create_embedding(query)
        
        results = self.qdrant_client.search(
            collection_name=self.ACCOUNTS_COLLECTION,
            query_vector=query_embedding,
            limit=limit,
        )
        
        return [
            {
                'account': hit.payload['account'],
                'parent': hit.payload['parent'],
                'description': hit.payload.get('description', ''),
                'score': hit.score,
            }
            for hit in results
        ]
    
    def search_entities(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search entities by semantic similarity."""
        query_embedding = self.create_embedding(query)
        
        results = self.qdrant_client.search(
            collection_name=self.ENTITIES_COLLECTION,
            query_vector=query_embedding,
            limit=limit,
        )
        
        return [
            {
                'entity': hit.payload['entity'],
                'parent': hit.payload['parent'],
                'description': hit.payload.get('description', ''),
                'score': hit.score,
            }
            for hit in results
        ]
    
    def search_departments(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search departments by semantic similarity."""
        query_embedding = self.create_embedding(query)
        
        results = self.qdrant_client.search(
            collection_name=self.DEPARTMENTS_COLLECTION,
            query_vector=query_embedding,
            limit=limit,
        )
        
        return [
            {
                'department': hit.payload['department'],
                'parent': hit.payload['parent'],
                'description': hit.payload.get('description', ''),
                'score': hit.score,
            }
            for hit in results
        ]
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics for all collections."""
        stats = {}
        
        for collection_name in [
            self.ACCOUNTS_COLLECTION, 
            self.ENTITIES_COLLECTION, 
            self.DEPARTMENTS_COLLECTION,
            self.METADATA_COLLECTION,
        ]:
            try:
                info = self.qdrant_client.get_collection(collection_name)
                stats[collection_name] = {
                    'points_count': info.points_count,
                    'vectors_count': info.vectors_count,
                    'status': info.status.value,
                }
            except Exception as e:
                stats[collection_name] = {'error': str(e)}
        
        return stats


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("📊 Embedding Service Test - WITH BATCH UPLOAD")
    print("=" * 60)
    
    service = EmbeddingService()
    
    # Step 1: First run
    print("\n1. FIRST RUN - Embedding all data (in batches)...")
    results = service.embed_all(force_refresh=False)
    
    for name, stats in results.items():
        print(f"\n   📊 {name.upper()}:")
        print(f"      Total: {stats.get('total', 0)}")
        print(f"      New: {stats.get('new', 0)}")
        print(f"      Skipped: {stats.get('skipped', 0)}")
        if 'reason' in stats:
            print(f"      Reason: {stats['reason']}")
    
    # Step 2: Second run
    print("\n" + "=" * 60)
    print("2. SECOND RUN - Should skip (ETag unchanged)...")
    print("=" * 60)
    
    results2 = service.embed_all(force_refresh=False)
    
    for name, stats in results2.items():
        print(f"\n   📊 {name.upper()}:")
        print(f"      New: {stats.get('new', 0)}")
        print(f"      Skipped: {stats.get('skipped', 0)}")
        if 'reason' in stats:
            print(f"      ✅ {stats['reason']}")
    
    # Step 3: Collection stats
    print("\n" + "=" * 60)
    print("3. Collection Statistics:")
    print("=" * 60)
    
    stats = service.get_collection_stats()
    for name, info in stats.items():
        print(f"   📊 {name}: {info}")
    
    # Step 4: Test semantic search
    print("\n" + "=" * 60)
    print("4. Testing Semantic Search:")
    print("=" * 60)
    
    test_queries = ["cash accounts", "receivables", "fixed assets"]
    
    for query in test_queries:
        print(f"\n   Query: '{query}'")
        results = service.search_accounts(query, limit=3)
        for r in results:
            print(f"      📄 {r['account']} (score: {r['score']:.3f})")
    
    print("\n" + "=" * 60)
    print("✅ Embedding Service Complete!")
    print("=" * 60)