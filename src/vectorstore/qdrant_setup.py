"""
Qdrant Vector Database Setup
Creates collections for CFG Ukraine financial data
"""
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PayloadSchemaType,
)
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class QdrantSetup:
    """
    Sets up Qdrant collections for CFG Ukraine analytics.
    """
    
    # Collection configurations
    COLLECTIONS = {
        "financial_statements": {
            "description": "P&L, Balance Sheet, Cash Flow statements",
            "vector_size": 1536,  # OpenAI text-embedding-3-small
        },
        "operational_kpis": {
            "description": "Production, yield, expenses, volumes",
            "vector_size": 1536,
        },
        "budget_forecast": {
            "description": "Budget, forecast, and LTP data",
            "vector_size": 1536,
        },
        "treasury": {
            "description": "Bank positions, payment schedules",
            "vector_size": 1536,
        },
    }
    
    def __init__(self):
        self.settings = get_settings()
        self.client = QdrantClient(
            host=self.settings.qdrant_host,
            port=self.settings.qdrant_port,
        )
    
    def create_collection(self, name: str, vector_size: int = 1536):
        """Create a single collection if it doesn't exist."""
        
        # Check if collection exists
        collections = self.client.get_collections().collections
        existing_names = [c.name for c in collections]
        
        if name in existing_names:
            logger.info(f"Collection '{name}' already exists, skipping...")
            return False
        
        # Create collection
        self.client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),
        )
        
        logger.info(f"✅ Created collection: {name}")
        return True
    
    def setup_all_collections(self):
        """Create all required collections."""
        logger.info("Setting up Qdrant collections...")
        
        created = 0
        for name, config in self.COLLECTIONS.items():
            if self.create_collection(name, config["vector_size"]):
                created += 1
        
        logger.info(f"Setup complete. Created {created} new collections.")
        return self.list_collections()
    
    def list_collections(self):
        """List all collections."""
        collections = self.client.get_collections().collections
        return [
            {
                "name": c.name,
                "vectors_count": self.client.get_collection(c.name).vectors_count,
            }
            for c in collections
        ]
    
    def delete_all_collections(self):
        """Delete all collections (use with caution!)."""
        collections = self.client.get_collections().collections
        for c in collections:
            self.client.delete_collection(c.name)
            logger.info(f"Deleted collection: {c.name}")
    
    def health_check(self) -> dict:
        """Check Qdrant connection health."""
        try:
            collections = self.client.get_collections()
            return {
                "status": "healthy",
                "collections_count": len(collections.collections),
                "message": "✅ Qdrant is connected and operational"
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "message": "❌ Qdrant connection failed"
            }


# Entry point for testing
if __name__ == "__main__":
    print("=" * 50)
    print("🔧 Qdrant Setup for CFG Ukraine Analytics")
    print("=" * 50)
    
    setup = QdrantSetup()
    
    # Health check
    health = setup.health_check()
    print(f"\nHealth: {health['message']}")
    
    # Setup collections
    if health["status"] == "healthy":
        collections = setup.setup_all_collections()
        print(f"\n📊 Collections:")
        for c in collections:
            print(f"   - {c['name']}: {c['vectors_count']} vectors")
    
    print("=" * 50)