"""
Health check endpoints
"""
from fastapi import APIRouter
from src.vectorstore.qdrant_setup import QdrantSetup
from src.connectors.onelake_connector import OneLakeConnector
from src.utils.config import get_settings
import redis

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("")
async def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "cfg-ukraine-analytics"}


@router.get("/detailed")
async def detailed_health_check():
    """Detailed health check with all dependencies"""
    settings = get_settings()
    
    # Check Qdrant
    try:
        qdrant = QdrantSetup()
        qdrant_status = qdrant.health_check()
    except Exception as e:
        qdrant_status = {"status": "unhealthy", "error": str(e)}
    
    # Check Redis
    try:
        r = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_connect_timeout=2
        )
        redis_ping = r.ping()
        redis_status = {"status": "healthy" if redis_ping else "unhealthy"}
    except Exception as e:
        redis_status = {"status": "unhealthy", "error": str(e)}
    
    # Check OpenAI
    openai_status = {
        "status": "configured" if settings.openai_api_key else "not_configured"
    }
    
    # Check OneLake
    try:
        onelake = OneLakeConnector()
        onelake_status = onelake.health_check()
    except Exception as e:
        onelake_status = {"status": "error", "error": str(e)}
    
    return {
        "status": "healthy",
        "service": "cfg-ukraine-analytics",
        "dependencies": {
            "qdrant": qdrant_status,
            "redis": redis_status,
            "openai": openai_status,
            "onelake": onelake_status,
        }
    }