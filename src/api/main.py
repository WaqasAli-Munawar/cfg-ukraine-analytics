"""
FastAPI Application - CFG Ukraine Analytics
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.api.routes import health, query
from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup
    logger.info("🚀 Starting CFG Ukraine Analytics API...")
    logger.info(f"   Environment: {settings.app_env}")
    logger.info(f"   Debug: {settings.debug}")
    yield
    # Shutdown
    logger.info("👋 Shutting down CFG Ukraine Analytics API...")


app = FastAPI(
    title="CFG Ukraine Analytics API",
    description="Agentic RAG system for financial and operational analytics",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(query.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "CFG Ukraine Analytics API",
        "version": "0.1.0",
        "status": "running",
        "docs": "/docs",
    }


# Entry point for running directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )