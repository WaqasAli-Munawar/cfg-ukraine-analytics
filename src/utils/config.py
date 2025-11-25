"""
Configuration management using Pydantic Settings
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
from functools import lru_cache
from dotenv import load_dotenv
import os

# Load .env file with override=True to ensure it takes priority
load_dotenv(".env", override=True)


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Application
    app_name: str = Field(default="cfg-ukraine-analytics")
    app_env: str = Field(default="development")
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")
    
    # Azure / OneLake
    azure_tenant_id: Optional[str] = Field(default=None)
    azure_client_id: Optional[str] = Field(default=None)
    azure_client_secret: Optional[str] = Field(default=None)
    onelake_workspace_id: Optional[str] = Field(default=None)
    onelake_lakehouse_id: Optional[str] = Field(default=None)
    
    # LLM Providers
    openai_api_key: Optional[str] = Field(default=None)
    
    # Qdrant
    qdrant_host: str = Field(default="localhost")
    qdrant_port: int = Field(default=6333)
    
    # Redis
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance"""
    # Clear cache if needed for fresh load
    return Settings()


def get_fresh_settings() -> Settings:
    """Get fresh settings without cache (useful for debugging)"""
    get_settings.cache_clear()
    return get_settings()


# Quick test
if __name__ == "__main__":
    settings = get_settings()
    print("=" * 50)
    print("✅ Configuration loaded successfully!")
    print("=" * 50)
    print(f"   App Name: {settings.app_name}")
    print(f"   Environment: {settings.app_env}")
    print(f"   Debug Mode: {settings.debug}")
    print(f"   Qdrant Host: {settings.qdrant_host}:{settings.qdrant_port}")
    print(f"   Redis Host: {settings.redis_host}:{settings.redis_port}")
    print(f"   OpenAI Key: {'✅ Set' if settings.openai_api_key else '❌ Not Set'}")
    if settings.openai_api_key:
        print(f"   OpenAI Key Preview: {settings.openai_api_key[:10]}...{settings.openai_api_key[-4:]}")
    print("=" * 50)