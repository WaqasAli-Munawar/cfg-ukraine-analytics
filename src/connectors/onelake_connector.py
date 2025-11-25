"""
OneLake Connector for Microsoft Fabric
Connects to CFG Ukraine data in OneLake
"""
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime

from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OneLakeConnector:
    """
    Connects to Microsoft Fabric OneLake.
    Supports Bronze, Silver, and Gold layer access for CFG Ukraine data.
    """
    
    # OneLake endpoint
    ONELAKE_ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"
    
    def __init__(self):
        self.settings = get_settings()
        self.credential = self._create_credential()
        self.client: Optional[DataLakeServiceClient] = None
        
    def _create_credential(self):
        """Create Azure credential"""
        # If all credentials are provided, use Service Principal
        if all([
            self.settings.azure_tenant_id,
            self.settings.azure_client_id,
            self.settings.azure_client_secret,
        ]):
            logger.info("Using Service Principal authentication")
            return ClientSecretCredential(
                tenant_id=self.settings.azure_tenant_id,
                client_id=self.settings.azure_client_id,
                client_secret=self.settings.azure_client_secret,
            )
        else:
            # Fallback to DefaultAzureCredential (uses az login, env vars, etc.)
            logger.info("Using DefaultAzureCredential (fallback)")
            return DefaultAzureCredential()
    
    def connect(self) -> DataLakeServiceClient:
        """Initialize connection to OneLake"""
        if self.client is None:
            try:
                self.client = DataLakeServiceClient(
                    account_url=self.ONELAKE_ACCOUNT_URL,
                    credential=self.credential
                )
                logger.info(
                    "Connected to OneLake",
                    workspace_id=self.settings.onelake_workspace_id,
                    lakehouse_id=self.settings.onelake_lakehouse_id,
                )
            except Exception as e:
                logger.error(f"Failed to connect to OneLake: {e}")
                raise
        
        return self.client
    
    def get_file_system_client(self):
        """Get the file system client for the workspace"""
        client = self.connect()
        return client.get_file_system_client(
            file_system=self.settings.onelake_workspace_id
        )
    
    def list_tables(self, layer: str = "gold") -> List[str]:
        """
        List available tables in a data layer.
        
        Args:
            layer: Data layer - 'bronze', 'silver', or 'gold'
        
        Returns:
            List of table names
        """
        try:
            fs_client = self.get_file_system_client()
            lakehouse_path = (
                f"{self.settings.onelake_lakehouse_id}/Tables/{layer}"
            )
            
            tables = []
            paths = fs_client.get_paths(path=lakehouse_path)
            
            for path in paths:
                if path.is_directory:
                    table_name = path.name.split("/")[-1]
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} tables in {layer} layer")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def read_table(
        self, 
        table_name: str, 
        layer: str = "gold",
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read a table from OneLake as DataFrame.
        
        Args:
            table_name: Name of the table
            layer: Data layer - 'bronze', 'silver', or 'gold'
            limit: Optional row limit
        
        Returns:
            pandas DataFrame
        """
        try:
            from deltalake import DeltaTable
            
            # Construct OneLake path
            table_path = (
                f"abfss://{self.settings.onelake_workspace_id}@"
                f"onelake.dfs.fabric.microsoft.com/"
                f"{self.settings.onelake_lakehouse_id}/Tables/{layer}/{table_name}"
            )
            
            # Storage options for authentication
            storage_options = {}
            if self.settings.azure_client_secret:
                storage_options = {
                    "azure_tenant_id": self.settings.azure_tenant_id,
                    "azure_client_id": self.settings.azure_client_id,
                    "azure_client_secret": self.settings.azure_client_secret,
                }
            
            # Read Delta table
            dt = DeltaTable(table_path, storage_options=storage_options)
            df = dt.to_pandas()
            
            if limit:
                df = df.head(limit)
            
            logger.info(
                f"Loaded table {table_name}",
                rows=len(df),
                columns=list(df.columns),
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            raise
    
    def query_financial_summary(
        self,
        metrics: List[str],
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Query financial summary data from Gold layer.
        
        Args:
            metrics: List of metrics to retrieve (e.g., ['ebitda', 'revenue'])
            start_period: Start period (e.g., '2020-Q1')
            end_period: End period (e.g., '2024-Q4')
        
        Returns:
            DataFrame with requested metrics
        """
        try:
            # Read financial summary table from Gold layer
            df = self.read_table("fact_financial_summary", layer="gold")
            
            # Filter by period if specified
            if start_period:
                df = df[df['period'] >= start_period]
            if end_period:
                df = df[df['period'] <= end_period]
            
            # Select requested metrics (if table has them)
            available_cols = df.columns.tolist()
            metric_cols = [m for m in metrics if m in available_cols]
            
            if not metric_cols:
                logger.warning(f"Requested metrics {metrics} not found in table")
                return df
            
            # Return period + requested metrics
            result_cols = ['period'] + metric_cols
            result_cols = [c for c in result_cols if c in available_cols]
            
            return df[result_cols]
            
        except Exception as e:
            logger.error(f"Failed to query financial summary: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check OneLake connection health.
        
        Returns:
            Dictionary with connection status
        """
        try:
            # Check if credentials are configured
            if not all([
                self.settings.azure_tenant_id,
                self.settings.azure_client_id,
                self.settings.onelake_workspace_id,
                self.settings.onelake_lakehouse_id,
            ]):
                return {
                    "status": "not_configured",
                    "message": "❌ OneLake credentials not fully configured",
                    "missing": [
                        k for k, v in {
                            "azure_tenant_id": self.settings.azure_tenant_id,
                            "azure_client_id": self.settings.azure_client_id,
                            "azure_client_secret": self.settings.azure_client_secret,
                            "onelake_workspace_id": self.settings.onelake_workspace_id,
                            "onelake_lakehouse_id": self.settings.onelake_lakehouse_id,
                        }.items() if not v
                    ]
                }
            
            # Try to connect
            self.connect()
            
            # Try to list tables in Gold layer
            tables = self.list_tables("gold")
            
            return {
                "status": "healthy",
                "message": "✅ OneLake connected successfully",
                "workspace_id": self.settings.onelake_workspace_id,
                "lakehouse_id": self.settings.onelake_lakehouse_id,
                "gold_tables_count": len(tables),
                "gold_tables": tables[:5] if tables else [],  # Show first 5
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"❌ OneLake connection failed: {str(e)}",
                "error": str(e),
            }


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("🔗 OneLake Connector Test - CFG Ukraine")
    print("=" * 60)
    
    connector = OneLakeConnector()
    
    # Health check
    health = connector.health_check()
    print(f"\n{health['message']}")
    
    if health['status'] == 'not_configured':
        print("\n⚠️  Missing credentials:")
        for cred in health.get('missing', []):
            print(f"   - {cred}")
        print("\n💡 Add these to your .env file")
    
    elif health['status'] == 'healthy':
        print(f"\n✅ Workspace: {health['workspace_id']}")
        print(f"✅ Lakehouse: {health['lakehouse_id']}")
        print(f"\n📊 Gold Layer Tables ({health['gold_tables_count']}):")
        for table in health.get('gold_tables', []):
            print(f"   - {table}")
    
    else:
        print(f"\n❌ Error: {health.get('error', 'Unknown error')}")
        print("\n💡 This will work once you receive AZURE_CLIENT_SECRET from the client")
    
    print("=" * 60)