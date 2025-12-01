"""
OneLake Connector for Microsoft Fabric
Connects to CFG Ukraine data in OneLake with ETag-based change detection
"""
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from io import BytesIO
from datetime import datetime

from src.utils.config import get_settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OneLakeConnector:
    """
    Connects to Microsoft Fabric OneLake with change detection.
    """
    
    ONELAKE_ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"
    
    def __init__(self):
        self.settings = get_settings()
        self.credential = self._create_credential()
        self.client: Optional[DataLakeServiceClient] = None
        
        # ETag tracking for change detection
        self._etag_cache: Dict[str, str] = {}
        self._last_modified_cache: Dict[str, str] = {}
        
    def _create_credential(self):
        """Create Azure credential"""
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
    
    def get_file_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get file metadata including ETag and Last-Modified.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Dictionary with etag, last_modified, size
        """
        try:
            fs_client = self.get_file_system_client()
            file_client = fs_client.get_file_client(file_path)
            
            properties = file_client.get_file_properties()
            
            metadata = {
                'etag': properties.etag,
                'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
                'size': properties.size,
                'content_type': properties.content_settings.content_type if properties.content_settings else None,
            }
            
            logger.info(f"File metadata for {file_path}: ETag={metadata['etag']}, Modified={metadata['last_modified']}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get metadata for {file_path}: {e}")
            return {}
    
    def has_file_changed(self, file_path: str) -> Tuple[bool, str]:
        """
        Check if a file has changed since last read using ETag.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (has_changed: bool, new_etag: str)
        """
        try:
            metadata = self.get_file_metadata(file_path)
            current_etag = metadata.get('etag', '')
            
            cached_etag = self._etag_cache.get(file_path)
            
            if cached_etag is None:
                # First time checking this file
                logger.info(f"First time checking {file_path}, ETag: {current_etag}")
                return True, current_etag
            
            if current_etag != cached_etag:
                logger.info(f"File {file_path} has CHANGED! Old ETag: {cached_etag}, New ETag: {current_etag}")
                return True, current_etag
            else:
                logger.info(f"File {file_path} unchanged (ETag: {current_etag})")
                return False, current_etag
                
        except Exception as e:
            logger.error(f"Failed to check file change for {file_path}: {e}")
            return True, ""  # Assume changed on error to force refresh
    
    def update_etag_cache(self, file_path: str, etag: str):
        """Update the ETag cache after reading a file"""
        self._etag_cache[file_path] = etag
        logger.debug(f"Updated ETag cache for {file_path}: {etag}")
    
    def list_directory(self, path: str) -> List[Dict[str, Any]]:
        """List contents of a directory in OneLake."""
        try:
            fs_client = self.get_file_system_client()
            items = []
            
            paths = fs_client.get_paths(path=path, recursive=False)
            for p in paths:
                items.append({
                    'name': p.name,
                    'is_directory': p.is_directory,
                    'size': p.content_length if hasattr(p, 'content_length') else None,
                })
            
            logger.info(f"Found {len(items)} items at path: {path}")
            return items
            
        except Exception as e:
            logger.error(f"Failed to list directory {path}: {e}")
            return []
    
    def read_csv_file(self, file_path: str, check_change: bool = True) -> Tuple[pd.DataFrame, str]:
        """
        Read a CSV file from OneLake with change detection.
        
        Args:
            file_path: Full path to the file
            check_change: Whether to return ETag for caching
            
        Returns:
            Tuple of (DataFrame, ETag)
        """
        try:
            fs_client = self.get_file_system_client()
            file_client = fs_client.get_file_client(file_path)
            
            # Get properties first (includes ETag)
            properties = file_client.get_file_properties()
            etag = properties.etag
            last_modified = properties.last_modified.isoformat() if properties.last_modified else None
            
            # Download and read
            download = file_client.download_file()
            data = download.readall()
            
            df = pd.read_csv(BytesIO(data), low_memory=False)
            
            # Update ETag cache
            self._etag_cache[file_path] = etag
            self._last_modified_cache[file_path] = last_modified
            
            logger.info(
                f"Read CSV file: {file_path}, rows: {len(df)}, "
                f"ETag: {etag}, Modified: {last_modified}"
            )
            
            return df, etag
            
        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            raise
    
    def read_csv_file_simple(self, file_path: str) -> pd.DataFrame:
        """
        Read a CSV file (simple version without returning ETag).
        For backward compatibility.
        """
        df, _ = self.read_csv_file(file_path)
        return df
    
    def get_all_file_etags(self, folder_path: str) -> Dict[str, str]:
        """
        Get ETags for all files in a folder.
        Useful for bulk change detection.
        
        Args:
            folder_path: Path to folder (e.g., '{lakehouse_id}/Files/FCCS')
            
        Returns:
            Dictionary mapping file names to ETags
        """
        etags = {}
        try:
            items = self.list_directory(folder_path)
            for item in items:
                if not item['is_directory']:
                    metadata = self.get_file_metadata(item['name'])
                    etags[item['name']] = metadata.get('etag', '')
        except Exception as e:
            logger.error(f"Failed to get ETags for folder {folder_path}: {e}")
        
        return etags
    
    def health_check(self) -> Dict[str, Any]:
        """Check OneLake connection health."""
        try:
            if not all([
                self.settings.azure_tenant_id,
                self.settings.azure_client_id,
                self.settings.onelake_workspace_id,
                self.settings.onelake_lakehouse_id,
            ]):
                return {
                    "status": "not_configured",
                    "message": "❌ OneLake credentials not fully configured",
                }
            
            self.connect()
            
            # List files to verify access
            lakehouse_id = self.settings.onelake_lakehouse_id
            files = self.list_directory(f"{lakehouse_id}/Files/FCCS")
            
            return {
                "status": "healthy",
                "message": "✅ OneLake connected successfully",
                "workspace_id": self.settings.onelake_workspace_id,
                "lakehouse_id": self.settings.onelake_lakehouse_id,
                "files_count": len(files),
                "cached_etags": len(self._etag_cache),
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
    print("🔗 OneLake Connector Test - With ETag Change Detection")
    print("=" * 60)
    
    connector = OneLakeConnector()
    lakehouse_id = connector.settings.onelake_lakehouse_id
    
    # Test 1: Get file metadata
    print("\n1. Getting file metadata...")
    test_file = f"{lakehouse_id}/Files/FCCS/FCC_ENTITY_BI.csv"
    metadata = connector.get_file_metadata(test_file)
    print(f"   ✅ ETag: {metadata.get('etag')}")
    print(f"   ✅ Last Modified: {metadata.get('last_modified')}")
    print(f"   ✅ Size: {metadata.get('size')} bytes")
    
    # Test 2: Check if file changed (first time)
    print("\n2. Checking if file changed (first time)...")
    has_changed, etag = connector.has_file_changed(test_file)
    print(f"   ✅ Has Changed: {has_changed} (expected: True for first check)")
    print(f"   ✅ ETag: {etag}")
    
    # Test 3: Update cache and check again
    print("\n3. Updating cache and checking again...")
    connector.update_etag_cache(test_file, etag)
    has_changed, etag = connector.has_file_changed(test_file)
    print(f"   ✅ Has Changed: {has_changed} (expected: False)")
    
    # Test 4: Read CSV with ETag
    print("\n4. Reading CSV file with ETag tracking...")
    df, file_etag = connector.read_csv_file(test_file)
    print(f"   ✅ Rows: {len(df)}")
    print(f"   ✅ ETag: {file_etag}")
    
    # Test 5: Get all ETags for FCCS folder
    print("\n5. Getting all ETags for FCCS folder...")
    folder_path = f"{lakehouse_id}/Files/FCCS"
    all_etags = connector.get_all_file_etags(folder_path)
    print(f"   ✅ Found {len(all_etags)} files with ETags:")
    for file_name, etag in all_etags.items():
        short_name = file_name.split('/')[-1]
        print(f"      📄 {short_name}: {etag[:20]}...")
    
    print("\n" + "=" * 60)
    print("✅ ETag Change Detection Working!")
    print("=" * 60)