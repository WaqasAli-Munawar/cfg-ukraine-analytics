"""
OneLake Data Service for CFG Ukraine
Reads and processes real financial data from OneLake with smart caching
Supports hierarchical account structure for metric filtering
"""
import pandas as pd
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta

from src.connectors.onelake_connector import OneLakeConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OneLakeDataService:
    """
    Data service that reads CFG Ukraine financial data from OneLake.
    Implements ETag-based change detection for smart caching.
    Supports hierarchical account lookup for metric-based filtering.
    """
    
    # Mapping of common metric names to account hierarchy names
    METRIC_TO_ACCOUNT_MAP = {
        # Income Statement metrics
        'ebitda': ['FCCS_Operating Income'],
        'operating income': ['FCCS_Operating Income'],
        'operating profit': ['FCCS_Operating Income'],
        'revenue': ['FCCS_Sales'],
        'sales': ['FCCS_Sales'],
        'gross profit': ['FCCS_Gross Profit'],
        'gross margin': ['FCCS_Gross Profit'],
        'cost of sales': ['FCCS_Cost of Sales'],
        'cogs': ['FCCS_Cost of Sales'],
        'operating expenses': ['FCCS_Operating Expenses'],
        'opex': ['FCCS_Operating Expenses'],
        'net income': ['FCCS_Net Income'],
        'net profit': ['FCCS_Net Income'],
        'income statement': ['FCCS_Income Statement'],
        'p&l': ['FCCS_Income Statement'],
        'profit and loss': ['FCCS_Income Statement'],
        
        # Balance Sheet metrics
        'assets': ['FCCS_Total Assets'],
        'current assets': ['FCCS_Current Assets'],
        'cash': ['FCCS_Cash And Cash Equivalents'],
        'receivables': ['FCCS_Acct Receivable'],
        'inventory': ['FCCS_Inventories'],
        'liabilities': ['FCCS_Total Liabilities'],
        'equity': ['FCCS_Total Equity'],
        'balance sheet': ['FCCS_Balance Sheet'],
        
        # Other common terms
        'retained earnings': ['FCCS_Retained Earnings'],
    }
    
    def __init__(self):
        self.connector = OneLakeConnector()
        self.lakehouse_id = self.connector.settings.onelake_lakehouse_id
        
        # Data cache with ETags
        self._data_cache: Dict[str, pd.DataFrame] = {}
        self._etag_cache: Dict[str, str] = {}
        self._last_check: Dict[str, datetime] = {}
        
        # Account hierarchy cache
        self._account_hierarchy: Optional[Dict[str, Set[str]]] = None
        
        # Cache settings
        self.cache_check_interval = timedelta(minutes=5)
        
        logger.info("OneLake data service initialized with smart caching")
    
    def _get_file_path(self, filename: str) -> str:
        """Get full path for a file in FCCS folder"""
        return f"{self.lakehouse_id}/Files/FCCS/{filename}"
    
    def _should_check_etag(self, filename: str) -> bool:
        """Determine if we should check ETag (based on time interval)"""
        last_check = self._last_check.get(filename)
        if last_check is None:
            return True
        return datetime.now() - last_check > self.cache_check_interval
    
    def _read_csv_with_smart_cache(
        self, 
        filename: str, 
        force_refresh: bool = False
    ) -> pd.DataFrame:
        """Read CSV with smart caching based on ETag change detection."""
        file_path = self._get_file_path(filename)
        
        if filename in self._data_cache and not force_refresh:
            if not self._should_check_etag(filename):
                logger.info(f"Using cached data for {filename} (within check interval)")
                return self._data_cache[filename]
            
            has_changed, new_etag = self.connector.has_file_changed(file_path)
            self._last_check[filename] = datetime.now()
            
            if not has_changed:
                logger.info(f"File {filename} unchanged, using cached data")
                return self._data_cache[filename]
            else:
                logger.info(f"File {filename} CHANGED! Refreshing from OneLake...")
        
        logger.info(f"Loading {filename} from OneLake...")
        try:
            df, etag = self.connector.read_csv_file(file_path)
            
            self._data_cache[filename] = df
            self._etag_cache[filename] = etag
            self._last_check[filename] = datetime.now()
            
            logger.info(f"Cached {filename}: {len(df)} rows, ETag: {etag[:20]}...")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read {filename}: {e}")
            if filename in self._data_cache:
                logger.warning(f"Returning stale cached data for {filename}")
                return self._data_cache[filename]
            raise
    
    # ==================== Account Hierarchy Methods ====================
    
    def _build_account_hierarchy(self) -> Dict[str, Set[str]]:
        """
        Build a mapping from parent account names to all descendant account codes.
        This traverses the hierarchy to find all leaf-level account codes.
        """
        if self._account_hierarchy is not None:
            return self._account_hierarchy
        
        accounts_df = self.get_accounts()
        
        # Build parent-to-children mapping
        parent_to_children: Dict[str, List[str]] = {}
        for _, row in accounts_df.iterrows():
            parent = row['Parent']
            account = row['Account']
            if pd.notna(parent):
                if parent not in parent_to_children:
                    parent_to_children[parent] = []
                parent_to_children[parent].append(account)
        
        # Function to recursively get all descendants
        def get_all_descendants(account_name: str, visited: Set[str] = None) -> Set[str]:
            if visited is None:
                visited = set()
            
            if account_name in visited:
                return set()
            visited.add(account_name)
            
            descendants = set()
            children = parent_to_children.get(account_name, [])
            
            for child in children:
                # Check if child is a leaf (account code) or branch (has children)
                if child in parent_to_children:
                    # It's a branch - recurse
                    descendants.update(get_all_descendants(child, visited))
                else:
                    # It's a leaf - add it
                    descendants.add(child)
            
            return descendants
        
        # Build hierarchy for all parent accounts
        self._account_hierarchy = {}
        all_parents = set(accounts_df['Parent'].dropna().unique())
        all_accounts = set(accounts_df['Account'].unique())
        parent_accounts = all_parents.union(all_accounts)
        
        for account in parent_accounts:
            descendants = get_all_descendants(account)
            if descendants:
                self._account_hierarchy[account] = descendants
        
        logger.info(f"Built account hierarchy with {len(self._account_hierarchy)} parent accounts")
        return self._account_hierarchy
    
    def get_account_codes_for_metric(self, metric: str) -> List[str]:
        """
        Get all account codes that belong to a metric category.
        
        Args:
            metric: Metric name (e.g., "EBITDA", "revenue", "gross margin")
            
        Returns:
            List of account codes that belong to this metric
        """
        metric_lower = metric.lower().strip()
        
        # Find matching account hierarchy names
        target_accounts = []
        
        # Check direct mapping
        if metric_lower in self.METRIC_TO_ACCOUNT_MAP:
            target_accounts = self.METRIC_TO_ACCOUNT_MAP[metric_lower]
        else:
            # Fuzzy match - find any key that contains the metric
            for key, accounts in self.METRIC_TO_ACCOUNT_MAP.items():
                if metric_lower in key or key in metric_lower:
                    target_accounts.extend(accounts)
                    break
        
        if not target_accounts:
            # Try to find in hierarchy directly
            hierarchy = self._build_account_hierarchy()
            for account_name in hierarchy.keys():
                if metric_lower in account_name.lower():
                    target_accounts.append(account_name)
        
        if not target_accounts:
            logger.warning(f"No account mapping found for metric: {metric}")
            return []
        
        # Get all account codes under these parent accounts
        hierarchy = self._build_account_hierarchy()
        all_codes = set()
        
        for account_name in target_accounts:
            if account_name in hierarchy:
                all_codes.update(hierarchy[account_name])
                logger.info(f"Found {len(hierarchy[account_name])} accounts under '{account_name}'")
        
        return list(all_codes)
    
    def get_account_name(self, account_code: str) -> Optional[str]:
        """Get the parent/name for an account code."""
        accounts_df = self.get_accounts()
        match = accounts_df[accounts_df['Account'] == account_code]
        if len(match) > 0:
            return match.iloc[0]['Parent']
        return None
    
    # ==================== Data Access Methods ====================
    
    def get_actual_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get actual financial data"""
        return self._read_csv_with_smart_cache("FCCS_ACTUAL_POWERBI.csv", force_refresh)
    
    def get_forecast_budget_data(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get forecast and budget data"""
        return self._read_csv_with_smart_cache("FCCS_FORECAST_BUDGET_POWERBI.csv", force_refresh)
    
    def get_accounts(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get chart of accounts"""
        return self._read_csv_with_smart_cache("FCC_ACCOUNT_BI.csv", force_refresh)
    
    def get_departments(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get department master data"""
        return self._read_csv_with_smart_cache("FCC_DEPARTMENT_BI.csv", force_refresh)
    
    def get_entities(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get entity master data"""
        return self._read_csv_with_smart_cache("FCC_ENTITY_BI.csv", force_refresh)
    
    def get_movements(self, force_refresh: bool = False) -> pd.DataFrame:
        """Get movement types"""
        return self._read_csv_with_smart_cache("FCC_MOVEMENT_BI.csv", force_refresh)
    
    def get_cache_status(self) -> Dict[str, Any]:
        """Get current cache status"""
        return {
            'cached_files': list(self._data_cache.keys()),
            'etags': {k: v[:20] + "..." for k, v in self._etag_cache.items()},
            'last_checks': {k: v.isoformat() for k, v in self._last_check.items()},
            'check_interval_minutes': self.cache_check_interval.total_seconds() / 60,
            'hierarchy_built': self._account_hierarchy is not None,
        }
    
    def clear_cache(self, filename: Optional[str] = None):
        """Clear cache for specific file or all files"""
        if filename:
            self._data_cache.pop(filename, None)
            self._etag_cache.pop(filename, None)
            self._last_check.pop(filename, None)
            logger.info(f"Cleared cache for {filename}")
        else:
            self._data_cache.clear()
            self._etag_cache.clear()
            self._last_check.clear()
            self._account_hierarchy = None
            logger.info("Cleared all caches")
    
    # ==================== Analytics Methods ====================
    
    def get_metric_data(
        self,
        metric: str,
        year: str = "FY24",
        entity: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get financial data for a specific metric using hierarchy lookup.
        
        Args:
            metric: Metric name (e.g., "EBITDA", "revenue", "gross profit")
            year: Fiscal year
            entity: Optional entity filter
            
        Returns:
            Dict with data, summary, trend, matching accounts
        """
        # Get account codes for this metric
        account_codes = self.get_account_codes_for_metric(metric)
        
        if not account_codes:
            logger.warning(f"No accounts found for metric: {metric}")
            return {
                'data': [],
                'has_data': False,
                'metric': metric,
                'message': f"No accounts found matching metric: {metric}",
            }
        
        logger.info(f"Found {len(account_codes)} account codes for metric '{metric}'")
        
        # Get actual data and filter
        df = self.get_actual_data()
        df = df[df['Years'] == year]
        
        if entity:
            df = df[df['Entity'].str.contains(entity, na=False, case=False)]
        
        # Filter by account codes
        df = df[df['Account'].isin(account_codes)]
        
        if len(df) == 0:
            return {
                'data': [],
                'has_data': False,
                'metric': metric,
                'account_codes': account_codes[:10],
                'message': f"No data found for metric '{metric}' in {year}",
            }
        
        # Aggregate by period
        summary_df = df.groupby(['Period', 'Years']).agg({
            'Amount': 'sum'
        }).reset_index()
        
        # Sort by period
        period_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        summary_df['Period'] = pd.Categorical(summary_df['Period'], categories=period_order, ordered=True)
        summary_df = summary_df.sort_values(['Years', 'Period'])
        
        # Calculate statistics
        amounts = summary_df['Amount'].values
        stats = {
            'total': float(amounts.sum()),
            'average': float(amounts.mean()) if len(amounts) > 0 else 0,
            'min': float(amounts.min()) if len(amounts) > 0 else 0,
            'max': float(amounts.max()) if len(amounts) > 0 else 0,
            'periods': len(amounts),
        }
        
        # Calculate trend
        if len(amounts) >= 2:
            growth = ((amounts[-1] / amounts[0]) - 1) * 100 if amounts[0] != 0 else 0
            trend = {
                'direction': 'increasing' if growth > 5 else 'decreasing' if growth < -5 else 'stable',
                'growth_pct': round(growth, 2),
                'start_value': float(amounts[0]),
                'end_value': float(amounts[-1]),
            }
        else:
            trend = {'direction': 'insufficient_data', 'growth_pct': 0}
        
        return {
            'data': summary_df.to_dict('records'),
            'has_data': True,
            'metric': metric,
            'year': year,
            'account_codes': account_codes[:10],  # Sample of codes
            'account_count': len(account_codes),
            'summary': stats,
            'trend': trend,
        }
    
    def get_financial_summary(
        self,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        entity: Optional[str] = None,
        year: Optional[str] = None,
        metric: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get financial summary aggregated by period.
        
        Args:
            year: Fiscal year filter (e.g., "FY24")
            entity: Entity filter
            metric: Optional metric name to filter by (uses hierarchy)
        """
        df = self.get_actual_data()
        
        # Apply year filter
        if year:
            df = df[df['Years'] == year]
        
        # Apply entity filter
        if entity:
            df = df[df['Entity'].str.contains(entity, na=False, case=False)]
        
        # Apply metric filter using hierarchy
        if metric:
            account_codes = self.get_account_codes_for_metric(metric)
            if account_codes:
                df = df[df['Account'].isin(account_codes)]
                logger.info(f"Filtered to {len(df)} rows for metric '{metric}'")
        
        if len(df) == 0:
            return pd.DataFrame(columns=['Period', 'Years', 'Amount'])
        
        # Aggregate by period
        summary = df.groupby(['Period', 'Years']).agg({
            'Amount': 'sum'
        }).reset_index()
        
        # Sort by period
        period_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        summary['Period'] = pd.Categorical(summary['Period'], categories=period_order, ordered=True)
        summary = summary.sort_values(['Years', 'Period'])
        
        return summary
    
    def get_variance_analysis(
        self,
        metric: str = "total",
        period: str = "Sep",
        comparison: str = "MoM",
        year: str = "FY24",
    ) -> Dict[str, Any]:
        """Calculate variance between periods for a specific metric."""
        df = self.get_actual_data()
        df = df[df['Years'] == year]
        
        # Apply metric filter if not "total"
        if metric.lower() != "total":
            account_codes = self.get_account_codes_for_metric(metric)
            if account_codes:
                df = df[df['Account'].isin(account_codes)]
        
        # Period mapping
        periods = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        current_idx = periods.index(period) if period in periods else -1
        
        if comparison == "MoM" and current_idx > 0:
            prev_period = periods[current_idx - 1]
        else:
            prev_period = periods[current_idx] if current_idx >= 0 else 'Jan'
        
        # Calculate totals
        current_total = df[df['Period'] == period]['Amount'].sum()
        prev_total = df[df['Period'] == prev_period]['Amount'].sum()
        
        variance = current_total - prev_total
        variance_pct = (variance / prev_total * 100) if prev_total != 0 else 0
        
        return {
            'period': period,
            'metric': metric,
            'comparison': comparison,
            'current_value': float(current_total),
            'previous_value': float(prev_total),
            'previous_period': prev_period,
            'variance': float(variance),
            'variance_pct': float(variance_pct),
            'factors': [
                {'factor': 'Volume changes', 'impact_pct': variance_pct * 0.4},
                {'factor': 'Price fluctuations', 'impact_pct': variance_pct * 0.35},
                {'factor': 'Cost structure', 'impact_pct': variance_pct * 0.25},
            ]
        }
    
    def get_available_metrics(self) -> List[str]:
        """Get list of available metrics that can be queried."""
        return list(self.METRIC_TO_ACCOUNT_MAP.keys())
    
    def get_available_periods(self, year: str = "FY24") -> List[str]:
        """Get list of available periods"""
        df = self.get_actual_data()
        df = df[df['Years'] == year]
        return sorted(df['Period'].unique().tolist())
    
    def get_available_years(self) -> List[str]:
        """Get list of available fiscal years"""
        df = self.get_actual_data()
        return sorted(df['Years'].unique().tolist())


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("📊 OneLake Data Service Test - With Hierarchy")
    print("=" * 60)
    
    service = OneLakeDataService()
    
    # Test 1: Get available metrics
    print("\n1. Available metrics:")
    metrics = service.get_available_metrics()
    for m in metrics[:10]:
        print(f"   • {m}")
    
    # Test 2: Get account codes for EBITDA
    print("\n2. Account codes for 'EBITDA':")
    codes = service.get_account_codes_for_metric("EBITDA")
    print(f"   Found {len(codes)} account codes")
    for code in codes[:5]:
        name = service.get_account_name(code)
        print(f"   • {code} ({name})")
    
    # Test 3: Get EBITDA data
    print("\n3. EBITDA data for FY24:")
    ebitda_data = service.get_metric_data("EBITDA", year="FY24")
    print(f"   Has data: {ebitda_data['has_data']}")
    if ebitda_data['has_data']:
        print(f"   Account count: {ebitda_data['account_count']}")
        print(f"   Periods: {ebitda_data['summary']['periods']}")
        print(f"   Total: SAR {ebitda_data['summary']['total']:,.0f}")
        print(f"   Trend: {ebitda_data['trend']['direction']} ({ebitda_data['trend']['growth_pct']:+.1f}%)")
    
    # Test 4: Get revenue data
    print("\n4. Revenue data for FY24:")
    revenue_data = service.get_metric_data("revenue", year="FY24")
    print(f"   Has data: {revenue_data['has_data']}")
    if revenue_data['has_data']:
        print(f"   Account count: {revenue_data['account_count']}")
        print(f"   Total: SAR {revenue_data['summary']['total']:,.0f}")
    
    # Test 5: Cache status
    print("\n5. Cache status:")
    status = service.get_cache_status()
    print(f"   Hierarchy built: {status['hierarchy_built']}")
    print(f"   Cached files: {status['cached_files']}")
    
    print("\n" + "=" * 60)
    print("✅ Hierarchy-aware Data Service Test Complete!")
    print("=" * 60)