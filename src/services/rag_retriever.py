"""
RAG Retriever Service for CFG Ukraine
Combines OneLake structured data with Qdrant semantic search
Uses hierarchical account lookup for accurate metric filtering
"""
from typing import Dict, Any, List, Optional
import pandas as pd

from src.services.onelake_data_service import OneLakeDataService
from src.services.embedding_service import EmbeddingService
from src.models.query import QueryClassification
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RAGRetriever:
    """
    Intelligent retriever that combines:
    1. Semantic search (Qdrant) - for understanding intent
    2. Structured data (OneLake) - for actual financial data
    3. Hierarchical account lookup - for accurate metric filtering
    """
    
    def __init__(self):
        self.data_service = OneLakeDataService()
        self.embedding_service = EmbeddingService()
        logger.info("RAG Retriever initialized")
    
    def find_relevant_accounts(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find accounts semantically related to the query."""
        try:
            results = self.embedding_service.search_accounts(query, limit=limit)
            logger.info(f"Found {len(results)} relevant accounts for: '{query}'")
            return results
        except Exception as e:
            logger.error(f"Account search failed: {e}")
            return []
    
    def find_relevant_entities(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find entities semantically related to the query."""
        try:
            results = self.embedding_service.search_entities(query, limit=limit)
            logger.info(f"Found {len(results)} relevant entities for: '{query}'")
            return results
        except Exception as e:
            logger.error(f"Entity search failed: {e}")
            return []
    
    def find_relevant_departments(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Find departments semantically related to the query."""
        try:
            results = self.embedding_service.search_departments(query, limit=limit)
            logger.info(f"Found {len(results)} relevant departments for: '{query}'")
            return results
        except Exception as e:
            logger.error(f"Department search failed: {e}")
            return []
    
    def retrieve_for_descriptive(
        self,
        classification: QueryClassification,
    ) -> Dict[str, Any]:
        """
        Retrieve data for descriptive queries ("What happened?").
        Uses hierarchical account lookup for accurate metric filtering.
        """
        # Determine year from temporal info
        year = "FY24"
        if classification.temporal and classification.temporal.start_period:
            if "2023" in str(classification.temporal.start_period):
                year = "FY23"
            elif "2024" in str(classification.temporal.start_period):
                year = "FY24"
        
        # Build semantic query from metrics
        semantic_query = " ".join(classification.metrics) if classification.metrics else "financial performance"
        
        # Semantic search for relevant accounts (for display)
        relevant_accounts = self.find_relevant_accounts(semantic_query, limit=10)
        relevant_entities = self.find_relevant_entities(semantic_query, limit=3)
        
        # Use hierarchy-aware metric data retrieval
        if classification.metrics and len(classification.metrics) > 0:
            metric = classification.metrics[0]  # Primary metric
            logger.info(f"Retrieving data for metric: {metric}")
            
            metric_data = self.data_service.get_metric_data(
                metric=metric,
                year=year,
            )
            
            if metric_data['has_data']:
                return {
                    'data': metric_data['data'],
                    'summary': metric_data['summary'],
                    'trend': metric_data['trend'],
                    'relevant_accounts': relevant_accounts,
                    'relevant_entities': relevant_entities,
                    'year': year,
                    'metric': metric,
                    'account_count': metric_data.get('account_count', 0),
                    'semantic_query': semantic_query,
                    'row_count': len(metric_data['data']),
                    'source': 'onelake_with_hierarchy',
                    'metric_filtered': True,
                }
            else:
                logger.warning(f"No data found for metric '{metric}', using general summary")
        
        # Fallback: Get general financial summary
        logger.info("Using general financial summary (no specific metric)")
        financial_summary = self.data_service.get_financial_summary(year=year)
        
        # Calculate statistics
        amounts = financial_summary['Amount'].values
        summary_stats = {
            'total': float(amounts.sum()),
            'average': float(amounts.mean()),
            'min': float(amounts.min()),
            'max': float(amounts.max()),
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
            'data': financial_summary.to_dict('records'),
            'summary': summary_stats,
            'trend': trend,
            'relevant_accounts': relevant_accounts,
            'relevant_entities': relevant_entities,
            'year': year,
            'semantic_query': semantic_query,
            'row_count': len(financial_summary),
            'source': 'onelake_general',
            'metric_filtered': False,
        }
    
    def retrieve_for_diagnostic(
        self,
        classification: QueryClassification,
    ) -> Dict[str, Any]:
        """
        Retrieve data for diagnostic queries ("Why did it happen?").
        """
        # Determine period
        period = "Sep"  # Default
        period_map = {
            'Q1': 'Mar', 'Q2': 'Jun', 'Q3': 'Sep', 'Q4': 'Dec',
            'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
            'april': 'Apr', 'may': 'May', 'june': 'Jun',
            'july': 'Jul', 'august': 'Aug', 'september': 'Sep',
            'october': 'Oct', 'november': 'Nov', 'december': 'Dec',
            'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar',
            'apr': 'Apr', 'jun': 'Jun', 'jul': 'Jul',
            'aug': 'Aug', 'sep': 'Sep', 'oct': 'Oct',
            'nov': 'Nov', 'dec': 'Dec',
        }
        
        if classification.temporal and classification.temporal.end_period:
            period_str = str(classification.temporal.end_period).lower()
            for key, val in period_map.items():
                if key in period_str:
                    period = val
                    break
        
        # Get metric for filtering
        metric = classification.metrics[0] if classification.metrics else "total"
        
        # Build semantic query
        query_parts = classification.metrics if classification.metrics else ['financial', 'variance']
        semantic_query = " ".join(query_parts)
        
        # Semantic search for context
        relevant_accounts = self.find_relevant_accounts(semantic_query, limit=5)
        
        # Get variance analysis using hierarchy-aware filtering
        variance = self.data_service.get_variance_analysis(
            metric=metric,
            period=period,
            comparison=classification.comparison_type or "MoM",
            year="FY24",
        )
        
        return {
            'variance': variance,
            'relevant_accounts': relevant_accounts,
            'period': period,
            'metric': metric,
            'semantic_query': semantic_query,
            'source': 'onelake_with_hierarchy',
        }
    
    def retrieve_for_predictive(
        self,
        classification: QueryClassification,
    ) -> Dict[str, Any]:
        """
        Retrieve data for predictive queries ("What will happen?").
        """
        # Get metric
        metric = classification.metrics[0] if classification.metrics else None
        
        # Get historical data - filtered by metric if specified
        if metric:
            metric_data = self.data_service.get_metric_data(metric=metric, year="FY24")
            if metric_data['has_data']:
                amounts = [r['Amount'] for r in metric_data['data']]
                periods_data = metric_data['data']
            else:
                financial_summary = self.data_service.get_financial_summary(year="FY24")
                amounts = financial_summary['Amount'].values.tolist()
                periods_data = financial_summary.to_dict('records')
        else:
            financial_summary = self.data_service.get_financial_summary(year="FY24")
            amounts = financial_summary['Amount'].values.tolist()
            periods_data = financial_summary.to_dict('records')
        
        # Calculate projections
        projections = []
        if len(amounts) >= 3:
            avg_growth = (amounts[-1] - amounts[0]) / len(amounts)
            last_value = amounts[-1]
            periods = ['Jan', 'Feb', 'Mar']  # Next FY
            
            for i, period in enumerate(periods):
                projected = last_value + (avg_growth * (i + 1))
                projections.append({
                    'period': period,
                    'year': 'FY25',
                    'projected_amount': float(projected),
                    'confidence': 0.85 - (i * 0.1),
                })
        
        # Semantic search for context
        query_parts = classification.metrics if classification.metrics else ['forecast', 'projection']
        semantic_query = " ".join(query_parts)
        relevant_accounts = self.find_relevant_accounts(semantic_query, limit=5)
        
        return {
            'historical_data': periods_data,
            'projections': projections,
            'relevant_accounts': relevant_accounts,
            'metric': metric,
            'methodology': 'linear_trend_projection',
            'source': 'onelake_with_hierarchy',
        }
    
    def retrieve_for_prescriptive(
        self,
        classification: QueryClassification,
    ) -> Dict[str, Any]:
        """
        Retrieve data for prescriptive queries ("What should we do?").
        """
        # Get metric
        metric = classification.metrics[0] if classification.metrics else None
        metric_name = metric if metric else "financial performance"
        
        # Get data - filtered by metric if specified
        if metric:
            metric_data = self.data_service.get_metric_data(metric=metric, year="FY24")
            if metric_data['has_data']:
                amounts = [r['Amount'] for r in metric_data['data']]
                financial_data = metric_data['data']
            else:
                financial_summary = self.data_service.get_financial_summary(year="FY24")
                amounts = financial_summary['Amount'].values.tolist()
                financial_data = financial_summary.to_dict('records')
        else:
            financial_summary = self.data_service.get_financial_summary(year="FY24")
            amounts = financial_summary['Amount'].values.tolist()
            financial_data = financial_summary.to_dict('records')
        
        # Get variance for latest period
        variance = self.data_service.get_variance_analysis(
            metric=metric if metric else "total",
            period="Dec",
            comparison="MoM",
            year="FY24",
        )
        
        # Generate recommendations based on analysis
        recommendations = []
        
        # Trend-based recommendation
        if len(amounts) >= 2:
            growth = ((amounts[-1] / amounts[0]) - 1) * 100 if amounts[0] != 0 else 0
            
            if growth > 10:
                recommendations.append({
                    'category': 'Growth Management',
                    'priority': 'Medium',
                    'recommendation': f'Strong {metric_name} growth trajectory. Consider capacity planning and resource allocation.',
                    'rationale': f'YTD growth of {growth:.1f}% indicates expansion.',
                })
            elif growth < -5:
                recommendations.append({
                    'category': 'Performance Improvement',
                    'priority': 'High',
                    'recommendation': f'Declining {metric_name} trend detected. Conduct root cause analysis and implement corrective measures.',
                    'rationale': f'YTD decline of {growth:.1f}% requires attention.',
                })
            else:
                recommendations.append({
                    'category': 'Optimization',
                    'priority': 'Low',
                    'recommendation': f'Stable {metric_name} performance. Focus on efficiency improvements and cost optimization.',
                    'rationale': f'YTD change of {growth:.1f}% shows stability.',
                })
        
        # Variance-based recommendation
        if variance['variance_pct'] > 5:
            recommendations.append({
                'category': 'Positive Variance',
                'priority': 'Medium',
                'recommendation': f'Analyze drivers of positive {metric_name} variance and replicate successful strategies.',
                'rationale': f"Recent {variance['variance_pct']:.1f}% increase in {variance['period']}.",
            })
        elif variance['variance_pct'] < -5:
            recommendations.append({
                'category': 'Negative Variance',
                'priority': 'High',
                'recommendation': f'Investigate causes of {metric_name} decline and implement immediate corrective actions.',
                'rationale': f"Recent {variance['variance_pct']:.1f}% decrease in {variance['period']}.",
            })
        
        # Semantic search for context
        query_parts = classification.metrics if classification.metrics else ['recommendation', 'action']
        semantic_query = " ".join(query_parts)
        relevant_accounts = self.find_relevant_accounts(semantic_query, limit=5)
        
        return {
            'financial_summary': financial_data,
            'variance': variance,
            'recommendations': recommendations,
            'relevant_accounts': relevant_accounts,
            'metric': metric,
            'source': 'onelake_with_hierarchy',
        }


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    
    print("=" * 60)
    print("📊 RAG Retriever Test - With Hierarchy")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    retriever = RAGRetriever()
    
    # Test queries with specific metrics
    test_queries = [
        ("Show me the EBITDA trend for FY24", "descriptive"),
        ("What was the revenue in FY24?", "descriptive"),
        ("Show me gross profit trend", "descriptive"),
        ("Why did operating income decrease in Q3?", "diagnostic"),
        ("Forecast revenue for next quarter", "predictive"),
        ("How can we improve gross margin?", "prescriptive"),
    ]
    
    for query, expected_type in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        # Classify
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        print(f"Metrics: {classification.metrics}")
        
        # Retrieve
        if classification.category.value == "descriptive":
            result = retriever.retrieve_for_descriptive(classification)
            print(f"\n📊 Descriptive Results:")
            print(f"   Metric filtered: {result.get('metric_filtered', False)}")
            print(f"   Metric: {result.get('metric', 'N/A')}")
            print(f"   Account count: {result.get('account_count', 'N/A')}")
            print(f"   Data rows: {result['row_count']}")
            if result.get('trend'):
                print(f"   Trend: {result['trend']['direction']} ({result['trend']['growth_pct']:+.1f}%)")
            if result.get('summary'):
                print(f"   Total: SAR {result['summary']['total']:,.0f}")
        
        elif classification.category.value == "diagnostic":
            result = retriever.retrieve_for_diagnostic(classification)
            print(f"\n🔍 Diagnostic Results:")
            print(f"   Metric: {result.get('metric', 'N/A')}")
            print(f"   Period: {result['period']}")
            print(f"   Variance: {result['variance']['variance_pct']:+.2f}%")
    
    print("\n" + "=" * 60)
    print("✅ Hierarchy-aware RAG Test Complete!")
    print("=" * 60)