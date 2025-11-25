"""
Descriptive Agent - "What happened?"
Handles queries about historical data and trends
NOW WITH INTEGRATED CHART GENERATION
"""
from typing import Dict, Any, List
import pandas as pd

from src.models.query import QueryClassification
from src.services.mock_data_service import MockDataService
from src.utils.visualizer import FinancialVisualizer
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DescriptiveAgent:
    """
    Handles "What happened?" queries.
    Retrieves and presents historical financial/operational data.
    NOW RETURNS BOTH TEXT AND CHART JSON.
    """
    
    def __init__(self):
        self.data_service = MockDataService()
        self.visualizer = FinancialVisualizer()
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve data for descriptive queries.
        
        Args:
            classification: Classified query with extracted entities
        
        Returns:
            Dictionary with retrieved data, text response, AND chart JSON
        """
        logger.info(
            "Descriptive retrieval",
            metrics=classification.metrics,
            temporal=classification.temporal.model_dump(),
        )
        
        # Determine which metrics to retrieve
        metrics = classification.metrics if classification.metrics else ['revenue', 'ebitda']
        
        # Get financial summary data
        df = self.data_service.get_financial_summary(
            start_period=classification.temporal.start_period,
            end_period=classification.temporal.end_period,
        )
        
        # Map common metric aliases
        metric_mapping = {
            'ebitda': 'ebitda',
            'revenue': 'revenue',
            'gross_margin': 'gross_margin_pct',
            'gross_profit': 'gross_profit',
            'net_income': 'net_income',
            'operating_expenses': 'opex',
            'opex': 'opex',
        }
        
        # Map metrics to actual column names
        mapped_metrics = []
        for m in metrics:
            m_lower = m.lower()
            if m_lower in df.columns:
                mapped_metrics.append(m_lower)
            elif m_lower in metric_mapping and metric_mapping[m_lower] in df.columns:
                mapped_metrics.append(metric_mapping[m_lower])
        
        if not mapped_metrics:
            # Default to key metrics
            mapped_metrics = ['revenue', 'ebitda']
        
        # Select relevant columns
        columns_to_include = ['period', 'fiscal_year', 'fiscal_quarter'] + mapped_metrics
        df_result = df[columns_to_include]
        
        # Calculate summary statistics
        summary = {}
        for col in mapped_metrics:
            summary[col] = {
                'min': float(df_result[col].min()),
                'max': float(df_result[col].max()),
                'mean': float(df_result[col].mean()),
                'latest': float(df_result[col].iloc[-1]),
            }
        
        # Calculate trends
        trends = {}
        for col in mapped_metrics:
            values = df_result[col].values
            if len(values) >= 2:
                growth = ((values[-1] / values[0]) - 1) * 100 if values[0] != 0 else 0
                trends[col] = {
                    'direction': 'increasing' if growth > 5 else 'decreasing' if growth < -5 else 'stable',
                    'growth_pct': round(growth, 2),
                }
        
        # Generate chart JSON
        chart_json = self._create_chart(df_result, mapped_metrics, classification)
        
        return {
            'data': df_result.to_dict('records'),
            'summary': summary,
            'trends': trends,
            'row_count': len(df_result),
            'chart': chart_json,  # NEW: Include chart JSON
            'source': 'mock_data_service',
        }
    
    def _create_chart(
        self,
        df: pd.DataFrame,
        metrics: List[str],
        classification: QueryClassification
    ) -> Dict[str, Any]:
        """
        Create appropriate chart based on data and query context.
        
        Returns:
            Plotly figure as JSON dict
        """
        # Decide chart type based on data characteristics
        num_periods = len(df)
        num_metrics = len(metrics)
        
        if num_periods >= 4 and num_metrics >= 1:
            # Trend over time - use line chart
            chart_type = 'line'
            title = f"CFG Ukraine - {', '.join([m.replace('_', ' ').title() for m in metrics])} Trend"
            chart_json = self.visualizer.create_trend_chart_json(
                df,
                metrics=metrics,
                title=title
            )
        elif num_periods <= 4 and num_metrics == 1:
            # Few periods, single metric - use bar chart
            chart_type = 'bar'
            title = f"CFG Ukraine - {metrics[0].replace('_', ' ').title()} Comparison"
            chart_json = self.visualizer.create_comparison_chart_json(
                df,
                metric=metrics[0],
                title=title
            )
        elif num_metrics == 2 and any(m.endswith('_pct') or m.endswith('_margin') for m in metrics):
            # Two metrics with different scales - use dual axis
            chart_type = 'dual_axis'
            title = f"CFG Ukraine - {metrics[0].replace('_', ' ').title()} vs {metrics[1].replace('_', ' ').title()}"
            chart_json = self.visualizer.create_dual_axis_chart_json(
                df,
                metric1=metrics[0],
                metric2=metrics[1],
                title=title
            )
        else:
            # Default to line chart
            chart_type = 'line'
            title = f"CFG Ukraine - Financial Metrics"
            chart_json = self.visualizer.create_trend_chart_json(
                df,
                metrics=metrics[:3],  # Limit to 3 metrics for clarity
                title=title
            )
        
        logger.info(f"Created {chart_type} chart with {len(metrics)} metrics")
        
        return chart_json
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format retrieved data into natural language response.
        """
        df = pd.DataFrame(data['data'])
        summary = data['summary']
        trends = data['trends']
        
        # Build response
        response_parts = []
        
        # Introduction
        metrics_str = ', '.join(classification.metrics) if classification.metrics else 'key metrics'
        period_str = f"from {df['period'].iloc[0]} to {df['period'].iloc[-1]}" if len(df) > 1 else f"for {df['period'].iloc[0]}"
        
        response_parts.append(f"Here's the {metrics_str} data for CFG Ukraine {period_str}:")
        
        # Latest values
        response_parts.append("\n📊 Latest Period:")
        latest_period = df['period'].iloc[-1]
        for col in df.columns:
            if col not in ['period', 'fiscal_year', 'fiscal_quarter']:
                latest_val = df[col].iloc[-1]
                if col.endswith('_pct'):
                    response_parts.append(f"   • {col.replace('_', ' ').title()}: {latest_val:.2f}%")
                else:
                    response_parts.append(f"   • {col.replace('_', ' ').title()}: ${latest_val:,.0f}")
        
        # Trends
        if trends:
            response_parts.append("\n📈 Trends:")
            for metric, trend_data in trends.items():
                direction_emoji = "📈" if trend_data['direction'] == 'increasing' else "📉" if trend_data['direction'] == 'decreasing' else "➡️"
                response_parts.append(
                    f"   {direction_emoji} {metric.replace('_', ' ').title()}: "
                    f"{trend_data['direction']} ({trend_data['growth_pct']:+.1f}% over period)"
                )
        
        # Summary stats
        response_parts.append("\n📋 Summary:")
        for metric, stats in summary.items():
            if metric.endswith('_pct'):
                response_parts.append(f"   • {metric.replace('_', ' ').title()}: Min {stats['min']:.2f}%, Max {stats['max']:.2f}%, Avg {stats['mean']:.2f}%")
            else:
                response_parts.append(f"   • {metric.replace('_', ' ').title()}: Min ${stats['min']:,.0f}, Max ${stats['max']:,.0f}, Avg ${stats['mean']:,.0f}")
        
        # NEW: Mention chart availability
        response_parts.append("\n📊 Visual chart included in response.")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("📊 Descriptive Agent Test - WITH CHARTS")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = DescriptiveAgent()
    
    # Test queries
    test_queries = [
        "Show me CFG Ukraine EBITDA trend for the last 4 years",
        "What was the revenue in 2024?",
        "Display gross margin for the last 8 quarters",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        # Classify
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        print(f"Metrics: {classification.metrics}")
        
        # Retrieve (includes chart JSON now!)
        data = agent.retrieve(classification)
        print(f"\n✅ Retrieved {data['row_count']} records")
        print(f"✅ Chart included: {data['chart']['data'][0]['type']} chart")
        print(f"✅ Chart has {len(data['chart']['data'])} traces")
        
        # Format text response
        response = agent.format_response(data, classification)
        print(f"\n📝 Text Response:\n{response}")
        
        # Verify chart JSON is serializable
        try:
            chart_json_str = json.dumps(data['chart'])
            print(f"\n✅ Chart JSON serializable: {len(chart_json_str)} characters")
        except Exception as e:
            print(f"\n❌ Chart JSON error: {e}")
    
    print("\n" + "=" * 60)
    print("✅ Descriptive Agent now returns TEXT + CHART!")
    print("=" * 60)