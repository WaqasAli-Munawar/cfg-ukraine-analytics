"""
Diagnostic Agent - "Why did it happen?"
Handles queries about root causes and variance analysis
NOW WITH INTEGRATED WATERFALL CHART GENERATION
"""
from typing import Dict, Any
import pandas as pd

from src.models.query import QueryClassification
from src.services.mock_data_service import MockDataService
from src.utils.visualizer import FinancialVisualizer
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DiagnosticAgent:
    """
    Handles "Why did it happen?" queries.
    Performs variance analysis and identifies contributing factors.
    NOW RETURNS BOTH TEXT AND WATERFALL CHART JSON.
    """
    
    def __init__(self):
        self.data_service = MockDataService()
        self.visualizer = FinancialVisualizer()
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve variance analysis for diagnostic queries.
        
        Args:
            classification: Classified query with extracted entities
        
        Returns:
            Dictionary with variance data, text response, AND waterfall chart JSON
        """
        logger.info(
            "Diagnostic retrieval",
            metrics=classification.metrics,
            temporal=classification.temporal.model_dump(),
        )
        
        # Determine metric to analyze
        metric = classification.metrics[0] if classification.metrics else 'revenue'
        
        # Convert to lowercase
        metric = metric.lower()
        
        # Map metric aliases to actual column names
        metric_mapping = {
            'ebitda': 'ebitda',
            'revenue': 'revenue',
            'gross_margin': 'gross_margin_pct',
            'gross_profit': 'gross_profit',
            'net_income': 'net_income',
            'opex': 'opex',
        }
        
        # Use mapped metric or original if not in mapping
        mapped_metric = metric_mapping.get(metric, metric)
        
        # Determine period
        period = classification.temporal.end_period or '2024-Q3'
        
        # Determine comparison type
        comparison = classification.comparison_type or 'QoQ'
        
        # Get variance analysis (use original metric for service)
        variance = self.data_service.get_variance_analysis(metric, period, comparison)
        
        # Get context data (use mapped_metric for DataFrame)
        df = self.data_service.get_financial_summary()
        
        # Generate waterfall chart JSON
        chart_json = self.visualizer.create_waterfall_chart_json(
            variance,
            title=f"{metric.replace('_', ' ').title()} Variance Analysis"
        )
        
        return {
            'variance': variance,
            'historical_context': df[['period', mapped_metric]].tail(8).to_dict('records'),
            'chart': chart_json,  # NEW: Include waterfall chart JSON
            'source': 'mock_data_service',
        }
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format variance analysis into natural language response.
        """
        variance = data['variance']
        
        response_parts = []
        
        # Introduction
        metric_name = variance['metric'].replace('_', ' ').title()
        response_parts.append(f"Analysis of {metric_name} variance for {variance['period']}:")
        
        # Variance summary
        current_val = variance['current_value']
        previous_val = variance['previous_value']
        variance_val = variance['variance']
        variance_pct = variance['variance_pct']
        
        direction = "increased" if variance_val > 0 else "decreased"
        emoji = "📈" if variance_val > 0 else "📉"
        
        response_parts.append(f"\n{emoji} {metric_name} {direction}:")
        
        if variance['metric'].endswith('_pct'):
            response_parts.append(f"   • Current: {current_val:.2f}%")
            response_parts.append(f"   • Previous ({variance['comparison']}): {previous_val:.2f}%")
            response_parts.append(f"   • Change: {variance_val:+.2f} percentage points")
        else:
            response_parts.append(f"   • Current: ${current_val:,.0f}")
            response_parts.append(f"   • Previous ({variance['comparison']}): ${previous_val:,.0f}")
            response_parts.append(f"   • Change: ${variance_val:+,.0f} ({variance_pct:+.1f}%)")
        
        # Contributing factors
        response_parts.append("\n🔍 Contributing Factors:")
        for factor in variance['factors']:
            impact = factor['impact_pct']
            impact_emoji = "🔺" if impact > 0 else "🔻" if impact < 0 else "➡️"
            response_parts.append(f"   {impact_emoji} {factor['factor']}: {impact:+.1f}% impact")
        
        # Interpretation
        response_parts.append("\n💡 Interpretation:")
        if abs(variance_pct) > 10:
            response_parts.append(f"   This is a significant {direction.lower()} ({abs(variance_pct):.1f}%).")
        elif abs(variance_pct) > 5:
            response_parts.append(f"   This is a moderate {direction.lower()} ({abs(variance_pct):.1f}%).")
        else:
            response_parts.append(f"   This is a minor {direction.lower()} ({abs(variance_pct):.1f}%).")
        
        if variance_val > 0:
            response_parts.append("   Positive variance suggests improved performance or favorable market conditions.")
        else:
            response_parts.append("   Negative variance may indicate challenges or unfavorable market conditions.")
        
        # NEW: Mention chart availability
        response_parts.append("\n📊 Waterfall chart included showing variance breakdown.")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("🔍 Diagnostic Agent Test - WITH WATERFALL CHARTS")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = DiagnosticAgent()
    
    # Test queries
    test_queries = [
        "Why did revenue drop in Q3 2024?",
        "Explain the EBITDA variance in 2024-Q2",
        "What caused the gross margin decrease?",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        # Classify
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        print(f"Metrics: {classification.metrics}")
        
        # Retrieve (includes waterfall chart JSON now!)
        data = agent.retrieve(classification)
        print(f"\n✅ Variance analysis retrieved")
        print(f"✅ Chart included: {data['chart']['data'][0]['type']} chart")
        print(f"✅ Chart has {len(data['chart']['data'][0]['x'])} bars")
        
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
    print("✅ Diagnostic Agent now returns TEXT + WATERFALL CHART!")
    print("=" * 60)