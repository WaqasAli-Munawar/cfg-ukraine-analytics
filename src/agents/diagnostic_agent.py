"""
Diagnostic Agent - "Why did it happen?"
Handles queries about root causes and variance analysis
Uses RAG Retriever for semantic-enhanced retrieval
"""
from typing import Dict, Any
import plotly.graph_objects as go

from src.models.query import QueryClassification
from src.services.rag_retriever import RAGRetriever
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DiagnosticAgent:
    """
    Handles "Why did it happen?" queries.
    Performs variance analysis and identifies contributing factors.
    """
    
    def __init__(self):
        self.retriever = RAGRetriever()
        logger.info("Diagnostic Agent initialized with RAG")
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve variance analysis using RAG.
        """
        logger.info(
            "Diagnostic retrieval with RAG",
            metrics=classification.metrics,
        )
        
        # Get data from RAG retriever
        result = self.retriever.retrieve_for_diagnostic(classification)
        
        # Generate waterfall chart
        chart_json = self._create_waterfall_chart(result)
        result['chart'] = chart_json
        
        return result
    
    def _create_waterfall_chart(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create waterfall chart for variance analysis.
        """
        variance = data.get('variance', {})
        
        # Build waterfall data
        labels = [f"Previous ({variance.get('previous_period', 'N/A')})"]
        values = [variance.get('previous_value', 0)]
        measures = ['absolute']
        
        # Add factors
        for factor in variance.get('factors', []):
            impact = variance.get('previous_value', 0) * (factor['impact_pct'] / 100)
            labels.append(factor['factor'])
            values.append(impact)
            measures.append('relative')
        
        # Add current as total
        labels.append(f"Current ({variance.get('period', 'N/A')})")
        values.append(variance.get('current_value', 0))
        measures.append('total')
        
        # Create waterfall chart
        fig = go.Figure(go.Waterfall(
            name="Variance",
            orientation="v",
            measure=measures,
            x=labels,
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            increasing={"marker": {"color": "#2E86AB"}},
            decreasing={"marker": {"color": "#E94F37"}},
            totals={"marker": {"color": "#F18F01"}},
        ))
        
        fig.update_layout(
            title=f"CFG Ukraine - Variance Analysis ({variance.get('period', 'N/A')} {variance.get('comparison', 'MoM')})",
            showlegend=False,
            template='plotly_white',
            height=500,
            width=800,
        )
        
        return fig.to_dict()
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format variance analysis into natural language.
        """
        variance = data.get('variance', {})
        
        response_parts = []
        
        # Introduction
        response_parts.append(f"🔍 **Variance Analysis for CFG Ukraine**")
        response_parts.append("")
        
        # Variance summary
        current = variance.get('current_value', 0)
        previous = variance.get('previous_value', 0)
        var_pct = variance.get('variance_pct', 0)
        var_amount = variance.get('variance', 0)
        
        direction = "increased" if var_amount > 0 else "decreased"
        emoji = "📈" if var_amount > 0 else "📉"
        
        response_parts.append(f"**Period Comparison ({variance.get('comparison', 'MoM')}):**")
        response_parts.append(f"   • Current ({variance.get('period', 'N/A')}): SAR {current:,.0f}")
        response_parts.append(f"   • Previous ({variance.get('previous_period', 'N/A')}): SAR {previous:,.0f}")
        response_parts.append(f"   • Change: {emoji} SAR {var_amount:+,.0f} ({var_pct:+.2f}%)")
        response_parts.append("")
        
        # Contributing factors
        factors = variance.get('factors', [])
        if factors:
            response_parts.append("**Contributing Factors:**")
            for factor in factors:
                impact = factor['impact_pct']
                factor_emoji = "🔺" if impact > 0 else "🔻" if impact < 0 else "➡️"
                response_parts.append(f"   {factor_emoji} {factor['factor']}: {impact:+.2f}% impact")
            response_parts.append("")
        
        # Interpretation
        response_parts.append("**Interpretation:**")
        if abs(var_pct) > 10:
            response_parts.append(f"   This is a **significant** {direction} ({abs(var_pct):.1f}%).")
        elif abs(var_pct) > 5:
            response_parts.append(f"   This is a **moderate** {direction} ({abs(var_pct):.1f}%).")
        else:
            response_parts.append(f"   This is a **minor** change ({abs(var_pct):.1f}%).")
        response_parts.append("")
        
        # Relevant accounts from semantic search
        relevant_accounts = data.get('relevant_accounts', [])
        if relevant_accounts:
            response_parts.append("🔍 **Related Accounts (Semantic Search):**")
            for acc in relevant_accounts[:3]:
                response_parts.append(f"   • {acc['account']} (relevance: {acc['score']:.0%})")
            response_parts.append("")
        
        response_parts.append("📊 Waterfall chart included showing variance breakdown.")
        response_parts.append("🔗 Data source: Microsoft Fabric OneLake + RAG")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("🔍 Diagnostic Agent Test - With RAG")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = DiagnosticAgent()
    
    test_queries = [
        "Why did revenue change in Q3?",
        "Explain the variance in September",
        "What caused the amount change?",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        
        data = agent.retrieve(classification)
        print(f"\n✅ Variance: {data['variance']['variance_pct']:+.2f}%")
        print(f"✅ Relevant accounts: {len(data.get('relevant_accounts', []))}")
        
        response = agent.format_response(data, classification)
        print(f"\n📝 Response:\n{response}")
    
    print("\n" + "=" * 60)
    print("✅ Diagnostic Agent with RAG Complete!")
    print("=" * 60)