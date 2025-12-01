"""
Prescriptive Agent - "What should we do?"
Handles queries about recommendations and actions
"""
from typing import Dict, Any, List
import pandas as pd
import plotly.graph_objects as go

from src.models.query import QueryClassification
from src.services.rag_retriever import RAGRetriever
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PrescriptiveAgent:
    """
    Handles "What should we do?" queries.
    Analyzes data and provides actionable recommendations.
    
    Capabilities:
    - Performance-based recommendations
    - Variance-driven actions
    - Priority-ranked suggestions
    """
    
    def __init__(self):
        self.retriever = RAGRetriever()
        logger.info("Prescriptive Agent initialized")
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve data and generate recommendations.
        """
        logger.info(
            "Prescriptive retrieval",
            metrics=classification.metrics,
        )
        
        # Get data from RAG retriever
        result = self.retriever.retrieve_for_prescriptive(classification)
        
        # Generate chart
        chart_json = self._create_recommendation_chart(result)
        result['chart'] = chart_json
        
        return result
    
    def _create_recommendation_chart(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a chart showing performance and recommendations.
        """
        fig = go.Figure()
        
        # Financial trend
        financial_data = data.get('financial_summary', [])
        if financial_data:
            periods = [d['Period'] for d in financial_data]
            amounts = [d['Amount'] for d in financial_data]
            
            # Calculate average for reference line
            avg_amount = sum(amounts) / len(amounts) if amounts else 0
            
            fig.add_trace(go.Bar(
                x=periods,
                y=amounts,
                name='Monthly Amount',
                marker_color=['#2E86AB' if amt >= avg_amount else '#E94F37' for amt in amounts],
                hovertemplate='<b>%{x}</b><br>Amount: SAR %{y:,.0f}<extra></extra>',
            ))
            
            # Average line
            fig.add_hline(
                y=avg_amount,
                line_dash="dash",
                line_color="gray",
                annotation_text=f"Average: {avg_amount:,.0f}",
                annotation_position="right",
            )
        
        # Recommendations as annotations
        recommendations = data.get('recommendations', [])
        priority_colors = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}
        
        fig.update_layout(
            title='CFG Ukraine - Performance Analysis & Recommendations',
            xaxis_title='Period',
            yaxis_title='Amount (SAR)',
            template='plotly_white',
            height=500,
            width=900,
            showlegend=True,
            hovermode='x unified',
        )
        
        return fig.to_dict()
    
    def _prioritize_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort recommendations by priority.
        """
        priority_order = {'High': 0, 'Medium': 1, 'Low': 2}
        return sorted(recommendations, key=lambda x: priority_order.get(x['priority'], 3))
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format recommendations into natural language.
        """
        response_parts = []
        
        # Introduction
        response_parts.append("💡 **Strategic Recommendations for CFG Ukraine**")
        response_parts.append("")
        
        # Current situation summary
        variance = data.get('variance', {})
        if variance:
            current = variance.get('current_value', 0)
            previous = variance.get('previous_value', 0)
            var_pct = variance.get('variance_pct', 0)
            
            response_parts.append("📊 **Current Situation:**")
            response_parts.append(f"   • Latest Period ({variance.get('period', 'N/A')}): SAR {current:,.0f}")
            response_parts.append(f"   • Previous Period ({variance.get('previous_period', 'N/A')}): SAR {previous:,.0f}")
            
            if var_pct > 0:
                response_parts.append(f"   • Change: 📈 +{var_pct:.1f}% (Positive trend)")
            elif var_pct < 0:
                response_parts.append(f"   • Change: 📉 {var_pct:.1f}% (Needs attention)")
            else:
                response_parts.append(f"   • Change: ➡️ {var_pct:.1f}% (Stable)")
            response_parts.append("")
        
        # Recommendations
        recommendations = data.get('recommendations', [])
        if recommendations:
            # Sort by priority
            sorted_recs = self._prioritize_recommendations(recommendations)
            
            response_parts.append("⭐ **Recommendations:**")
            response_parts.append("")
            
            priority_emoji = {'High': '🔴', 'Medium': '🟡', 'Low': '🟢'}
            
            for i, rec in enumerate(sorted_recs, 1):
                emoji = priority_emoji.get(rec['priority'], '⚪')
                response_parts.append(f"**{i}. {rec['category']}** {emoji} {rec['priority']} Priority")
                response_parts.append(f"   📌 {rec['recommendation']}")
                response_parts.append(f"   📋 Rationale: {rec['rationale']}")
                response_parts.append("")
        else:
            response_parts.append("✅ No immediate actions required. Performance is within expected range.")
            response_parts.append("")
        
        # Action items summary
        high_priority = [r for r in recommendations if r['priority'] == 'High']
        if high_priority:
            response_parts.append("🚨 **Immediate Actions Required:**")
            for rec in high_priority:
                response_parts.append(f"   ➤ {rec['recommendation']}")
            response_parts.append("")
        
        # Relevant accounts from semantic search
        relevant_accounts = data.get('relevant_accounts', [])
        if relevant_accounts:
            response_parts.append("🔍 **Related Accounts to Monitor:**")
            for acc in relevant_accounts[:3]:
                response_parts.append(f"   • {acc['account']} (relevance: {acc['score']:.0%})")
            response_parts.append("")
        
        response_parts.append("📊 Performance chart included in response.")
        response_parts.append("🔗 Data source: Microsoft Fabric OneLake + RAG")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("💡 Prescriptive Agent Test - CFG Ukraine")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = PrescriptiveAgent()
    
    # Test queries
    test_queries = [
        "What should we do to improve our financial performance?",
        "Give me recommendations for next quarter",
        "What actions should we take based on recent trends?",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        # Classify
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        
        # Retrieve
        data = agent.retrieve(classification)
        print(f"\n✅ Recommendations generated: {len(data.get('recommendations', []))}")
        print(f"✅ Financial data points: {len(data.get('financial_summary', []))}")
        print(f"✅ Relevant accounts: {len(data.get('relevant_accounts', []))}")
        
        # Format response
        response = agent.format_response(data, classification)
        print(f"\n📝 Response:\n{response}")
        
        # Verify chart
        if 'chart' in data:
            chart_str = json.dumps(data['chart'])
            print(f"\n✅ Chart JSON: {len(chart_str)} characters")
    
    print("\n" + "=" * 60)
    print("✅ Prescriptive Agent Test Complete!")
    print("=" * 60)