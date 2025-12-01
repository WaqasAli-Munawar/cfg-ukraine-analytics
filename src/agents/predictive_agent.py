"""
Predictive Agent - "What will happen?"
Handles queries about future projections and forecasts
"""
from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go

from src.models.query import QueryClassification
from src.services.rag_retriever import RAGRetriever
from src.utils.logger import get_logger

logger = get_logger(__name__)


class PredictiveAgent:
    """
    Handles "What will happen?" queries.
    Uses historical data to project future trends.
    
    Capabilities:
    - Linear trend projection
    - Seasonal pattern detection
    - Confidence intervals
    """
    
    def __init__(self):
        self.retriever = RAGRetriever()
        logger.info("Predictive Agent initialized")
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve data and generate predictions.
        """
        logger.info(
            "Predictive retrieval",
            metrics=classification.metrics,
        )
        
        # Get data from RAG retriever
        result = self.retriever.retrieve_for_predictive(classification)
        
        # Generate chart
        chart_json = self._create_forecast_chart(result)
        result['chart'] = chart_json
        
        return result
    
    def _create_forecast_chart(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a chart showing historical data and projections.
        """
        fig = go.Figure()
        
        # Historical data
        historical = data.get('historical_data', [])
        if historical:
            periods = [d['Period'] for d in historical]
            amounts = [d['Amount'] for d in historical]
            
            fig.add_trace(go.Scatter(
                x=periods,
                y=amounts,
                mode='lines+markers',
                name='Historical (FY24)',
                line=dict(color='#2E86AB', width=2),
                marker=dict(size=8),
                hovertemplate='<b>%{x}</b><br>Amount: SAR %{y:,.0f}<extra></extra>',
            ))
        
        # Projections
        projections = data.get('projections', [])
        if projections:
            proj_periods = [p['period'] for p in projections]
            proj_amounts = [p['projected_amount'] for p in projections]
            proj_confidence = [p['confidence'] for p in projections]
            
            fig.add_trace(go.Scatter(
                x=proj_periods,
                y=proj_amounts,
                mode='lines+markers',
                name='Projected (FY25)',
                line=dict(color='#F18F01', width=2, dash='dash'),
                marker=dict(size=10, symbol='diamond'),
                hovertemplate='<b>%{x} FY25</b><br>Projected: SAR %{y:,.0f}<extra></extra>',
            ))
            
            # Confidence band (upper/lower)
            upper_band = [amt * (1 + (1 - conf) * 0.5) for amt, conf in zip(proj_amounts, proj_confidence)]
            lower_band = [amt * (1 - (1 - conf) * 0.5) for amt, conf in zip(proj_amounts, proj_confidence)]
            
            fig.add_trace(go.Scatter(
                x=proj_periods + proj_periods[::-1],
                y=upper_band + lower_band[::-1],
                fill='toself',
                fillcolor='rgba(241, 143, 1, 0.2)',
                line=dict(color='rgba(255,255,255,0)'),
                name='Confidence Band',
                showlegend=True,
                hoverinfo='skip',
            ))
        
        fig.update_layout(
            title='CFG Ukraine - Financial Forecast',
            xaxis_title='Period',
            yaxis_title='Amount (SAR)',
            template='plotly_white',
            height=500,
            width=900,
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            ),
            hovermode='x unified',
        )
        
        return fig.to_dict()
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format prediction results into natural language.
        """
        response_parts = []
        
        # Introduction
        response_parts.append("🔮 **Financial Forecast for CFG Ukraine**")
        response_parts.append("")
        
        # Historical context
        historical = data.get('historical_data', [])
        if historical:
            first_val = historical[0]['Amount']
            last_val = historical[-1]['Amount']
            growth = ((last_val / first_val) - 1) * 100 if first_val else 0
            
            response_parts.append("📊 **Historical Performance (FY24):**")
            response_parts.append(f"   • Starting (Jan): SAR {first_val:,.0f}")
            response_parts.append(f"   • Latest (Dec): SAR {last_val:,.0f}")
            response_parts.append(f"   • YTD Growth: {growth:+.1f}%")
            response_parts.append("")
        
        # Projections
        projections = data.get('projections', [])
        if projections:
            response_parts.append("📈 **FY25 Projections:**")
            for proj in projections:
                conf_pct = proj['confidence'] * 100
                response_parts.append(
                    f"   • {proj['period']} {proj['year']}: SAR {proj['projected_amount']:,.0f} "
                    f"(confidence: {conf_pct:.0f}%)"
                )
            response_parts.append("")
            
            # Summary
            avg_projection = sum(p['projected_amount'] for p in projections) / len(projections)
            response_parts.append(f"📌 **Average Projected Amount:** SAR {avg_projection:,.0f}")
        else:
            response_parts.append("⚠️ Insufficient historical data for reliable projections.")
        
        # Methodology
        response_parts.append("")
        response_parts.append("📋 **Methodology:**")
        response_parts.append(f"   • Model: {data.get('methodology', 'Linear Trend Projection')}")
        response_parts.append("   • Based on FY24 historical patterns")
        response_parts.append("   • Confidence decreases for longer-term projections")
        
        # Relevant accounts from semantic search
        relevant_accounts = data.get('relevant_accounts', [])
        if relevant_accounts:
            response_parts.append("")
            response_parts.append("🔍 **Related Accounts (from semantic search):**")
            for acc in relevant_accounts[:3]:
                response_parts.append(f"   • {acc['account']} (relevance: {acc['score']:.0%})")
        
        response_parts.append("")
        response_parts.append("📊 Interactive forecast chart included in response.")
        response_parts.append("🔗 Data source: Microsoft Fabric OneLake + RAG")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("🔮 Predictive Agent Test - CFG Ukraine")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = PredictiveAgent()
    
    # Test queries
    test_queries = [
        "What will our financials look like next quarter?",
        "Forecast the revenue for FY25",
        "Predict the trend for next 3 months",
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
        print(f"\n✅ Projections generated: {len(data.get('projections', []))}")
        print(f"✅ Historical data points: {len(data.get('historical_data', []))}")
        print(f"✅ Relevant accounts: {len(data.get('relevant_accounts', []))}")
        
        # Format response
        response = agent.format_response(data, classification)
        print(f"\n📝 Response:\n{response}")
        
        # Verify chart
        if 'chart' in data:
            chart_str = json.dumps(data['chart'])
            print(f"\n✅ Chart JSON: {len(chart_str)} characters")
    
    print("\n" + "=" * 60)
    print("✅ Predictive Agent Test Complete!")
    print("=" * 60)