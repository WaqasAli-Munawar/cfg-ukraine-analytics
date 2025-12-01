"""
Descriptive Agent - "What happened?"
Handles queries about historical data and trends
Uses RAG Retriever for semantic-enhanced retrieval with hierarchy support
"""
from typing import Dict, Any
import pandas as pd
import plotly.graph_objects as go

from src.models.query import QueryClassification
from src.services.rag_retriever import RAGRetriever
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DescriptiveAgent:
    """
    Handles "What happened?" queries.
    Retrieves and presents historical financial/operational data.
    """
    
    def __init__(self):
        self.retriever = RAGRetriever()
        logger.info("Descriptive Agent initialized with RAG")
    
    def retrieve(self, classification: QueryClassification) -> Dict[str, Any]:
        """
        Retrieve data for descriptive queries using RAG.
        """
        logger.info(
            "Descriptive retrieval with RAG",
            metrics=classification.metrics,
        )
        
        # Get data from RAG retriever
        result = self.retriever.retrieve_for_descriptive(classification)
        
        # Generate chart with proper data
        chart_json = self._create_chart(result)
        result['chart'] = chart_json
        
        return result
    
    def _create_chart(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create trend chart from retrieved data.
        """
        fig = go.Figure()
        
        records = data.get('data', [])
        metric = data.get('metric', 'Amount')
        year = data.get('year', 'FY24')
        
        if records and len(records) > 0:
            periods = [r['Period'] for r in records]
            amounts = [r['Amount'] for r in records]
            
            # Determine chart color based on trend
            trend = data.get('trend', {})
            direction = trend.get('direction', 'stable')
            
            if direction == 'increasing':
                line_color = '#27AE60'  # Green
            elif direction == 'decreasing':
                line_color = '#E74C3C'  # Red
            else:
                line_color = '#2E86AB'  # Blue
            
            # Add line trace
            fig.add_trace(go.Scatter(
                x=periods,
                y=amounts,
                mode='lines+markers',
                name=f'{metric} (SAR)',
                line=dict(color=line_color, width=3),
                marker=dict(size=10, color=line_color),
                hovertemplate='<b>%{x}</b><br>' + f'{metric}: ' + 'SAR %{y:,.0f}<extra></extra>',
                fill='tozeroy',
                fillcolor=f'rgba({44 if direction == "increasing" else 231}, {160 if direction == "increasing" else 76}, {44 if direction == "increasing" else 60}, 0.1)',
            ))
            
            # Add bar trace as secondary visualization
            fig.add_trace(go.Bar(
                x=periods,
                y=amounts,
                name=f'{metric} (Bar)',
                marker=dict(
                    color=amounts,
                    colorscale='Blues',
                    showscale=False,
                ),
                opacity=0.3,
                hovertemplate='<b>%{x}</b><br>' + f'{metric}: ' + 'SAR %{y:,.0f}<extra></extra>',
                visible='legendonly',  # Hidden by default, can toggle
            ))
        
        # Chart title with metric name
        chart_title = f"CFG Ukraine - {metric.upper() if metric else 'Financial'} Trend ({year})"
        
        fig.update_layout(
            title=dict(
                text=chart_title,
                font=dict(size=16, color='#2C3E50'),
            ),
            xaxis_title="Period",
            yaxis_title=f"{metric} (SAR)" if metric else "Amount (SAR)",
            template='plotly_white',
            height=400,
            width=None,  # Auto width
            hovermode='x unified',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1,
            ),
            margin=dict(l=60, r=30, t=80, b=60),
            yaxis=dict(
                tickformat=',.0f',
                gridcolor='#E5E5E5',
            ),
            xaxis=dict(
                gridcolor='#E5E5E5',
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
        )
        
        return fig.to_dict()
    
    def format_response(self, data: Dict[str, Any], classification: QueryClassification) -> str:
        """
        Format retrieved data into natural language response.
        """
        summary = data.get('summary', {})
        trend = data.get('trend', {})
        year = data.get('year', 'FY24')
        metric = data.get('metric', None)
        metric_filtered = data.get('metric_filtered', False)
        account_count = data.get('account_count', 0)
        
        response_parts = []
        
        # Dynamic title based on metric
        if metric and metric_filtered:
            title = f"📊 **CFG Ukraine {metric.upper()} Summary ({year})**"
        else:
            title = f"📊 **CFG Ukraine Financial Summary ({year})**"
        
        response_parts.append(title)
        response_parts.append("")
        
        # Show metric info if filtered
        if metric_filtered and account_count > 0:
            response_parts.append(f"*Showing {metric.upper()} data from {account_count} related accounts*")
            response_parts.append("")
        
        # Summary statistics
        response_parts.append("**Key Metrics:**")
        response_parts.append(f"   • Total Amount: SAR {summary.get('total', 0):,.0f}")
        response_parts.append(f"   • Monthly Average: SAR {summary.get('average', 0):,.0f}")
        response_parts.append(f"   • Minimum: SAR {summary.get('min', 0):,.0f}")
        response_parts.append(f"   • Maximum: SAR {summary.get('max', 0):,.0f}")
        response_parts.append(f"   • Periods: {summary.get('periods', 0)}")
        response_parts.append("")
        
        # Trend analysis
        if trend:
            direction = trend.get('direction', 'stable')
            growth = trend.get('growth_pct', 0)
            
            if direction == 'increasing':
                emoji = "📈"
                trend_text = "Increasing"
            elif direction == 'decreasing':
                emoji = "📉"
                trend_text = "Decreasing"
            else:
                emoji = "➡️"
                trend_text = "Stable"
            
            response_parts.append(f"**Trend Analysis:** {emoji}")
            response_parts.append(f"   • Direction: {trend_text}")
            response_parts.append(f"   • Growth: {growth:+.1f}%")
            response_parts.append(f"   • Start Value: SAR {trend.get('start_value', 0):,.0f}")
            response_parts.append(f"   • End Value: SAR {trend.get('end_value', 0):,.0f}")
            response_parts.append("")
        
        # Relevant accounts from semantic search
        relevant_accounts = data.get('relevant_accounts', [])
        if relevant_accounts:
            response_parts.append("🔍 **Related Accounts (Semantic Search):**")
            for acc in relevant_accounts[:3]:
                score = acc.get('score', 0)
                response_parts.append(f"   • {acc['account']} (relevance: {score:.0%})")
            response_parts.append("")
        
        response_parts.append("📊 Interactive trend chart included in response.")
        response_parts.append("🔗 Data source: Microsoft Fabric OneLake + RAG")
        
        return '\n'.join(response_parts)


# Entry point for testing
if __name__ == "__main__":
    from src.agents.classifier_agent import QueryClassifierAgent
    import json
    
    print("=" * 60)
    print("📊 Descriptive Agent Test - With Hierarchy")
    print("=" * 60)
    
    classifier = QueryClassifierAgent()
    agent = DescriptiveAgent()
    
    test_queries = [
        "Show me the EBITDA trend for FY24",
        "What was the revenue in FY24?",
        "Display gross profit trend",
        "Show me net income for FY24",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        classification = classifier.classify(query)
        print(f"Category: {classification.category.value}")
        print(f"Metrics: {classification.metrics}")
        
        data = agent.retrieve(classification)
        print(f"\n✅ Metric filtered: {data.get('metric_filtered', False)}")
        print(f"✅ Metric: {data.get('metric', 'N/A')}")
        print(f"✅ Account count: {data.get('account_count', 'N/A')}")
        print(f"✅ Data rows: {data['row_count']}")
        print(f"✅ Chart included: {data.get('chart') is not None}")
        
        response = agent.format_response(data, classification)
        print(f"\n📝 Response Preview:\n{response[:500]}...")
    
    print("\n" + "=" * 60)
    print("✅ Descriptive Agent with Hierarchy Complete!")
    print("=" * 60)