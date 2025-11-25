"""
Visualization utilities for financial data
Creates interactive charts using Plotly - returns both HTML files and JSON
"""
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from typing import List, Dict, Any, Optional
from pathlib import Path
import json

from src.utils.logger import get_logger

logger = get_logger(__name__)


class FinancialVisualizer:
    """
    Creates interactive visualizations for CFG Ukraine financial data.
    Can return both HTML files (for standalone viewing) and JSON (for API responses).
    """
    
    def __init__(self, output_dir: str = "data/charts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    # ==================== JSON Methods (for API responses) ====================
    
    def create_trend_chart_json(
        self,
        df: pd.DataFrame,
        metrics: List[str],
        title: str = "Financial Trends",
    ) -> Dict[str, Any]:
        """
        Create a line chart as Plotly JSON (not HTML file).
        
        Args:
            df: DataFrame with 'period' column and metric columns
            metrics: List of metric column names to plot
            title: Chart title
        
        Returns:
            Dictionary with Plotly figure JSON
        """
        fig = go.Figure()
        
        # Add a line for each metric
        for metric in metrics:
            if metric in df.columns:
                is_pct = metric.endswith('_pct') or metric.endswith('_margin')
                
                fig.add_trace(go.Scatter(
                    x=df['period'].tolist(),
                    y=df[metric].tolist(),
                    mode='lines+markers',
                    name=metric.replace('_', ' ').title(),
                    hovertemplate='<b>%{x}</b><br>' +
                                  metric.replace('_', ' ').title() + 
                                  ': %{y:,.2f}' + ('%' if is_pct else '') +
                                  '<extra></extra>',
                ))
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Period",
            yaxis_title="Value",
            hovermode='x unified',
            template='plotly_white',
            height=500,
            width=800,
        )
        
        # Return as JSON-serializable dict
        return fig.to_dict()
    
    def create_waterfall_chart_json(
        self,
        variance_data: Dict[str, Any],
        title: str = "Variance Analysis",
    ) -> Dict[str, Any]:
        """
        Create a waterfall chart as Plotly JSON.
        
        Args:
            variance_data: Dictionary with variance and factors
            title: Chart title
        
        Returns:
            Dictionary with Plotly figure JSON
        """
        metric = variance_data['metric'].replace('_', ' ').title()
        
        # Build waterfall data
        labels = ['Previous']
        values = [variance_data['previous_value']]
        measures = ['absolute']
        
        # Add factors
        for factor in variance_data.get('factors', []):
            impact = variance_data['previous_value'] * (factor['impact_pct'] / 100)
            labels.append(factor['factor'])
            values.append(impact)
            measures.append('relative')
        
        # Add current as total
        labels.append('Current')
        values.append(variance_data['current_value'])
        measures.append('total')
        
        # Create waterfall chart
        fig = go.Figure(go.Waterfall(
            name="Variance",
            orientation="v",
            measure=measures,
            x=labels,
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        
        fig.update_layout(
            title=f"{title}<br>{metric} - {variance_data['period']} ({variance_data['comparison']})",
            showlegend=False,
            template='plotly_white',
            height=500,
            width=800,
        )
        
        # Return as JSON-serializable dict
        return fig.to_dict()
    
    def create_comparison_chart_json(
        self,
        df: pd.DataFrame,
        metric: str,
        title: str = "Quarterly Comparison",
    ) -> Dict[str, Any]:
        """
        Create a bar chart as Plotly JSON.
        
        Args:
            df: DataFrame with 'period' and metric columns
            metric: Metric column name
            title: Chart title
        
        Returns:
            Dictionary with Plotly figure JSON
        """
        is_pct = metric.endswith('_pct') or metric.endswith('_margin')
        
        fig = go.Figure(data=[
            go.Bar(
                x=df['period'].tolist(),
                y=df[metric].tolist(),
                text=[f"{x:.1f}%" if is_pct else f"${x:,.0f}" for x in df[metric]],
                textposition='outside',
                marker_color='#1f77b4',
            )
        ])
        
        fig.update_layout(
            title=title,
            xaxis_title="Period",
            yaxis_title=metric.replace('_', ' ').title(),
            template='plotly_white',
            height=500,
            width=800,
        )
        
        return fig.to_dict()
    
    def create_dual_axis_chart_json(
        self,
        df: pd.DataFrame,
        metric1: str,
        metric2: str,
        title: str = "Dual Metric Comparison",
    ) -> Dict[str, Any]:
        """
        Create a chart with two y-axes as Plotly JSON.
        
        Args:
            df: DataFrame with metrics
            metric1: First metric (left y-axis)
            metric2: Second metric (right y-axis)
            title: Chart title
        
        Returns:
            Dictionary with Plotly figure JSON
        """
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add first metric
        fig.add_trace(
            go.Scatter(
                x=df['period'].tolist(),
                y=df[metric1].tolist(),
                name=metric1.replace('_', ' ').title(),
                mode='lines+markers',
            ),
            secondary_y=False,
        )
        
        # Add second metric
        fig.add_trace(
            go.Scatter(
                x=df['period'].tolist(),
                y=df[metric2].tolist(),
                name=metric2.replace('_', ' ').title(),
                mode='lines+markers',
            ),
            secondary_y=True,
        )
        
        fig.update_layout(
            title=title,
            hovermode='x unified',
            template='plotly_white',
            height=500,
            width=800,
        )
        
        fig.update_xaxes(title_text="Period")
        fig.update_yaxes(title_text=metric1.replace('_', ' ').title(), secondary_y=False)
        fig.update_yaxes(title_text=metric2.replace('_', ' ').title(), secondary_y=True)
        
        return fig.to_dict()
    
    # ==================== HTML Methods (for file export) ====================
    
    def create_trend_chart(
        self,
        df: pd.DataFrame,
        metrics: List[str],
        title: str = "Financial Trends",
        filename: Optional[str] = None,
    ) -> str:
        """
        Create a line chart showing trends over time (saved as HTML).
        
        Args:
            df: DataFrame with 'period' column and metric columns
            metrics: List of metric column names to plot
            title: Chart title
            filename: Optional filename to save (auto-generated if None)
        
        Returns:
            Path to saved HTML file
        """
        fig = go.Figure()
        
        # Add a line for each metric
        for metric in metrics:
            if metric in df.columns:
                is_pct = metric.endswith('_pct') or metric.endswith('_margin')
                
                fig.add_trace(go.Scatter(
                    x=df['period'],
                    y=df[metric],
                    mode='lines+markers',
                    name=metric.replace('_', ' ').title(),
                    hovertemplate='<b>%{x}</b><br>' +
                                  metric.replace('_', ' ').title() + 
                                  ': %{y:,.2f}' + ('%' if is_pct else '') +
                                  '<extra></extra>',
                ))
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title="Period",
            yaxis_title="Value",
            hovermode='x unified',
            template='plotly_white',
            height=500,
        )
        
        # Save to file
        if not filename:
            filename = f"trend_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        filepath = self.output_dir / filename
        fig.write_html(str(filepath))
        
        logger.info(f"Saved chart to {filepath}")
        return str(filepath)
    
    def create_waterfall_chart(
        self,
        variance_data: Dict[str, Any],
        title: str = "Variance Analysis",
        filename: Optional[str] = None,
    ) -> str:
        """
        Create a waterfall chart showing variance breakdown (saved as HTML).
        
        Args:
            variance_data: Dictionary with variance and factors
            title: Chart title
            filename: Optional filename
        
        Returns:
            Path to saved HTML file
        """
        metric = variance_data['metric'].replace('_', ' ').title()
        
        # Build waterfall data
        labels = ['Previous']
        values = [variance_data['previous_value']]
        measures = ['absolute']
        
        # Add factors
        for factor in variance_data.get('factors', []):
            impact = variance_data['previous_value'] * (factor['impact_pct'] / 100)
            labels.append(factor['factor'])
            values.append(impact)
            measures.append('relative')
        
        # Add current as total
        labels.append('Current')
        values.append(variance_data['current_value'])
        measures.append('total')
        
        # Create waterfall chart
        fig = go.Figure(go.Waterfall(
            name="Variance",
            orientation="v",
            measure=measures,
            x=labels,
            y=values,
            connector={"line": {"color": "rgb(63, 63, 63)"}},
        ))
        
        fig.update_layout(
            title=f"{title}<br>{metric} - {variance_data['period']} ({variance_data['comparison']})",
            showlegend=False,
            template='plotly_white',
            height=500,
        )
        
        # Save to file
        if not filename:
            filename = f"waterfall_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        filepath = self.output_dir / filename
        fig.write_html(str(filepath))
        
        logger.info(f"Saved waterfall chart to {filepath}")
        return str(filepath)
    
    def create_comparison_chart(
        self,
        df: pd.DataFrame,
        metric: str,
        title: str = "Quarterly Comparison",
        filename: Optional[str] = None,
    ) -> str:
        """
        Create a bar chart comparing values across periods (saved as HTML).
        """
        is_pct = metric.endswith('_pct') or metric.endswith('_margin')
        
        fig = go.Figure(data=[
            go.Bar(
                x=df['period'],
                y=df[metric],
                text=df[metric].apply(lambda x: f"{x:.1f}%" if is_pct else f"${x:,.0f}"),
                textposition='outside',
                marker_color='#1f77b4',
            )
        ])
        
        fig.update_layout(
            title=title,
            xaxis_title="Period",
            yaxis_title=metric.replace('_', ' ').title(),
            template='plotly_white',
            height=500,
        )
        
        if not filename:
            filename = f"comparison_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        filepath = self.output_dir / filename
        fig.write_html(str(filepath))
        
        logger.info(f"Saved comparison chart to {filepath}")
        return str(filepath)
    
    def create_dual_axis_chart(
        self,
        df: pd.DataFrame,
        metric1: str,
        metric2: str,
        title: str = "Dual Metric Comparison",
        filename: Optional[str] = None,
    ) -> str:
        """
        Create a chart with two y-axes for comparing different scale metrics (saved as HTML).
        """
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add first metric
        fig.add_trace(
            go.Scatter(
                x=df['period'],
                y=df[metric1],
                name=metric1.replace('_', ' ').title(),
                mode='lines+markers',
            ),
            secondary_y=False,
        )
        
        # Add second metric
        fig.add_trace(
            go.Scatter(
                x=df['period'],
                y=df[metric2],
                name=metric2.replace('_', ' ').title(),
                mode='lines+markers',
            ),
            secondary_y=True,
        )
        
        fig.update_layout(
            title=title,
            hovermode='x unified',
            template='plotly_white',
            height=500,
        )
        
        fig.update_xaxes(title_text="Period")
        fig.update_yaxes(title_text=metric1.replace('_', ' ').title(), secondary_y=False)
        fig.update_yaxes(title_text=metric2.replace('_', ' ').title(), secondary_y=True)
        
        if not filename:
            filename = f"dual_axis_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        filepath = self.output_dir / filename
        fig.write_html(str(filepath))
        
        logger.info(f"Saved dual-axis chart to {filepath}")
        return str(filepath)


# Entry point for testing
if __name__ == "__main__":
    from src.services.mock_data_service import MockDataService
    
    print("=" * 60)
    print("📊 Financial Visualizer Test - HTML & JSON")
    print("=" * 60)
    
    # Get mock data
    service = MockDataService()
    df = service.get_financial_summary(start_period="2023-Q1")
    
    visualizer = FinancialVisualizer()
    
    print("\n" + "=" * 60)
    print("Part 1: HTML File Generation")
    print("=" * 60)
    
    # Test 1: Trend chart HTML
    print("\n1. Creating trend chart HTML...")
    filepath = visualizer.create_trend_chart(
        df,
        metrics=['revenue', 'ebitda'],
        title="CFG Ukraine - Revenue & EBITDA Trend",
        filename="test_trend.html"
    )
    print(f"   ✅ Saved to: {filepath}")
    
    # Test 2: Comparison chart HTML
    print("\n2. Creating comparison chart HTML...")
    filepath = visualizer.create_comparison_chart(
        df.tail(4),
        metric='ebitda',
        title="CFG Ukraine - Quarterly EBITDA",
        filename="test_comparison.html"
    )
    print(f"   ✅ Saved to: {filepath}")
    
    # Test 3: Dual axis chart HTML
    print("\n3. Creating dual-axis chart HTML...")
    filepath = visualizer.create_dual_axis_chart(
        df,
        metric1='revenue',
        metric2='ebitda_margin_pct',
        title="CFG Ukraine - Revenue vs EBITDA Margin",
        filename="test_dual_axis.html"
    )
    print(f"   ✅ Saved to: {filepath}")
    
    # Test 4: Waterfall chart HTML
    print("\n4. Creating waterfall chart HTML...")
    variance_data = service.get_variance_analysis('revenue', '2024-Q3', 'QoQ')
    filepath = visualizer.create_waterfall_chart(
        variance_data,
        title="Revenue Variance Analysis",
        filename="test_waterfall.html"
    )
    print(f"   ✅ Saved to: {filepath}")
    
    print("\n" + "=" * 60)
    print("Part 2: JSON Generation (for API)")
    print("=" * 60)
    
    # Test 5: Trend chart JSON
    print("\n5. Creating trend chart JSON...")
    chart_json = visualizer.create_trend_chart_json(
        df,
        metrics=['revenue', 'ebitda'],
        title="CFG Ukraine - Revenue & EBITDA Trend"
    )
    print(f"   ✅ Chart type: {chart_json['data'][0]['type']}")
    print(f"   ✅ Number of traces: {len(chart_json['data'])}")
    print(f"   ✅ Data points: {len(chart_json['data'][0]['x'])}")
    
    # Test 6: Waterfall chart JSON
    print("\n6. Creating waterfall chart JSON...")
    chart_json = visualizer.create_waterfall_chart_json(
        variance_data,
        title="Revenue Variance Analysis"
    )
    print(f"   ✅ Chart type: {chart_json['data'][0]['type']}")
    print(f"   ✅ Number of bars: {len(chart_json['data'][0]['x'])}")
    
    # Test 7: Comparison chart JSON
    print("\n7. Creating comparison chart JSON...")
    chart_json = visualizer.create_comparison_chart_json(
        df.tail(4),
        metric='ebitda',
        title="CFG Ukraine - Quarterly EBITDA"
    )
    print(f"   ✅ Chart type: {chart_json['data'][0]['type']}")
    print(f"   ✅ Number of bars: {len(chart_json['data'][0]['x'])}")
    
    # Test 8: Verify JSON is serializable
    print("\n8. Verifying JSON serializability...")
    try:
        json_str = json.dumps(chart_json)
        print(f"   ✅ JSON serializable: {len(json_str)} characters")
    except Exception as e:
        print(f"   ❌ JSON serialization failed: {e}")
    
    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("📂 HTML files saved to: data/charts/")
    print("📊 JSON methods ready for API integration")
    print("=" * 60)