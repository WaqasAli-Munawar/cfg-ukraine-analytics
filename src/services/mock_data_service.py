"""
Mock Data Service for CFG Ukraine
Provides realistic financial and operational data for testing
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import random

from src.utils.logger import get_logger

logger = get_logger(__name__)


class MockDataService:
    """
    Generates realistic mock data for CFG Ukraine agricultural business.
    
    Data includes:
    - Financial statements (P&L, Balance Sheet, Cash Flow)
    - Operational KPIs (production, yield, costs)
    - Budget vs Actual
    - Treasury positions
    """
    
    def __init__(self):
        self.base_year = 2020
        self.current_year = 2025
        logger.info("Mock data service initialized")
    
    def get_financial_summary(
        self,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Generate financial summary with key metrics.
        
        Metrics include:
        - Revenue
        - COGS (Cost of Goods Sold)
        - Gross Profit & Margin
        - OPEX (Operating Expenses)
        - EBITDA
        - Net Income
        """
        data = []
        
        # Generate quarterly data from 2020 to 2024
        for year in range(2020, 2026):
            for quarter in range(1, 5):
                # Base values with growth trend
                base_revenue = 50_000_000  # $50M base
                growth_factor = 1 + (year - 2020) * 0.08  # 8% annual growth
                seasonal_factor = [0.9, 1.0, 1.1, 1.05][quarter - 1]  # Seasonal variation
                
                # Add some randomness
                random_factor = random.uniform(0.95, 1.05)
                
                revenue = base_revenue * growth_factor * seasonal_factor * random_factor
                
                # Calculate other metrics
                cogs_rate = random.uniform(0.55, 0.60)  # 55-60% of revenue
                cogs = revenue * cogs_rate
                gross_profit = revenue - cogs
                gross_margin = (gross_profit / revenue) * 100
                
                opex_rate = random.uniform(0.15, 0.20)  # 15-20% of revenue
                opex = revenue * opex_rate
                
                ebitda = gross_profit - opex
                ebitda_margin = (ebitda / revenue) * 100
                
                depreciation = revenue * 0.03  # 3% of revenue
                ebit = ebitda - depreciation
                
                interest = revenue * 0.02  # 2% of revenue
                tax_rate = 0.20
                ebt = ebit - interest
                tax = ebt * tax_rate if ebt > 0 else 0
                net_income = ebt - tax
                
                # Create record
                period = f"{year}-Q{quarter}"
                data.append({
                    'period': period,
                    'fiscal_year': year,
                    'fiscal_quarter': quarter,
                    'revenue': round(revenue, 2),
                    'cogs': round(cogs, 2),
                    'gross_profit': round(gross_profit, 2),
                    'gross_margin_pct': round(gross_margin, 2),
                    'opex': round(opex, 2),
                    'ebitda': round(ebitda, 2),
                    'ebitda_margin_pct': round(ebitda_margin, 2),
                    'depreciation': round(depreciation, 2),
                    'ebit': round(ebit, 2),
                    'interest': round(interest, 2),
                    'ebt': round(ebt, 2),
                    'tax': round(tax, 2),
                    'net_income': round(net_income, 2),
                    'currency': 'USD',
                })
        
        df = pd.DataFrame(data)
        
        # Filter by period if specified
        if start_period:
            df = df[df['period'] >= start_period]
        if end_period:
            df = df[df['period'] <= end_period]
        
        return df
    
    def get_operational_kpis(
        self,
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Generate operational KPIs for agricultural operations.
        
        KPIs include:
        - Production volume (tons)
        - Yield (tons/hectare)
        - Planted area (hectares)
        - Cost per ton
        """
        data = []
        
        for year in range(2020, 2026):
            for quarter in range(1, 5):
                period = f"{year}-Q{quarter}"
                
                # Seasonal production (harvest in Q3, Q4)
                seasonal_production = [0.1, 0.2, 0.4, 0.3][quarter - 1]
                
                base_production = 100_000  # 100k tons base annual
                growth = 1 + (year - 2020) * 0.05  # 5% annual growth
                
                production_tons = base_production * growth * seasonal_production
                planted_area_ha = 50_000  # 50k hectares
                yield_tons_per_ha = production_tons / planted_area_ha if seasonal_production > 0 else 0
                
                cost_per_ton = random.uniform(300, 350)  # $300-350 per ton
                
                data.append({
                    'period': period,
                    'fiscal_year': year,
                    'fiscal_quarter': quarter,
                    'production_tons': round(production_tons, 2),
                    'planted_area_ha': planted_area_ha,
                    'yield_tons_per_ha': round(yield_tons_per_ha, 2),
                    'cost_per_ton': round(cost_per_ton, 2),
                })
        
        df = pd.DataFrame(data)
        
        if start_period:
            df = df[df['period'] >= start_period]
        if end_period:
            df = df[df['period'] <= end_period]
        
        return df
    
    def get_budget_vs_actual(self, year: int = 2024) -> pd.DataFrame:
        """
        Generate budget vs actual comparison for a given year.
        """
        data = []
        
        for quarter in range(1, 5):
            period = f"{year}-Q{quarter}"
            
            # Budget (planned)
            budget_revenue = 55_000_000 * [0.9, 1.0, 1.1, 1.05][quarter - 1]
            budget_ebitda = budget_revenue * 0.25  # 25% EBITDA margin target
            
            # Actual (with variance)
            actual_variance = random.uniform(0.95, 1.08)  # -5% to +8% variance
            actual_revenue = budget_revenue * actual_variance
            actual_ebitda = actual_revenue * random.uniform(0.23, 0.27)
            
            revenue_variance = actual_revenue - budget_revenue
            revenue_variance_pct = (revenue_variance / budget_revenue) * 100
            
            ebitda_variance = actual_ebitda - budget_ebitda
            ebitda_variance_pct = (ebitda_variance / budget_ebitda) * 100
            
            data.append({
                'period': period,
                'fiscal_year': year,
                'fiscal_quarter': quarter,
                'budget_revenue': round(budget_revenue, 2),
                'actual_revenue': round(actual_revenue, 2),
                'revenue_variance': round(revenue_variance, 2),
                'revenue_variance_pct': round(revenue_variance_pct, 2),
                'budget_ebitda': round(budget_ebitda, 2),
                'actual_ebitda': round(actual_ebitda, 2),
                'ebitda_variance': round(ebitda_variance, 2),
                'ebitda_variance_pct': round(ebitda_variance_pct, 2),
            })
        
        return pd.DataFrame(data)
    
    def get_variance_analysis(
    self,
    metric: str,
    period: str,
    comparison: str = "QoQ"
) -> Dict:
        """
        Generate variance analysis for diagnostic queries.
        Compares actual vs expected performance [[1](https://corporatefinanceinstitute.com/resources/accounting/variance-analysis/)].

        Args:
            metric: Metric name (e.g., 'revenue', 'gross_margin')
            period: Period to analyze (e.g., '2024-Q3')
            comparison: Comparison type ('QoQ', 'YoY', 'vs_budget')

        Returns:
            Dictionary with variance breakdown
        """
        # Map metric aliases to actual column names
        metric_mapping = {
            'ebitda': 'ebitda',
            'revenue': 'revenue',
            'gross_margin': 'gross_margin_pct',
            'gross_profit': 'gross_profit',
            'net_income': 'net_income',
            'opex': 'opex',
        }

        # Use mapped metric for DataFrame lookup
        mapped_metric = metric_mapping.get(metric, metric)

        # Parse period
        year, quarter_str = period.split('-')
        year = int(year)
        quarter = int(quarter_str[1])

        # Get current period data
        df = self.get_financial_summary()
        current = df[df['period'] == period].iloc[0]

        # Determine comparison period
        if comparison == "QoQ":
            if quarter == 1:
                prev_period = f"{year-1}-Q4"
            else:
                prev_period = f"{year}-Q{quarter-1}"
        elif comparison == "YoY":
            prev_period = f"{year-1}-Q{quarter}"
        else:  # vs_budget
            budget_df = self.get_budget_vs_actual(year)
            budget_row = budget_df[budget_df['period'] == period].iloc[0]
            return {
                'period': period,
                'metric': metric,
                'comparison': comparison,
                'current_value': float(current.get(mapped_metric, 0)),
                'budget_value': float(budget_row.get(f'budget_{metric}', 0)),
                'variance': float(budget_row.get(f'{metric}_variance', 0)),
                'variance_pct': float(budget_row.get(f'{metric}_variance_pct', 0)),
                'factors': [
                    {'factor': 'Market conditions', 'impact_pct': random.uniform(-5, 5)},
                    {'factor': 'Operational efficiency', 'impact_pct': random.uniform(-3, 3)},
                    {'factor': 'Commodity prices', 'impact_pct': random.uniform(-4, 4)},
                ]
            }

        # Get previous period data
        previous = df[df['period'] == prev_period].iloc[0]

        current_value = float(current.get(mapped_metric, 0))
        previous_value = float(previous.get(mapped_metric, 0))
        variance = current_value - previous_value
        variance_pct = (variance / previous_value * 100) if previous_value != 0 else 0

        return {
            'period': period,
            'metric': metric,
            'comparison': comparison,
            'current_value': current_value,
            'previous_value': previous_value,
            'variance': variance,
            'variance_pct': variance_pct,
            'factors': [
                {'factor': 'Volume changes', 'impact_pct': random.uniform(-5, 5)},
                {'factor': 'Price fluctuations', 'impact_pct': random.uniform(-4, 4)},
                {'factor': 'Cost structure', 'impact_pct': random.uniform(-3, 3)},
            ]
        }
    
    def get_forecast(
        self,
        metric: str,
        periods: int = 4
    ) -> pd.DataFrame:
        """
        Generate simple forecast for predictive queries.
        
        Uses linear trend from historical data.
        """
        # Get historical data
        df = self.get_financial_summary()
        
        # Calculate trend
        historical = df[metric].values[-8:]  # Last 8 quarters
        avg_growth = (historical[-1] / historical[0]) ** (1/7) - 1  # Quarterly growth rate
        
        # Generate forecast
        last_value = historical[-1]
        last_period = df['period'].iloc[-1]
        year, quarter = last_period.split('-')
        year = int(year)
        quarter = int(quarter[1])
        
        forecast_data = []
        for i in range(1, periods + 1):
            # Calculate next period
            quarter += 1
            if quarter > 4:
                quarter = 1
                year += 1
            
            period = f"{year}-Q{quarter}"
            forecast_value = last_value * ((1 + avg_growth) ** i)
            
            forecast_data.append({
                'period': period,
                'fiscal_year': year,
                'fiscal_quarter': quarter,
                metric: round(forecast_value, 2),
                'forecast_type': 'trend_based',
                'confidence': 'medium'
            })
        
        return pd.DataFrame(forecast_data)


# Entry point for testing
if __name__ == "__main__":
    print("=" * 60)
    print("📊 Mock Data Service - CFG Ukraine")
    print("=" * 60)
    
    service = MockDataService()
    
    # Test financial summary
    print("\n1. Financial Summary (Last 4 Quarters):")
    df = service.get_financial_summary(start_period="2024-Q1")
    print(df[['period', 'revenue', 'ebitda', 'gross_margin_pct', 'ebitda_margin_pct']].to_string(index=False))
    
    # Test operational KPIs
    print("\n2. Operational KPIs (2024):")
    df_ops = service.get_operational_kpis(start_period="2024-Q1")
    print(df_ops[['period', 'production_tons', 'yield_tons_per_ha']].to_string(index=False))
    
    # Test budget vs actual
    print("\n3. Budget vs Actual (2024):")
    df_budget = service.get_budget_vs_actual(2024)
    print(df_budget[['period', 'actual_revenue', 'revenue_variance_pct', 'ebitda_variance_pct']].to_string(index=False))
    
    # Test variance analysis
    print("\n4. Variance Analysis (Revenue Q3 2024 vs Q2 2024):")
    variance = service.get_variance_analysis('revenue', '2024-Q3', 'QoQ')
    print(f"   Current: ${variance['current_value']:,.0f}")
    print(f"   Previous: ${variance['previous_value']:,.0f}")
    print(f"   Variance: ${variance['variance']:,.0f} ({variance['variance_pct']:.1f}%)")
    
    # Test forecast
    print("\n5. Revenue Forecast (Next 4 Quarters):")
    df_forecast = service.get_forecast('revenue', periods=4)
    print(df_forecast[['period', 'revenue', 'confidence']].to_string(index=False))
    
    print("\n" + "=" * 60)