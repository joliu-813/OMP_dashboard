"""
AI Analyzer Module
Provides AI-powered analysis of performance data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class AIAnalyzer:
    """AI-powered analysis for programmatic ads performance."""
    
    def __init__(self):
        """Initialize AI analyzer."""
        pass
    
    def generate_analysis(self, df: pd.DataFrame, pageviews_df: Optional[pd.DataFrame], 
                         alerts: List[Dict]) -> Dict:
        """
        Generate comprehensive AI analysis of performance data.
        
        Args:
            df: Combined DataFrame with wrapper data
            pageviews_df: DataFrame with pageview data (optional)
            alerts: List of alert dictionaries
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            'summary': self._generate_summary(df, pageviews_df),
            'insights': self._generate_insights(df, pageviews_df),
            'wrapper_analysis': self._analyze_wrappers(df),
            'recommendations': self._generate_recommendations(df, alerts),
            'anomalies': self._identify_anomalies(df, alerts)
        }
        
        return analysis
    
    def _generate_summary(self, df: pd.DataFrame, pageviews_df: Optional[pd.DataFrame]) -> str:
        """Generate executive summary."""
        total_revenue = df['revenue'].sum()
        total_impressions = df['impressions'].sum()
        avg_cpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
        
        date_range = df['date'].nunique()
        wrappers = df['wrapper'].nunique()
        
        summary = f"""Over the past {date_range} days, your programmatic advertising generated ${total_revenue:,.2f}
in revenue across {wrappers} wrappers with {total_impressions:,.0f} impressions and an average CPM of ${avg_cpm:.2f}."""

        if pageviews_df is not None:
            total_pv = pageviews_df['pageviews'].sum()
            rpm = (total_revenue / total_pv * 1000) if total_pv > 0 else 0
            summary += f""" Based on {total_pv:,.0f} pageviews, your overall RPM is ${rpm:.2f}."""
        
        return summary
    
    def _generate_insights(self, df: pd.DataFrame, pageviews_df: Optional[pd.DataFrame]) -> List[str]:
        """Generate key insights."""
        insights = []
        
        # Calculate wrapper performance
        agg_dict = {'revenue': 'sum', 'impressions': 'sum'}
        if 'ecpm' in df.columns:
            agg_dict['ecpm'] = 'mean'
        if 'cpm_revenue' in df.columns:
            agg_dict['cpm_revenue'] = 'sum'
        if 'cpc_revenue' in df.columns:
            agg_dict['cpc_revenue'] = 'sum'
        if 'ad_spend' in df.columns:
            agg_dict['ad_spend'] = 'sum'
            
        wrapper_perf = df.groupby('wrapper').agg(agg_dict).reset_index()
        wrapper_perf['cpm'] = (wrapper_perf['revenue'] / wrapper_perf['impressions'] * 1000)
        wrapper_perf['revenue_share'] = (wrapper_perf['revenue'] / wrapper_perf['revenue'].sum() * 100)
        
        # Top performing wrapper
        top_wrapper = wrapper_perf.loc[wrapper_perf['revenue'].idxmax()]
        insights.append(
            f"{top_wrapper['wrapper']} is your top revenue contributor "
            f"(${top_wrapper['revenue']:,.2f}, {top_wrapper['revenue_share']:.1f}% share)"
        )

        # Best CPM
        best_cpm_wrapper = wrapper_perf.loc[wrapper_perf['cpm'].idxmax()]
        if best_cpm_wrapper['wrapper'] != top_wrapper['wrapper']:
            insights.append(
                f"{best_cpm_wrapper['wrapper']} has the highest CPM (${best_cpm_wrapper['cpm']:.2f}), "
                f"suggesting strong optimization potential"
            )
        
        # CPM insights if available (treat CPM and eCPM as the same)
        if 'ecpm' in wrapper_perf.columns and wrapper_perf['ecpm'].sum() > 0:
            best_cpm_wrapper = wrapper_perf.loc[wrapper_perf['ecpm'].idxmax()]
            avg_cpm = wrapper_perf['ecpm'].mean()
            insights.append(
                f"Average CPM across wrappers is ${avg_cpm:.2f}, with {best_cpm_wrapper['wrapper']} "
                f"achieving the highest at ${best_cpm_wrapper['ecpm']:.2f}"
            )
        
        # Trend analysis
        daily_revenue = df.groupby('date')['revenue'].sum().reset_index()
        if len(daily_revenue) >= 7:
            first_week = daily_revenue.head(3)['revenue'].mean()
            last_week = daily_revenue.tail(3)['revenue'].mean()

            if first_week > 0:
                trend = ((last_week - first_week) / first_week) * 100
                if trend > 5:
                    insights.append(f"Revenue shows positive momentum with a {trend:.1f}% increase over the period")
                elif trend < -5:
                    insights.append(f"Revenue shows a declining trend of {abs(trend):.1f}% - investigation recommended")
        
        # Weekend vs weekday pattern
        df_with_dow = df.copy()
        df_with_dow['date'] = pd.to_datetime(df_with_dow['date'])
        df_with_dow['day_of_week'] = df_with_dow['date'].dt.dayofweek
        df_with_dow['is_weekend'] = df_with_dow['day_of_week'].isin([5, 6])

        weekend_perf = df_with_dow.groupby('is_weekend')['revenue'].mean()
        if len(weekend_perf) == 2:
            weekday_avg = weekend_perf[False]
            weekend_avg = weekend_perf[True]
            if weekday_avg > 0:
                weekend_diff = ((weekend_avg - weekday_avg) / weekday_avg) * 100
                if abs(weekend_diff) > 10:
                    pattern = "higher" if weekend_diff > 0 else "lower"
                    insights.append(
                        f"Weekend performance is {pattern} than weekdays "
                        f"({abs(weekend_diff):.1f}% difference)"
                    )
        
        return insights
    
    def _analyze_wrappers(self, df: pd.DataFrame) -> List[Dict]:
        """Analyze each wrapper's performance."""
        analyses = []
        
        for wrapper in df['wrapper'].unique():
            wrapper_df = df[df['wrapper'] == wrapper]
            
            revenue = wrapper_df['revenue'].sum()
            impressions = wrapper_df['impressions'].sum()
            cpm = (revenue / impressions * 1000) if impressions > 0 else 0
            
            analysis_parts = [f"Generated ${revenue:,.2f} from {impressions:,.0f} impressions (CPM: ${cpm:.2f})"]
            
            # Calculate trend
            wrapper_df_sorted = wrapper_df.sort_values('date')
            if len(wrapper_df_sorted) >= 2:
                first_half = wrapper_df_sorted.head(len(wrapper_df_sorted)//2)['revenue'].sum()
                second_half = wrapper_df_sorted.tail(len(wrapper_df_sorted)//2)['revenue'].sum()
                trend = "growing" if second_half > first_half else "declining" if second_half < first_half else "stable"
                analysis_parts.append(f"Trend: {trend}")
            else:
                analysis_parts.append("Trend: insufficient data")
            
            analyses.append({
                'name': wrapper,
                'analysis': ". ".join(analysis_parts)
            })
        
        return analyses
    
    def _generate_recommendations(self, df: pd.DataFrame, alerts: List[Dict]) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Check alerts for specific recommendations
        critical_wrappers = set(a['wrapper'] for a in alerts if a['severity'] == 'critical')
        
        for wrapper in df['wrapper'].unique():
            wrapper_df = df[df['wrapper'] == wrapper]
            wrapper_alerts = [a for a in alerts if a['wrapper'] == wrapper]
            
            if wrapper in critical_wrappers:
                # Find the most critical issue
                critical_alert = next((a for a in wrapper_alerts if a['severity'] == 'critical'), None)
                if critical_alert:
                    recommendations.append(
                        f"{wrapper}: Address {critical_alert['metric'].lower()} issue - "
                        f"{critical_alert['message']}"
                    )
            
            # Check fill rate / optimization
            if wrapper == 'Magnite':
                recommendations.append(
                    f"{wrapper}: Review prebid timeout settings and bidder priority "
                    f"to maximize yield"
                )
            elif wrapper == 'Google':
                recommendations.append(
                    f"{wrapper}: Optimize floor prices based on recent CPM trends "
                    f"to increase revenue"
                )
            elif wrapper == 'Amazon':
                recommendations.append(
                    f"{wrapper}: Check for any blocked demand or policy issues "
                    f"that might affect fill rate"
                )
        
        # General recommendations
        wrapper_perf = df.groupby('wrapper')['revenue'].sum()
        total_revenue = wrapper_perf.sum()
        
        if len(wrapper_perf) > 1:
            # Suggest rebalancing if one wrapper dominates
            max_share = (wrapper_perf.max() / total_revenue) * 100
            if max_share > 70:
                dominant = wrapper_perf.idxmax()
                recommendations.append(
                    f"Diversification: {dominant} accounts for {max_share:.1f}% of revenue. "
                    f"Consider strategies to grow other wrappers for better risk distribution"
                )

        # Data quality recommendation
        recommendations.append(
            "Data Quality: Ensure consistent tracking across all wrappers "
            "for accurate performance comparison"
        )
        
        return recommendations
    
    def _identify_anomalies(self, df: pd.DataFrame, alerts: List[Dict]) -> List[str]:
        """Identify and describe anomalies."""
        anomalies = []
        
        for alert in alerts:
            if alert['severity'] in ['critical', 'warning']:
                anomalies.append(
                    f"{alert['wrapper']} on {alert['date']}: {alert['message']}"
                )

        # Check for data gaps
        for wrapper in df['wrapper'].unique():
            wrapper_df = df[df['wrapper'] == wrapper].sort_values('date')
            if len(wrapper_df) >= 2:
                dates = pd.to_datetime(wrapper_df['date'])
                date_diffs = dates.diff().dt.days.dropna()

                if not date_diffs.empty and date_diffs.max() > 2:
                    anomalies.append(
                        f"{wrapper}: Potential data gap detected "
                        f"({date_diffs.max():.0f} days between records)"
                    )
        
        return anomalies
