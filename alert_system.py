"""
Alert System Module
Detects abnormal patterns and generates alerts.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict


class AlertSystem:
    """System for detecting and generating performance alerts."""
    
    def __init__(self):
        """Initialize alert system."""
        self.alerts = []
    
    def check_alerts(self, df: pd.DataFrame, revenue_threshold: float = 10.0, 
                     cpm_threshold: float = 10.0) -> List[Dict]:
        """
        Check for abnormal patterns and generate alerts.
        
        Args:
            df: Combined DataFrame with all wrapper data
            revenue_threshold: Threshold for revenue change alert (%)
            cpm_threshold: Threshold for CPM change alert (%)
            
        Returns:
            List of alert dictionaries
        """
        self.alerts = []
        
        if df is None or df.empty:
            return self.alerts
        
        # Get recent dates for analysis
        latest_date = pd.to_datetime(df['date']).max()
        
        # Check each wrapper
        for wrapper in df['wrapper'].unique():
            wrapper_df = df[df['wrapper'] == wrapper].copy()
            wrapper_df = wrapper_df.sort_values('date')
            
            if len(wrapper_df) < 2:
                continue
            
            # Get latest and previous day data
            latest = wrapper_df.iloc[-1]
            previous = wrapper_df.iloc[-2]
            
            # Check revenue change
            if previous['revenue'] > 0:
                revenue_change = ((latest['revenue'] - previous['revenue']) / previous['revenue']) * 100
                
                if abs(revenue_change) >= revenue_threshold:
                    severity = 'critical' if abs(revenue_change) >= 20 else 'warning'
                    direction = 'increase' if revenue_change > 0 else 'decrease'
                    
                    self.alerts.append({
                        'severity': severity,
                        'icon': '🚨' if severity == 'critical' else '⚠️',
                        'title': f"{wrapper}: Revenue {direction.title()}",
                        'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                        'wrapper': wrapper,
                        'metric': 'Revenue',
                        'change': revenue_change,
                        'message': f"Revenue {direction}d by {abs(revenue_change):.1f}% from ${previous['revenue']:.2f} to ${latest['revenue']:.2f}"
                    })
            
            # Check CPM change
            if previous['impressions'] > 0 and latest['impressions'] > 0:
                prev_cpm = (previous['revenue'] / previous['impressions']) * 1000
                latest_cpm = (latest['revenue'] / latest['impressions']) * 1000
                
                if prev_cpm > 0:
                    cpm_change = ((latest_cpm - prev_cpm) / prev_cpm) * 100
                    
                    if abs(cpm_change) >= cpm_threshold:
                        severity = 'critical' if abs(cpm_change) >= 20 else 'warning'
                        direction = 'increase' if cpm_change > 0 else 'decrease'
                        
                        self.alerts.append({
                            'severity': severity,
                            'icon': '💰' if direction == 'increase' else '📉',
                            'title': f"{wrapper}: CPM {direction.title()}",
                            'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                            'wrapper': wrapper,
                            'metric': 'CPM',
                            'change': cpm_change,
                            'message': f"CPM {direction}d by {abs(cpm_change):.1f}% from ${prev_cpm:.2f} to ${latest_cpm:.2f}"
                        })
            
            # Check for zero impressions (potential issue)
            if latest['impressions'] == 0 and previous['impressions'] > 0:
                self.alerts.append({
                    'severity': 'critical',
                    'icon': '🔴',
                    'title': f"{wrapper}: No Impressions",
                    'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                    'wrapper': wrapper,
                    'metric': 'Impressions',
                    'change': -100,
                    'message': f"Impressions dropped to 0 from {previous['impressions']:,.0f}"
                })
            
            # Check for unusual impression drop (>30%)
            if previous['impressions'] > 0:
                imp_change = ((latest['impressions'] - previous['impressions']) / previous['impressions']) * 100
                if imp_change <= -30:
                    self.alerts.append({
                        'severity': 'warning',
                        'icon': '📉',
                        'title': f"{wrapper}: Significant Impression Drop",
                        'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                        'wrapper': wrapper,
                        'metric': 'Impressions',
                        'change': imp_change,
                        'message': f"Impressions dropped by {abs(imp_change):.1f}% from {previous['impressions']:,.0f} to {latest['impressions']:,.0f}"
                    })
            
            # Check eCPM change if available
            if 'ecpm' in latest and 'ecpm' in previous:
                if previous['ecpm'] > 0:
                    ecpm_change = ((latest['ecpm'] - previous['ecpm']) / previous['ecpm']) * 100
                    if abs(ecpm_change) >= cpm_threshold:
                        severity = 'critical' if abs(ecpm_change) >= 20 else 'warning'
                        direction = 'increase' if ecpm_change > 0 else 'decrease'
                        
                        self.alerts.append({
                            'severity': severity,
                            'icon': '💎' if direction == 'increase' else '⚡',
                            'title': f"{wrapper}: eCPM {direction.title()}",
                            'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                            'wrapper': wrapper,
                            'metric': 'eCPM',
                            'change': ecpm_change,
                            'message': f"eCPM {direction}d by {abs(ecpm_change):.1f}% from ${previous['ecpm']:.2f} to ${latest['ecpm']:.2f}"
                        })
            
            # Check CPM/CPC revenue mix change
            if 'cpm_revenue' in latest and 'cpm_revenue' in previous:
                prev_cpm_rev = previous['cpm_revenue']
                latest_cpm_rev = latest['cpm_revenue']
                prev_cpc_rev = previous['cpc_revenue'] if 'cpc_revenue' in previous else 0
                latest_cpc_rev = latest['cpc_revenue'] if 'cpc_revenue' in latest else 0
                
                prev_total = prev_cpm_rev + prev_cpc_rev
                latest_total = latest_cpm_rev + latest_cpc_rev
                
                if prev_total > 0 and latest_total > 0:
                    prev_cpm_pct = (prev_cpm_rev / prev_total) * 100
                    latest_cpm_pct = (latest_cpm_rev / latest_total) * 100
                    pct_change = abs(latest_cpm_pct - prev_cpm_pct)
                    
                    if pct_change >= 10:
                        direction = "more CPM-heavy" if latest_cpm_pct > prev_cpm_pct else "more CPC-heavy"
                        self.alerts.append({
                            'severity': 'info',
                            'icon': '📊',
                            'title': f"{wrapper}: Revenue Mix Shift",
                            'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                            'wrapper': wrapper,
                            'metric': 'Revenue Mix',
                            'change': pct_change,
                            'message': f"Revenue mix shifted by {pct_change:.1f}% toward {direction} (CPM: {latest_cpm_pct:.1f}% vs {prev_cpm_pct:.1f}%)"
                        })
            
            # Check Ad Spend efficiency
            if 'ad_spend' in latest and 'ad_spend' in previous:
                if previous['ad_spend'] > 0 and latest['ad_spend'] > 0:
                    prev_roas = previous['revenue'] / previous['ad_spend']
                    latest_roas = latest['revenue'] / latest['ad_spend']
                    
                    if prev_roas > 0:
                        roas_change = ((latest_roas - prev_roas) / prev_roas) * 100
                        if roas_change <= -15:
                            self.alerts.append({
                                'severity': 'warning',
                                'icon': '💸',
                                'title': f"{wrapper}: ROAS Decline",
                                'date': latest['date'].strftime('%Y-%m-%d') if isinstance(latest['date'], datetime) else str(latest['date']),
                                'wrapper': wrapper,
                                'metric': 'ROAS',
                                'change': roas_change,
                                'message': f"ROAS declined by {abs(roas_change):.1f}% from {prev_roas:.2f}x to {latest_roas:.2f}x"
                            })
        
        # Add trend-based alerts
        self._check_trends(df)
        
        # Sort alerts by severity and date
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        self.alerts.sort(key=lambda x: (severity_order.get(x['severity'], 3), x['date']), reverse=False)
        
        return self.alerts
    
    def _check_trends(self, df: pd.DataFrame):
        """
        Check for longer-term trends and anomalies.
        
        Args:
            df: Combined DataFrame
        """
        for wrapper in df['wrapper'].unique():
            wrapper_df = df[df['wrapper'] == wrapper].copy().sort_values('date')
            
            if len(wrapper_df) < 7:
                continue
            
            # Calculate 7-day moving average
            wrapper_df['revenue_ma7'] = wrapper_df['revenue'].rolling(window=7).mean()
            
            # Check for consistent declining trend
            recent = wrapper_df.tail(7)
            if len(recent) >= 7:
                first_half = recent.head(3)['revenue'].mean()
                second_half = recent.tail(3)['revenue'].mean()
                
                if first_half > 0:
                    trend_change = ((second_half - first_half) / first_half) * 100
                    if trend_change <= -15:
                        self.alerts.append({
                            'severity': 'info',
                            'icon': '📊',
                            'title': f"{wrapper}: Declining Trend",
                            'date': str(recent.iloc[-1]['date']),
                            'wrapper': wrapper,
                            'metric': '7-Day Trend',
                            'change': trend_change,
                            'message': f"Revenue shows a declining trend over the past 7 days ({trend_change:.1f}%)"
                        })
    
    def get_alert_summary(self) -> Dict:
        """
        Get summary of current alerts.
        
        Returns:
            Dictionary with alert counts by severity
        """
        critical = len([a for a in self.alerts if a['severity'] == 'critical'])
        warning = len([a for a in self.alerts if a['severity'] == 'warning'])
        info = len([a for a in self.alerts if a['severity'] == 'info'])
        
        return {
            'total': len(self.alerts),
            'critical': critical,
            'warning': warning,
            'info': info,
            'has_issues': critical > 0 or warning > 0
        }
