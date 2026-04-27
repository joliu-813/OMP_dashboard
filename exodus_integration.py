"""
Exodus MCP Integration Module
Fetches pageview data from Exodus MCP for RPM calculations.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import streamlit as st
import json


class ExodusIntegration:
    """Integration with Exodus MCP for pageview data."""
    
    def __init__(self):
        """Initialize Exodus integration."""
        self.cube_name = "Events"
        self.pageview_measure = "Events.pageviews"
    
    def get_omp_pageviews(self, days: int = 14) -> pd.DataFrame:
        """
        Fetch OMP pageview data for the specified number of days.
        Excludes Hong Kong and Singapore markets.
        
        Priority:
        1. Try to load from manual CSV file (real_pageviews.csv)
        2. Try to fetch from Exodus MCP API
        3. Fall back to mock data with warning
        
        Args:
            days: Number of days to fetch (default 14)
            
        Returns:
            DataFrame with columns: date, pageviews
        """
        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Option 1: Try to load manual CSV file
        manual_data = self._load_manual_csv(start_date, end_date)
        if manual_data is not None:
            st.success("✅ Using real pageview data from CSV file")
            return manual_data
        
        # Option 2: Try to fetch real data from Exodus MCP
        try:
            return self._fetch_exodus_data(start_date, end_date)
        except Exception as e:
            # Option 3: Fall back to mock data
            st.warning("⚠️ Using mock pageview data (NOT REAL). Upload 'real_pageviews.csv' or connect Exodus MCP for accurate data.")
            with st.expander("📊 Why am I seeing mock data?"):
                st.info("""
                The dashboard is currently using **mock/estimated** pageview data.
                
                To use **real data**:
                1. Export pageviews from Exodus MCP (excluding HK & SG)
                2. Save as 'real_pageviews.csv' with columns: date, pageviews
                3. Restart the dashboard
                
                Or contact your Exodus MCP admin for API access.
                """)
            return self._generate_mock_pageviews(start_date, end_date)
    
    def _load_manual_csv(self, start_date, end_date) -> pd.DataFrame:
        """
        Try to load pageview data from a manual CSV file.
        
        Args:
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            DataFrame with date and pageviews, or None if file not found
        """
        try:
            import os
            if not os.path.exists('real_pageviews.csv'):
                return None
            
            df = pd.read_csv('real_pageviews.csv')
            
            # Validate columns
            if 'date' not in df.columns or 'pageviews' not in df.columns:
                st.error("❌ real_pageviews.csv must have 'date' and 'pageviews' columns")
                return None
            
            # Process data
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['pageviews'] = pd.to_numeric(df['pageviews'], errors='coerce')
            df = df.dropna()
            
            # Filter to requested date range
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            if len(df) == 0:
                st.warning("⚠️ real_pageviews.csv has no data in the requested date range")
                return None
            
            return df.sort_values('date')
            
        except Exception as e:
            st.error(f"❌ Error loading real_pageviews.csv: {str(e)}")
            return None
    
    def _fetch_exodus_data(self, start_date, end_date) -> pd.DataFrame:
        """
        Fetch real pageview data from Exodus MCP.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with real pageview data from Exodus MCP
        """
        # Build Cube.js query for OMP pageviews (excluding HK and SG)
        query = {
            "measures": ["Events.pageviews"],
            "timeDimensions": [{
                "dimension": "Events.date",
                "granularity": "day",
                "dateRange": [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")]
            }],
            "filters": [
                {
                    "member": "Events.region",
                    "operator": "notEquals",
                    "values": ["Hong Kong", "Singapore"]
                }
            ],
            "order": {
                "Events.date": "asc"
            }
        }
        
        # Note: This requires the Exodus MCP tool to be available
        # In the current environment, this will use mock data
        # In production with Exodus MCP access, uncomment the following:
        
        # result = exodus_mcp_execute_query(json_query=json.dumps(query))
        # if result:
        #     # Parse the result into DataFrame
        #     data = []
        #     for row in result.get('data', []):
        #         data.append({
        #             'date': pd.to_datetime(row['Events.date']).date(),
        #             'pageviews': int(row['Events.pageviews'])
        #         })
        #     return pd.DataFrame(data)
        
        # For now, raise exception to trigger mock data fallback
        raise Exception("Exodus MCP not connected - using mock data")
    
    def _generate_mock_pageviews(self, start_date, end_date) -> pd.DataFrame:
        """
        Generate mock pageview data for demonstration.
        In production, this would be replaced with actual Exodus MCP API call.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with mock pageview data
        """
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Generate realistic-looking pageview data with some weekly seasonality
        np.random.seed(42)
        base_pageviews = 1500000  # Updated: More realistic OMP daily pageviews (1.5M)
        
        data = []
        for date in date_range:
            # Add weekly pattern (lower on weekends)
            day_of_week = date.weekday()
            weekend_factor = 0.75 if day_of_week >= 5 else 1.0
            
            # Add some random variation (±15%)
            variation = np.random.normal(1, 0.15)
            
            pageviews = int(base_pageviews * weekend_factor * variation)
            
            data.append({
                'date': date.date(),
                'pageviews': pageviews
            })
        
        return pd.DataFrame(data)
    
    def calculate_rpm(self, revenue: float, pageviews: int) -> float:
        """
        Calculate Revenue Per Mille (RPM).
        
        Args:
            revenue: Total revenue
            pageviews: Total pageviews
            
        Returns:
            RPM value
        """
        if pageviews == 0:
            return 0.0
        return (revenue / pageviews) * 1000
    
    def get_pageview_summary(self, df: pd.DataFrame) -> dict:
        """
        Get summary statistics for pageviews.
        
        Args:
            df: DataFrame with pageview data
            
        Returns:
            Dictionary with summary statistics
        """
        return {
            'total_pageviews': int(df['pageviews'].sum()),
            'avg_daily_pageviews': int(df['pageviews'].mean()),
            'max_daily_pageviews': int(df['pageviews'].max()),
            'min_daily_pageviews': int(df['pageviews'].min()),
            'date_range': {
                'start': df['date'].min().strftime('%Y-%m-%d'),
                'end': df['date'].max().strftime('%Y-%m-%d')
            }
        }
