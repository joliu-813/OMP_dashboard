"""
Data Processor Module
Handles loading and processing of CSV/Excel files from different wrappers.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, BinaryIO
import streamlit as st


class DataProcessor:
    """Process and normalize data from different ad wrappers."""
    
    # Unified column mappings - all wrappers use the same normalized names
    # These are the STANDARD names all data will be mapped to
    COLUMN_MAPPINGS = {
        'date': ['date', 'Date', 'DATE', 'day', 'Day', 'date_time', 'datetime'],
        
        # Revenue: all variations map to 'revenue'
        'revenue': [
            'revenue', 'Revenue', 'REVENUE', 
            'earnings', 'Earnings', 
            'total revenue', 'total_revenue', 'totalrevenue',
            'total cpm and cpc revenue', 'total_cpm_and_cpc_revenue', 'totalcpmandcpcrevenue',
            'cpm revenue', 'cpm_revenue', 'cpmrevenue',
            'total cpm revenue', 'total_cpm_revenue',
            'gross revenue', 'gross_revenue'
        ],
        
        # Impressions: all variations map to 'impressions'  
        'impressions': [
            'impressions', 'Impressions', 'IMPRESSIONS', 
            'imp', 'imps', 'Imps',
            'total impressions', 'total_impressions', 'totalimpressions',
            'wrapper impressions', 'wrapper_impressions', 'wrapperimpressions'
        ],
        
        # CPM/ECPM: all variations map to 'ecpm' (effective CPM)
        'ecpm': [
            'ecpm', 'eCPM', 'ECPM',
            'cpm', 'CPM',
            'effective_cpm', 'effective cpm',
            'total average ecpm', 'total_average_ecpm', 'totalaverageecpm',
            'avg ecpm', 'avg_ecpm', 'average ecpm', 'average_ecpm',
            'wrapper ecpm', 'wrapper_ecpm', 'wrapperecpm'
        ],
        
        # Clicks: all variations map to 'clicks'
        'clicks': [
            'clicks', 'Clicks', 'CLICKS',
            'total clicks', 'total_clicks', 'totalclicks',
            'click', 'Click'
        ],
        
        # Ad Spend: cost/spend data
        'ad_spend': [
            'ad_spend', 'ad spend', 'adspend',
            'spend', 'Spend',
            'wrapper ad spend', 'wrapper_ad_spend', 'wrapperadspend',
            'total ad spend', 'total_ad_spend',
            'media_cost', 'media cost', 'mediacost',
            'cost', 'Cost'
        ]
    }
    
    def load_file(self, file: BinaryIO, wrapper_type: str) -> pd.DataFrame:
        """
        Load and process a file from a specific wrapper.
        
        Args:
            file: The uploaded file object
            wrapper_type: Type of wrapper ('google', 'magnite', 'amazon')
            
        Returns:
            DataFrame with normalized columns
        """
        # Determine file type and read
        file_name = file.name.lower()
        
        if file_name.endswith('.csv'):
            # Try reading with comma separator first (handle thousands separators like "86,907")
            df = pd.read_csv(file, thousands=',')
            # If only one column, try tab separator
            if len(df.columns) == 1:
                file.seek(0)
                df = pd.read_csv(file, sep='\t', thousands=',')
            # If still only one column, try auto-detect
            if len(df.columns) == 1:
                file.seek(0)
                df = pd.read_csv(file, sep=None, engine='python', thousands=',')
        elif file_name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file)
        else:
            raise ValueError(f"Unsupported file format: {file_name}")
        
        # Debug: show raw columns
        print(f"DEBUG {wrapper_type}: Raw columns: {list(df.columns)}")
        
        # Normalize column names
        df = self._normalize_columns(df, wrapper_type)
        
        # Check what columns we have after normalization
        has_revenue = 'revenue' in df.columns
        has_ecpm = 'ecpm' in df.columns
        has_impressions = 'impressions' in df.columns
        has_ad_spend = 'ad_spend' in df.columns
        
        # Ensure required columns exist (date is always required)
        if 'date' not in df.columns:
            raise ValueError(f"Missing required column: 'date'. Found columns: {list(df.columns)}")
        
        # Process date column
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # Handle revenue column logic:
        # Priority 1: If revenue exists, use it
        # Priority 2: If ad_spend exists (like Magnite), use it as revenue
        # Priority 3: If eCPM and impressions exist, calculate revenue
        if not has_revenue and has_ad_spend:
            # For Magnite: use ad_spend as revenue
            df['revenue'] = pd.to_numeric(df['ad_spend'], errors='coerce').fillna(0)
            has_revenue = True
        elif not has_revenue and has_ecpm and has_impressions:
            # Calculate revenue from eCPM and impressions
            df['revenue'] = (df['ecpm'] * df['impressions']) / 1000
            has_revenue = True
        
        # Check if we have the minimum required columns now
        required = ['revenue', 'impressions']
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}. Found columns: {list(df.columns)}")
        
        # Convert numeric columns
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0)
        
        # Ensure ad_spend exists (set to 0 if not present)
        if 'ad_spend' not in df.columns:
            df['ad_spend'] = 0
        else:
            df['ad_spend'] = pd.to_numeric(df['ad_spend'], errors='coerce').fillna(0)
        
        # Process eCPM/CPM column - treat CPM and eCPM as the same
        if 'ecpm' in df.columns:
            df['ecpm'] = pd.to_numeric(df['ecpm'], errors='coerce').fillna(0)
        else:
            df['ecpm'] = 0
        
        # If eCPM is 0 or missing, calculate it from revenue/impressions
        # CPM = eCPM = (Revenue / Impressions) * 1000
        df['ecpm'] = np.where(
            (df['ecpm'] == 0) & (df['impressions'] > 0),
            (df['revenue'] / df['impressions']) * 1000,
            df['ecpm']
        )
        
        # CPM = eCPM (they're the same metric)
        df['cpm'] = df['ecpm']
        
        # Simple unified metrics - no CPM/CPC breakdown
        # All campaigns are treated the same
        df['cpm_revenue'] = df['revenue']
        df['cpc_revenue'] = 0
        df['total_cpm_cpc_revenue'] = df['revenue']
        
        # Add clicks if present (optional)
        if 'clicks' in df.columns:
            df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce').fillna(0)
        else:
            df['clicks'] = 0
        
        # Add wrapper identifier
        df['wrapper'] = wrapper_type.capitalize()
        
        # Sort by date
        df = df.sort_values('date')
        
        return df
    
    def _normalize_columns(self, df: pd.DataFrame, wrapper_type: str) -> pd.DataFrame:
        """
        Normalize column names to standard format.
        All wrappers use the same unified column naming.
        
        Args:
            df: Input DataFrame
            wrapper_type: Type of wrapper (kept for compatibility, not used)
            
        Returns:
            DataFrame with normalized column names
        """
        df = df.copy()
        
        # Clean column names first - remove quotes, extra spaces, and standardize
        cleaned_columns = {}
        for col in df.columns:
            # Remove BOM, quotes, strip whitespace, and normalize
            clean_col = col.strip().strip('\ufeff').strip('"').strip("'")
            # Also remove any other non-breaking spaces or special whitespace
            clean_col = clean_col.replace('\xa0', ' ').replace('\u200b', '')
            cleaned_columns[col] = clean_col
        
        df = df.rename(columns=cleaned_columns)
        
        # Debug output
        print(f"DEBUG {wrapper_type}: Cleaned columns: {list(df.columns)}")
        
        # Create reverse mapping from all variations to standard names
        reverse_map = {}
        for standard, variations in self.COLUMN_MAPPINGS.items():
            for var in variations:
                reverse_map[var.lower().strip()] = standard
        
        # Rename columns - track what we found
        new_columns = {}
        unmapped_columns = []
        
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in reverse_map:
                new_columns[col] = reverse_map[col_lower]
            else:
                unmapped_columns.append(col)
        
        # Fuzzy matching for unmapped columns - try to match by similarity
        for col in unmapped_columns[:]:
            col_normalized = col.lower().strip().replace(' ', '_').replace('-', '_')
            if col_normalized in reverse_map:
                new_columns[col] = reverse_map[col_normalized]
                unmapped_columns.remove(col)
                print(f"DEBUG {wrapper_type}: Fuzzy matched '{col}' -> '{reverse_map[col_normalized]}'")
            elif col_normalized == 'impressions' or 'impression' in col_normalized:
                new_columns[col] = 'impressions'
                unmapped_columns.remove(col)
                print(f"DEBUG {wrapper_type}: Fuzzy matched '{col}' -> 'impressions'")
            elif col_normalized == 'earnings' or 'earning' in col_normalized:
                new_columns[col] = 'revenue'
                unmapped_columns.remove(col)
                print(f"DEBUG {wrapper_type}: Fuzzy matched '{col}' -> 'revenue'")
            elif col_normalized == 'cpm':
                new_columns[col] = 'ecpm'
                unmapped_columns.remove(col)
                print(f"DEBUG {wrapper_type}: Fuzzy matched '{col}' -> 'ecpm'")
        
        if unmapped_columns:
            print(f"Note: Unmapped columns in {wrapper_type} data: {unmapped_columns}")
        
        df = df.rename(columns=new_columns)
        print(f"DEBUG {wrapper_type}: Final columns: {list(df.columns)}")
        return df
    
    def combine_wrapper_data(self, data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Combine data from multiple wrappers into a single DataFrame.
        
        Args:
            data_dict: Dictionary with wrapper names as keys and DataFrames as values
            
        Returns:
            Combined DataFrame
        """
        if not data_dict:
            return pd.DataFrame()
        
        combined = pd.concat(data_dict.values(), ignore_index=True)
        
        # Ensure consistent data types
        combined['date'] = pd.to_datetime(combined['date']).dt.date
        combined['wrapper'] = combined['wrapper'].astype(str)
        
        return combined
    
    def calculate_dod_change(self, df: pd.DataFrame, metric: str = 'revenue') -> pd.DataFrame:
        """
        Calculate day-over-day change for a metric.
        
        Args:
            df: DataFrame with daily data
            metric: Metric to calculate change for
            
        Returns:
            DataFrame with DoD change column added
        """
        df = df.copy()
        df = df.sort_values(['wrapper', 'date'])
        
        df[f'{metric}_dod'] = df.groupby('wrapper')[metric].pct_change() * 100
        
        return df
    
    def get_date_range(self, df: pd.DataFrame) -> tuple:
        """
        Get the date range of the data.
        
        Args:
            df: DataFrame with date column
            
        Returns:
            Tuple of (min_date, max_date)
        """
        min_date = pd.to_datetime(df['date']).min()
        max_date = pd.to_datetime(df['date']).max()
        return min_date, max_date
