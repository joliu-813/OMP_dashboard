"""
Programmatic Ads Performance Dashboard
A Streamlit application for analyzing programmatic advertising performance across multiple wrappers.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple

# Import custom modules
from data_processor import DataProcessor
from exodus_integration import ExodusIntegration
from ai_analyzer import AIAnalyzer
from alert_system import AlertSystem

# Page configuration
st.set_page_config(
    page_title="Programmatic Ads Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 1rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #3498db;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .alert-box {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 5px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    .alert-critical {
        background-color: #f8d7da;
        border: 1px solid #dc3545;
    }
    .alert-success {
        background-color: #d4edda;
        border: 1px solid #28a745;
    }
    .upload-section {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize session state variables."""
    if 'google_data' not in st.session_state:
        st.session_state.google_data = None
    if 'magnite_data' not in st.session_state:
        st.session_state.magnite_data = None
    if 'amazon_data' not in st.session_state:
        st.session_state.amazon_data = None
    if 'combined_data' not in st.session_state:
        st.session_state.combined_data = None
    if 'exodus_pageviews' not in st.session_state:
        st.session_state.exodus_pageviews = None
    if 'alerts' not in st.session_state:
        st.session_state.alerts = []
    if 'date_range' not in st.session_state:
        st.session_state.date_range = 14


def render_header():
    """Render the main header."""
    st.markdown('<div class="main-header">📊 Programmatic Ads Performance Dashboard</div>', 
                unsafe_allow_html=True)
    st.markdown("""
    Monitor and analyze your programmatic advertising performance across **Google (Adx+OB)**, 
    **Magnite Prebid Demand Manager**, and **Amazon Publisher Service**.
    """)


def render_sidebar():
    """Render the sidebar with controls."""
    with st.sidebar:
        st.header("⚙️ Dashboard Settings")
        
        # Date range selection
        st.subheader("Date Range")
        date_range = st.radio(
            "Select analysis period:",
            options=[7, 14],
            index=1,
            format_func=lambda x: f"Last {x} days"
        )
        st.session_state.date_range = date_range
        
        st.divider()
        
        # Alert threshold settings
        st.subheader("Alert Thresholds")
        revenue_threshold = st.slider(
            "Revenue Change Alert (%)",
            min_value=5,
            max_value=50,
            value=10,
            step=5
        )
        cpm_threshold = st.slider(
            "CPM/eCPM Change Alert (%)",
            min_value=5,
            max_value=50,
            value=10,
            step=5
        )
        
        st.divider()
        
        # Data refresh button
        if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        return revenue_threshold, cpm_threshold


def render_file_uploads():
    """Render file upload sections for each wrapper."""
    st.markdown('<div class="section-header">📁 Upload Data Files</div>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown('<div class="upload-section">', unsafe_allow_html=True)
        st.subheader("🔵 Google (Adx+OB)")
        google_file = st.file_uploader(
            "Upload Google CSV/Excel",
            type=['csv', 'xlsx', 'xls'],
            key='google_upload',
            help="Upload your Google Adx+OB performance data"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="upload-section">', unsafe_allow_html=True)
        st.subheader("🟣 Magnite Prebid")
        magnite_file = st.file_uploader(
            "Upload Magnite CSV/Excel",
            type=['csv', 'xlsx', 'xls'],
            key='magnite_upload',
            help="Upload your Magnite Prebid Demand Manager data"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="upload-section">', unsafe_allow_html=True)
        st.subheader("🟠 Amazon Publisher")
        amazon_file = st.file_uploader(
            "Upload Amazon CSV/Excel",
            type=['csv', 'xlsx', 'xls'],
            key='amazon_upload',
            help="Upload your Amazon Publisher Service data"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    return google_file, magnite_file, amazon_file


def process_uploaded_files(google_file, magnite_file, amazon_file, processor: DataProcessor):
    """Process uploaded files and store in session state."""
    data_dict = {}
    
    if google_file is not None:
        try:
            df = processor.load_file(google_file, 'google')
            st.session_state.google_data = df
            data_dict['Google'] = df
            st.success(f"✅ Google data loaded: {len(df)} rows")
        except Exception as e:
            st.error(f"❌ Error loading Google data: {str(e)}")
    
    if magnite_file is not None:
        try:
            df = processor.load_file(magnite_file, 'magnite')
            st.session_state.magnite_data = df
            data_dict['Magnite'] = df
            st.success(f"✅ Magnite data loaded: {len(df)} rows")
        except Exception as e:
            st.error(f"❌ Error loading Magnite data: {str(e)}")
    
    if amazon_file is not None:
        try:
            df = processor.load_file(amazon_file, 'amazon')
            st.session_state.amazon_data = df
            data_dict['Amazon'] = df
            st.success(f"✅ Amazon data loaded: {len(df)} rows")
        except Exception as e:
            st.error(f"❌ Error loading Amazon data: {str(e)}")
    
    if data_dict:
        # Combine all data
        combined = processor.combine_wrapper_data(data_dict)
        st.session_state.combined_data = combined


def render_summary_metrics():
    """Render summary KPI cards."""
    if st.session_state.combined_data is None:
        return
    
    st.markdown('<div class="section-header">📈 Key Performance Metrics</div>', unsafe_allow_html=True)
    
    df = st.session_state.combined_data
    
    # Calculate metrics for the selected period
    latest_date = df['date'].max()
    date_range = st.session_state.date_range
    start_date = latest_date - timedelta(days=date_range)
    
    period_df = df[df['date'] >= start_date]
    
    # Calculate unified totals - Just 3 metrics
    total_revenue = period_df['revenue'].sum()
    total_impressions = period_df['impressions'].sum()
    avg_cpm = (total_revenue / total_impressions * 1000) if total_impressions > 0 else 0
    
    # Day-over-day comparison
    latest_day = df[df['date'] == latest_date]
    previous_day = df[df['date'] == latest_date - timedelta(days=1)]
    
    latest_revenue = latest_day['revenue'].sum() if not latest_day.empty else 0
    prev_revenue = previous_day['revenue'].sum() if not previous_day.empty else 0
    revenue_dod = ((latest_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0
    
    latest_cpm = (latest_day['revenue'].sum() / latest_day['impressions'].sum() * 1000) if not latest_day.empty and latest_day['impressions'].sum() > 0 else 0
    prev_cpm = (previous_day['revenue'].sum() / previous_day['impressions'].sum() * 1000) if not previous_day.empty and previous_day['impressions'].sum() > 0 else 0
    cpm_dod = ((latest_cpm - prev_cpm) / prev_cpm * 100) if prev_cpm > 0 else 0
    
    # Display unified metrics - Just 3 key metrics
    st.subheader("📊 Key Performance Metrics")
    st.info("Revenue includes all earnings (ad spend treated as revenue). CPM = eCPM.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label=f"💰 Total Revenue (Last {date_range} days)",
            value=f"${total_revenue:,.2f}",
            delta=f"{revenue_dod:.1f}% DoD"
        )
    
    with col2:
        st.metric(
            label=f"👁️ Total Impressions (Last {date_range} days)",
            value=f"{total_impressions:,.0f}",
            delta=None
        )
    
    with col3:
        st.metric(
            label=f"💵 Avg CPM (Last {date_range} days)",
            value=f"${avg_cpm:.2f}",
            delta=f"{cpm_dod:.1f}% DoD"
        )
    
    # Overall RPM if pageviews available
    if st.session_state.exodus_pageviews is not None:
        st.markdown("<br>", unsafe_allow_html=True)
        total_pv = st.session_state.exodus_pageviews['pageviews'].sum()
        rpm = (total_revenue / total_pv * 1000) if total_pv > 0 else 0
        
        col1, col2, col3 = st.columns(3)
        with col2:
            st.metric(
                label="📈 Overall RPM (Revenue per 1000 Pageviews)",
                value=f"${rpm:.2f}",
                help="Based on OMP pageviews (All Markets excl HK & SG)"
            )


def render_daily_trends():
    """Render daily trends charts."""
    if st.session_state.combined_data is None:
        return
    
    st.markdown('<div class="section-header">📊 Daily Trends</div>', unsafe_allow_html=True)
    
    df = st.session_state.combined_data
    date_range = st.session_state.date_range
    
    # Filter by date range
    latest_date = df['date'].max()
    start_date = latest_date - timedelta(days=date_range)
    df = df[df['date'] >= start_date]
    
    # Simplified: Just 3 metrics - Revenue, Impressions, CPM
    tab1, tab2, tab3 = st.tabs(["📊 Revenue", "👁️ Impressions", "💰 CPM/eCPM"])
    
    with tab1:
        st.subheader("Daily Revenue Trends")
        
        # Revenue trends by wrapper
        fig = px.line(
            df.groupby(['date', 'wrapper'])['revenue'].sum().reset_index(),
            x='date',
            y='revenue',
            color='wrapper',
            title='Revenue by Wrapper',
            labels={'revenue': 'Revenue ($)', 'date': 'Date', 'wrapper': 'Wrapper'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Stacked area chart - total revenue composition
        fig2 = px.area(
            df.groupby(['date', 'wrapper'])['revenue'].sum().reset_index(),
            x='date',
            y='revenue',
            color='wrapper',
            title='Total Revenue Composition',
            labels={'revenue': 'Revenue ($)', 'date': 'Date', 'wrapper': 'Wrapper'}
        )
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        st.subheader("Daily Impressions Trends")
        
        # Impressions trends by wrapper
        fig = px.line(
            df.groupby(['date', 'wrapper'])['impressions'].sum().reset_index(),
            x='date',
            y='impressions',
            color='wrapper',
            title='Impressions by Wrapper',
            labels={'impressions': 'Impressions', 'date': 'Date', 'wrapper': 'Wrapper'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Stacked area chart
        fig2 = px.area(
            df.groupby(['date', 'wrapper'])['impressions'].sum().reset_index(),
            x='date',
            y='impressions',
            color='wrapper',
            title='Total Impressions Composition',
            labels={'impressions': 'Impressions', 'date': 'Date', 'wrapper': 'Wrapper'}
        )
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        st.subheader("Daily CPM (Cost Per Mille)")
        st.info("CPM = (Revenue / Impressions) × 1000. CPM and eCPM are treated as the same metric.")
        
        # Calculate CPM by wrapper
        cpm_df = df.groupby(['date', 'wrapper']).apply(
            lambda x: (x['revenue'].sum() / x['impressions'].sum() * 1000) if x['impressions'].sum() > 0 else 0
        ).reset_index(name='cpm')
        
        fig = px.line(
            cpm_df,
            x='date',
            y='cpm',
            color='wrapper',
            title='CPM by Wrapper',
            labels={'cpm': 'CPM ($)', 'date': 'Date', 'wrapper': 'Wrapper'},
            markers=True
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)


def render_wrapper_comparison():
    """Render wrapper comparison analysis."""
    if st.session_state.combined_data is None:
        return
    
    st.markdown('<div class="section-header">⚖️ Wrapper Performance Comparison</div>', 
                unsafe_allow_html=True)
    
    df = st.session_state.combined_data
    date_range = st.session_state.date_range
    
    # Filter by date range
    latest_date = df['date'].max()
    start_date = latest_date - timedelta(days=date_range)
    df = df[df['date'] >= start_date]
    
    # Calculate wrapper-level metrics - simplified to only 4 columns
    wrapper_metrics = df.groupby('wrapper').agg({
        'revenue': 'sum',
        'impressions': 'sum'
    }).reset_index()
    
    # Calculate CPM and revenue share
    wrapper_metrics['cpm'] = (wrapper_metrics['revenue'] / wrapper_metrics['impressions'] * 1000).round(2)
    wrapper_metrics['revenue_share'] = (wrapper_metrics['revenue'] / wrapper_metrics['revenue'].sum() * 100).round(1)
    
    # Display comparison table - simplified
    st.subheader("Performance Summary")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Format table - only show 4 columns
        display_df = wrapper_metrics[['wrapper', 'revenue', 'impressions', 'cpm', 'revenue_share']].copy()
        display_df['revenue'] = display_df['revenue'].apply(lambda x: f"${x:,.2f}")
        display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{x:,.0f}")
        display_df['cpm'] = display_df['cpm'].apply(lambda x: f"${x:.2f}")
        display_df['revenue_share'] = display_df['revenue_share'].apply(lambda x: f"{x}%")
        
        # Rename columns for display
        display_df = display_df.rename(columns={
            'wrapper': 'Wrapper',
            'revenue': 'Total Revenue',
            'impressions': 'Impressions',
            'cpm': 'CPM',
            'revenue_share': 'Rev Share'
        })
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    with col2:
        # Pie chart for revenue share
        fig = px.pie(
            wrapper_metrics,
            values='revenue',
            names='wrapper',
            title='Revenue Share by Wrapper',
            hole=0.4
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    # Unified Metrics Summary - Just 3 metrics
    st.subheader("📊 Unified Performance Metrics")
    st.info("All campaigns are treated uniformly: Revenue = Total Earnings, CPM = eCPM (effective cost per mille)")
    
    # Create summary table with just 3 key metrics
    summary_df = wrapper_metrics[['wrapper', 'revenue', 'impressions', 'cpm', 'revenue_share']].copy()
    summary_df['rpm'] = summary_df.apply(
        lambda row: (row['revenue'] / row['impressions'] * 1000) if row['impressions'] > 0 else 0, axis=1
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fig = px.bar(
            summary_df,
            x='wrapper',
            y='revenue',
            title='💰 Revenue by Wrapper',
            labels={'revenue': 'Revenue ($)', 'wrapper': ''},
            color='wrapper',
            text=summary_df['revenue'].apply(lambda x: f"${x:,.0f}")
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            summary_df,
            x='wrapper',
            y='impressions',
            title='👁️ Impressions by Wrapper',
            labels={'impressions': 'Impressions', 'wrapper': ''},
            color='wrapper',
            text=summary_df['impressions'].apply(lambda x: f"{x:,.0f}")
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        fig = px.bar(
            summary_df,
            x='wrapper',
            y='cpm',
            title='💵 CPM by Wrapper',
            labels={'cpm': 'CPM ($)', 'wrapper': ''},
            color='wrapper',
            text=summary_df['cpm'].apply(lambda x: f"${x:.2f}")
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    # Bar comparison - Revenue
    st.subheader("Side-by-Side Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            wrapper_metrics,
            x='wrapper',
            y='revenue',
            title='Total Revenue by Wrapper',
            labels={'revenue': 'Revenue ($)', 'wrapper': ''},
            color='wrapper',
            text=wrapper_metrics['revenue'].apply(lambda x: f"${x:,.0f}")
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Show eCPM if available, otherwise CPM
        y_col = 'ecpm' if 'ecpm' in wrapper_metrics.columns and wrapper_metrics['ecpm'].sum() > 0 else 'cpm'
        y_label = 'eCPM ($)' if y_col == 'ecpm' else 'CPM ($)'
        title = 'eCPM by Wrapper' if y_col == 'ecpm' else 'CPM by Wrapper'
        
        fig = px.bar(
            wrapper_metrics,
            x='wrapper',
            y=y_col,
            title=title,
            labels={y_col: y_label, 'wrapper': ''},
            color='wrapper',
            text=wrapper_metrics[y_col].apply(lambda x: f"${x:.2f}")
        )
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    # Overall Daily Breakdown
    st.subheader("📅 Overall Daily Breakdown (All Wrappers Combined)")
    
    # Calculate daily totals across all wrappers
    daily_totals = df.groupby('date').agg({
        'revenue': 'sum',
        'impressions': 'sum'
    }).reset_index()
    daily_totals['cpm'] = (daily_totals['revenue'] / daily_totals['impressions'] * 1000).round(2)
    
    # Calculate RPM if pageviews available
    if st.session_state.exodus_pageviews is not None:
        # Merge with pageviews
        pv_df = st.session_state.exodus_pageviews.copy()
        daily_totals = daily_totals.merge(pv_df, on='date', how='left')
        daily_totals['pageviews'] = daily_totals['pageviews'].fillna(0)
        daily_totals['rpm'] = daily_totals.apply(
            lambda row: (row['revenue'] / row['pageviews'] * 1000) if row['pageviews'] > 0 else 0, axis=1
        )
    else:
        daily_totals['pageviews'] = 0
        daily_totals['rpm'] = 0
    
    # Overall Daily Breakdown Chart - Revenue, CPM, Impressions, RPM
    st.subheader("📊 Overall Daily Trends Chart")
    
    # Create subplot with 4 charts
    fig_overall = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Revenue ($)', 'CPM ($)', 'Impressions', 'RPM ($)'),
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )
    
    # Revenue chart
    fig_overall.add_trace(
        go.Scatter(x=daily_totals['date'], y=daily_totals['revenue'], 
                   mode='lines+markers', name='Revenue',
                   line=dict(color='#1f77b4')),
        row=1, col=1
    )
    
    # CPM chart
    fig_overall.add_trace(
        go.Scatter(x=daily_totals['date'], y=daily_totals['cpm'], 
                   mode='lines+markers', name='CPM',
                   line=dict(color='#ff7f0e')),
        row=1, col=2
    )
    
    # Impressions chart
    fig_overall.add_trace(
        go.Scatter(x=daily_totals['date'], y=daily_totals['impressions'], 
                   mode='lines+markers', name='Impressions',
                   line=dict(color='#2ca02c')),
        row=2, col=1
    )
    
    # RPM chart (if pageviews available)
    fig_overall.add_trace(
        go.Scatter(x=daily_totals['date'], y=daily_totals['rpm'], 
                   mode='lines+markers', name='RPM',
                   line=dict(color='#9467bd')),
        row=2, col=2
    )
    
    fig_overall.update_layout(
        height=600,
        title_text="Overall Daily Breakdown (Combined All Wrappers)",
        showlegend=False
    )
    
    st.plotly_chart(fig_overall, use_container_width=True)
    
    # Display table with RPM
    display_daily = daily_totals.copy()
    display_daily['revenue'] = display_daily['revenue'].apply(lambda x: f"${x:,.2f}")
    display_daily['impressions'] = display_daily['impressions'].apply(lambda x: f"{x:,.0f}")
    display_daily['cpm'] = display_daily['cpm'].apply(lambda x: f"${x:.2f}")
    display_daily['rpm'] = display_daily['rpm'].apply(lambda x: f"${x:.2f}" if x > 0 else "N/A")
    
    display_cols = ['date', 'revenue', 'impressions', 'cpm', 'rpm']
    display_daily = display_daily[display_cols].rename(columns={
        'date': 'Date',
        'revenue': 'Total Revenue',
        'impressions': 'Total Impressions',
        'cpm': 'CPM',
        'rpm': 'RPM'
    })
    
    st.dataframe(display_daily, use_container_width=True, hide_index=True)


def render_alerts(revenue_threshold: float, cpm_threshold: float):
    """Render alerts section."""
    if st.session_state.combined_data is None:
        return
    
    st.markdown('<div class="section-header">🚨 Performance Alerts</div>', unsafe_allow_html=True)
    
    df = st.session_state.combined_data
    alert_system = AlertSystem()
    
    # Generate alerts
    alerts = alert_system.check_alerts(df, revenue_threshold, cpm_threshold)
    st.session_state.alerts = alerts
    
    if alerts:
        # Group alerts by severity
        critical = [a for a in alerts if a['severity'] == 'critical']
        warning = [a for a in alerts if a['severity'] == 'warning']
        info = [a for a in alerts if a['severity'] == 'info']
        
        # Display summary
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Critical Alerts", len(critical))
        with col2:
            st.metric("Warnings", len(warning))
        with col3:
            st.metric("Info", len(info))
        
        # Display detailed alerts
        for alert in alerts:
            severity_class = "alert-critical" if alert['severity'] == 'critical' else \
                           "alert-success" if alert['severity'] == 'info' else ""
            
            with st.expander(f"{alert['icon']} {alert['title']} ({alert['date']})", 
                            expanded=alert['severity'] == 'critical'):
                st.markdown(f'<div class="alert-box {severity_class}">', unsafe_allow_html=True)
                st.write(f"**Wrapper:** {alert['wrapper']}")
                st.write(f"**Metric:** {alert['metric']}")
                st.write(f"**Change:** {alert['change']:.1f}%")
                st.write(f"**Details:** {alert['message']}")
                st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.success("✅ No abnormal patterns detected. All metrics are within expected ranges.")


def render_exodus_integration():
    """Render Exodus MCP integration for pageviews."""
    st.markdown('<div class="section-header">🌐 Exodus Pageview Integration (OMP)</div>', 
                unsafe_allow_html=True)
    
    st.info("""
    This section loads daily pageview data for OMP (all markets excluding Hong Kong and Singapore).
    This data is used to calculate the overall RPM (Revenue Per Mille pageviews).
    
    **Data Source Priority:**
    1. Upload CSV file (recommended - most accurate)
    2. Auto-fetch from Exodus MCP (requires API access)
    3. Use mock data (for demonstration only)
    """)
    
    # File upload for Exodus data
    st.subheader("📁 Upload Pageview Data (Recommended)")
    exodus_file = st.file_uploader(
        "Upload Exodus/GA4 Pageview CSV",
        type=['csv', 'xlsx', 'xls'],
        key='exodus_upload',
        help="Upload CSV with columns: date, pageviews (OMP = all markets excl HK & SG)"
    )
    
    if exodus_file is not None:
        try:
            # Read the file
            if exodus_file.name.lower().endswith('.csv'):
                pv_df = pd.read_csv(exodus_file)
            else:
                pv_df = pd.read_excel(exodus_file)
            
            # Normalize column names
            pv_df = _normalize_pageview_columns(pv_df)
            
            # Validate required columns
            if 'date' not in pv_df.columns or 'pageviews' not in pv_df.columns:
                st.error(f"❌ Missing required columns. Found: {list(pv_df.columns)}. Need: date, pageviews")
            else:
                # Process data
                pv_df['date'] = pd.to_datetime(pv_df['date']).dt.date
                pv_df['pageviews'] = pd.to_numeric(pv_df['pageviews'], errors='coerce').fillna(0)
                pv_df = pv_df.sort_values('date')
                
                st.session_state.exodus_pageviews = pv_df
                st.success(f"✅ Loaded {len(pv_df)} days of pageview data from file")
        except Exception as e:
            st.error(f"❌ Error loading file: {str(e)}")
    
    # Or fetch from Exodus API
    st.subheader("🔌 Or Fetch from Exodus MCP")
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if st.button("🔍 Fetch Pageview Data", type="secondary", use_container_width=True):
            with st.spinner("Fetching data from Exodus MCP..."):
                try:
                    exodus = ExodusIntegration()
                    pageviews_df = exodus.get_omp_pageviews(st.session_state.date_range)
                    st.session_state.exodus_pageviews = pageviews_df
                    st.success(f"✅ Retrieved {len(pageviews_df)} days of pageview data")
                except Exception as e:
                    st.error(f"❌ Error fetching Exodus data: {str(e)}")
    
    with col2:
        if st.session_state.exodus_pageviews is not None:
            pv_df = st.session_state.exodus_pageviews
            
            # Display summary metrics
            total_pv = pv_df['pageviews'].sum()
            avg_daily_pv = pv_df['pageviews'].mean()
            
            col1a, col2a = st.columns(2)
            with col1a:
                st.metric("Total Pageviews", f"{total_pv:,.0f}")
            with col2a:
                st.metric("Avg Daily Pageviews", f"{avg_daily_pv:,.0f}")
            
            # Display chart
            fig = px.line(
                pv_df,
                x='date',
                y='pageviews',
                title='Daily Pageviews (OMP - Excluding HK & SG)',
                labels={'pageviews': 'Pageviews', 'date': 'Date'},
                markers=True
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)


def _normalize_pageview_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize pageview column names to standard format."""
    df = df.copy()
    
    # Column name mappings
    column_map = {
        'date': [
            'date', 'Date', 'DATE', 'day', 'Day', 
            'traffic date', 'traffic_date', 'Traffic Date', 'Traffic_Date',
            'date_time', 'datetime', 'timestamp'
        ],
        'pageviews': [
            'pageviews', 'pageview', 'Pageviews', 'Pageview', 'PAGEVIEWS',
            'views', 'Views', 'VIEWS',
            'sessions', 'Sessions', 'SESSIONS',
            'users', 'Users', 'USERS',
            'omp_pageviews', 'omp pageviews', 'omp_pageview',
            'total_pageviews', 'total pageviews',
            'screenpageviews', 'screen_page_views', 'screen page views'
        ]
    }
    
    # Create reverse mapping
    reverse_map = {}
    for standard, variations in column_map.items():
        for var in variations:
            reverse_map[var.lower().strip()] = standard
    
    # Rename columns
    new_columns = {}
    for col in df.columns:
        col_lower = col.lower().strip()
        if col_lower in reverse_map:
            new_columns[col] = reverse_map[col_lower]
    
    df = df.rename(columns=new_columns)
    return df


def render_ai_analysis():
    """Render AI-powered analysis."""
    if st.session_state.combined_data is None:
        return
    
    st.markdown('<div class="section-header">🤖 AI Performance Analysis</div>', unsafe_allow_html=True)
    
    if st.button("🧠 Generate AI Analysis", type="primary", use_container_width=True):
        with st.spinner("Analyzing performance data..."):
            try:
                ai = AIAnalyzer()
                
                # Get combined data for analysis
                df = st.session_state.combined_data
                date_range = st.session_state.date_range
                latest_date = df['date'].max()
                start_date = latest_date - timedelta(days=date_range)
                df = df[df['date'] >= start_date]
                
                # Get pageviews if available
                pv_df = st.session_state.exodus_pageviews
                
                analysis = ai.generate_analysis(df, pv_df, st.session_state.alerts)
                
                st.markdown("### 📋 Executive Summary")
                st.write(analysis['summary'])
                
                st.markdown("### 💡 Key Insights")
                for insight in analysis['insights']:
                    st.write(f"• {insight}")
                
                st.markdown("### 📊 Wrapper Performance")
                for wrapper_perf in analysis['wrapper_analysis']:
                    st.write(f"**{wrapper_perf['name']}:** {wrapper_perf['analysis']}")
                
                st.markdown("### 🎯 Recommendations")
                for rec in analysis['recommendations']:
                    st.write(f"• {rec}")
                
                if analysis['anomalies']:
                    st.markdown("### ⚠️ Notable Anomalies")
                    for anomaly in analysis['anomalies']:
                        st.write(f"• {anomaly}")
                
            except Exception as e:
                st.error(f"❌ Error generating AI analysis: {str(e)}")


def main():
    """Main application function."""
    initialize_session_state()
    
    render_header()
    revenue_threshold, cpm_threshold = render_sidebar()
    
    # File uploads
    google_file, magnite_file, amazon_file = render_file_uploads()
    
    # Process uploaded files
    processor = DataProcessor()
    process_uploaded_files(google_file, magnite_file, amazon_file, processor)
    
    # Exodus integration (can work without wrapper data)
    render_exodus_integration()
    
    # If we have wrapper data, show the rest of the dashboard
    if st.session_state.combined_data is not None:
        # Summary metrics
        render_summary_metrics()
        
        # Daily trends
        render_daily_trends()
        
        # Wrapper comparison
        render_wrapper_comparison()
        
        # Alerts
        render_alerts(revenue_threshold, cpm_threshold)
        
        # AI Analysis
        render_ai_analysis()
    else:
        st.info("👆 Please upload data files for at least one wrapper to see the performance dashboard.")


if __name__ == "__main__":
    main()
