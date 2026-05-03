import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
from streamlit_extras.add_vertical_space import add_vertical_space


class FeaturesStatisticsAgent():


    def detailed_dataframe_summary(self, df): 

        summary_list = []

        if "df" not in st.session_state:
            st.session_state.df = df

        for col in st.session_state.df.columns:
            col_data = st.session_state.df[col]
            
            # Basic stats that work for all columns
            count = len(col_data)
            missing = col_data.isnull().sum()
            distinct = col_data.nunique()
            duplicates = count - distinct
            
            # Check if this looks like a date/time column
            is_date_column = False
            date_series = None
            
            # Try to detect date columns
            if col_data.dtype == 'object':
                # For object columns, try to convert to datetime
                date_series = pd.to_datetime(col_data, errors='coerce')
                if date_series.notna().sum() > count * 0.5:  # More than 50% convert to dates
                    is_date_column = True
                    working_series = date_series
                else:
                    working_series = pd.to_numeric(col_data, errors='coerce')
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                # Already a datetime column
                is_date_column = True
                working_series = col_data
            else:
                # Numeric column
                working_series = pd.to_numeric(col_data, errors='coerce')
            
            # Calculate stats based on type
            if is_date_column and working_series.notna().sum() > 0:
                # DATE/TIME STATISTICS
                valid_dates = working_series.dropna()
                if len(valid_dates) > 0:
                    max_date = valid_dates.max()
                    min_date = valid_dates.min()
                    
                    # Check if times have time components (not just 00:00:00)
                    has_time_component = (valid_dates.dt.hour != 0).any() or (valid_dates.dt.minute != 0).any()
                    
                    if has_time_component:
                        # Has time component - format with time
                        max_str = max_date.strftime('%Y-%m-%d %H:%M:%S')
                        min_str = min_date.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # Date only - format without time
                        max_str = max_date.strftime('%Y-%m-%d')
                        min_str = min_date.strftime('%Y-%m-%d')
                    
                    range_days = (max_date - min_date).days
                    
                    # Calculate quantiles for dates
                    quantiles = valid_dates.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
                    
                    # Format quantiles based on whether there's time component
                    if has_time_component:
                        q05 = quantiles.get(0.05, pd.NaT).strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(quantiles.get(0.05, pd.NaT)) else np.nan
                        q25 = quantiles.get(0.25, pd.NaT).strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(quantiles.get(0.25, pd.NaT)) else np.nan
                        q50 = quantiles.get(0.5, pd.NaT).strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(quantiles.get(0.5, pd.NaT)) else np.nan
                        q75 = quantiles.get(0.75, pd.NaT).strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(quantiles.get(0.75, pd.NaT)) else np.nan
                        q95 = quantiles.get(0.95, pd.NaT).strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(quantiles.get(0.95, pd.NaT)) else np.nan
                    else:
                        q05 = quantiles.get(0.05, pd.NaT).strftime('%Y-%m-%d') if not pd.isna(quantiles.get(0.05, pd.NaT)) else np.nan
                        q25 = quantiles.get(0.25, pd.NaT).strftime('%Y-%m-%d') if not pd.isna(quantiles.get(0.25, pd.NaT)) else np.nan
                        q50 = quantiles.get(0.5, pd.NaT).strftime('%Y-%m-%d') if not pd.isna(quantiles.get(0.5, pd.NaT)) else np.nan
                        q75 = quantiles.get(0.75, pd.NaT).strftime('%Y-%m-%d') if not pd.isna(quantiles.get(0.75, pd.NaT)) else np.nan
                        q95 = quantiles.get(0.95, pd.NaT).strftime('%Y-%m-%d') if not pd.isna(quantiles.get(0.95, pd.NaT)) else np.nan
                    
                    # Calculate stats on year component for dates, hour component for times
                    if has_time_component:
                        # For times, use hour + minute/60 as decimal
                        hours = valid_dates.dt.hour + valid_dates.dt.minute / 60.0
                        avg = hours.mean()
                        std = hours.std()
                        var = hours.var()
                        kurt = hours.kurtosis()
                        skew = hours.skew()
                    else:
                        # For dates only, use year
                        years = valid_dates.dt.year
                        avg = years.mean()
                        std = years.std()
                        var = years.var()
                        kurt = years.kurtosis()
                        skew = years.skew()
                    
                    total_sum = np.nan
                    
                    # IQR calculation
                    q75_date = quantiles.get(0.75, pd.NaT)
                    q25_date = quantiles.get(0.25, pd.NaT)
                    if has_time_component:
                        iqr = (q75_date - q25_date).total_seconds() / 3600.0 if not pd.isna(q75_date) and not pd.isna(q25_date) else np.nan  # hours
                    else:
                        iqr = (q75_date - q25_date).days if not pd.isna(q75_date) and not pd.isna(q25_date) else np.nan  # days
                    
                    if has_time_component:
                        col_type = 'datetime64[ns] (with time)'
                    else:
                        col_type = 'datetime64[ns]'
                else:
                    # No valid dates
                    max_str = min_str = q05 = q25 = q50 = q75 = q95 = np.nan
                    range_days = iqr = avg = std = var = kurt = skew = total_sum = np.nan
                    col_type = 'object (date-like)'
            
            else:
                # NUMERIC STATISTICS
                numeric_series = working_series
                valid_numeric = numeric_series.notna().sum()
                
                if valid_numeric > 0:
                    # Calculate numeric stats
                    maximum = numeric_series.max()
                    minimum = numeric_series.min()
                    rng = maximum - minimum
                    
                    quantiles = numeric_series.quantile([0.05, 0.25, 0.5, 0.75, 0.95])
                    q05 = quantiles.get(0.05, np.nan)
                    q25 = quantiles.get(0.25, np.nan)
                    q50 = quantiles.get(0.5, np.nan)
                    q75 = quantiles.get(0.75, np.nan)
                    q95 = quantiles.get(0.95, np.nan)
                    
                    iqr = q75 - q25
                    avg = numeric_series.mean()
                    std = numeric_series.std()
                    var = numeric_series.var()
                    kurt = numeric_series.kurtosis()
                    skew = numeric_series.skew()
                    total_sum = numeric_series.sum()
                    
                    # Format for display
                    max_str = maximum
                    min_str = minimum
                    range_days = rng
                    col_type = str(col_data.dtype)
                else:
                    max_str = min_str = q05 = q25 = q50 = q75 = q95 = np.nan
                    range_days = iqr = avg = std = var = kurt = skew = total_sum = np.nan
                    col_type = str(col_data.dtype)
            
            # Add to summary
            summary_list.append({
                'column': col,
                'type': col_type,
                'count': count,
                'missing': missing,
                'distinct': distinct,
                'duplicates': duplicates,
                'zeroes': (col_data == 0).sum() if pd.api.types.is_numeric_dtype(col_data) else 0,
                'max': max_str,
                'min': min_str,
                'q05': q05,
                'q25': q25,
                'q50': q50,
                'q75': q75,
                'q95': q95,
                'rng': range_days,
                'iqr': iqr,
                'avg': avg,
                'std': std,
                'var': var,
                'kurt': kurt,
                'skew': skew,
                'sum': total_sum,
                'is_date': is_date_column
            })
        
        return summary_list

    def feature_bar_chart(self, feature_stats):
        stats_order = ['min', '5%', 'q1', 'median', 'avg', 'q3', '95%', 'max']
        # Extract values ensuring keys exist
        values = [feature_stats.get(stat, np.nan) for stat in stats_order]
        
        fig = go.Figure(go.Bar(x=stats_order, y=values, marker_color='steelblue'))
        fig.update_layout(
            title=f"Descriptive Stats: {feature_stats['column']}",
            margin=dict(t=30, b=10, l=10, r=10),
            height=300
        )
        return fig

    def display_summary_in_columns(self, df):

        if "df" not in st.session_state:
            st.session_state.df = df

        def safe_format(value):
            if pd.isna(value):
                return "N/A"
            try:
                # Try to format as float
                return f"{float(value):.2f}"
            except (ValueError, TypeError):
                # If it fails, return as string (for dates)
                return str(value)
        
        
        #st.markdown("<hr>", unsafe_allow_html=True)

        st.markdown(f"<h4>Feature Level Statistics Report</h4>", unsafe_allow_html=True)
        
        #st.markdown("<hr>", unsafe_allow_html=True)

        summary_list = self.detailed_dataframe_summary(st.session_state.df)


        # Now use safe_format for ALL your display lines
        for feature_data in summary_list:
            cols = st.columns(5)
            col1, col2, col3, col4, col5 = cols

            # col1
            col1.markdown("<br>", unsafe_allow_html=True)
            col1.markdown(f" **:green[{feature_data['column']}]**")
            col1.text(f"{'type:':<12} {feature_data['type']}")
            col1.text(f"{'count:':<12} {feature_data['count']}")
            col1.text(f"{'missing:':<12} {feature_data['missing']}")
            col1.text(f"{'duplicates:':<12} {feature_data['duplicates']}")
            col1.text(f"{'distinct:':<12} {feature_data['distinct']}")
            col1.text(f"{'zeroes:':<12} {feature_data['zeroes']}")

            # col2 - USE safe_format
            #col2.markdown("<br>", unsafe_allow_html=True)
            col2.markdown("<br>" * 2, unsafe_allow_html=True)
            col2.text(f"{'max:':<12} {safe_format(feature_data.get('max', 'N/A'))}")
            col2.text(f"{'95%:':<12} {safe_format(feature_data.get('q95', 'N/A'))}")
            col2.text(f"{'q3:':<12} {safe_format(feature_data.get('q75', 'N/A'))}")
            col2.text(f"{'median:':<12} {safe_format(feature_data.get('q50', 'N/A'))}")

            # col3 - USE safe_format
            #col3.markdown("<br>", unsafe_allow_html=True)
            col3.markdown("<br>" * 2, unsafe_allow_html=True)
            col3.text(f"{'avg:':<12} {safe_format(feature_data.get('avg', 'N/A'))}")
            col3.text(f"{'q1:':<12} {safe_format(feature_data.get('q25', 'N/A'))}")
            col3.text(f"{'5%:':<12} {safe_format(feature_data.get('q05', 'N/A'))}")
            col3.text(f"{'min:':<12} {safe_format(feature_data.get('min', 'N/A'))}")

            # col4 - USE safe_format
            col4.markdown("<br>" * 2, unsafe_allow_html=True)
            col4.text(f"{'range:':<12} {safe_format(feature_data.get('rng', 'N/A'))}")
            col4.text(f"{'iqr:':<12} {safe_format(feature_data.get('iqr', 'N/A'))}")
            col4.text(f"{'std:':<12} {safe_format(feature_data.get('std', 'N/A'))}")
            col4.text(f"{'var:':<12} {safe_format(feature_data.get('var', 'N/A'))}")
            col4.text(f"{'kurt:':<12} {safe_format(feature_data.get('kurt', 'N/A'))}")
            col4.text(f"{'skew:':<12} {safe_format(feature_data.get('skew', 'N/A'))}")
            col4.text(f"{'sum:':<12} {safe_format(feature_data.get('sum', 'N/A'))}")

            # col4 - bar chart of descriptive stats
            fig = self.feature_bar_chart(feature_data)
            col5.markdown("<br>" * 2, unsafe_allow_html=True)
            # Add unique key to prevent duplicate element ID errors
            col5.plotly_chart(fig, width='stretch', key=f"chart_{feature_data['column']}_{id(df)}")

            st.markdown("---")


    

    def single_dataframe_summary(self, df):
        if "df" not in st.session_state:
            st.session_state.df = df

        for col in st.session_state.df.columns:
            col_data = st.session_state.df[col]
        num_rows, num_features = st.session_state.df.shape
        num_rows = num_rows
        num_duplicates = st.session_state.df.duplicated().sum()
        ram_usage_kb = st.session_state.df.memory_usage(deep=True).sum() / 1024
        num_features = num_features

        categorical_cols = st.session_state.df.select_dtypes(include=['category', 'object']).columns
        numerical_cols = st.session_state.df.select_dtypes(include=[np.number]).columns
        text_cols = [col for col in categorical_cols if st.session_state.df[col].astype(str).str.len().max() > 20]

        group_summary_df = pd.DataFrame(self.detailed_dataframe_summary(st.session_state.df))

        overall_summary = pd.DataFrame({
            'Metric': ['ROWS', 'DUPLICATE ROWS', 'RAM (KB)', 'FEATURES', 'CATEGORICAL', 'NUMERICAL', 'TEXT'],
            'Value': [
                num_rows,
                num_duplicates,
                round(ram_usage_kb, 1),
                num_features,
                len(categorical_cols) - len(text_cols),
                len(numerical_cols),
                len(text_cols)
            ]
        })

        # Layout: col1 (25%), col2 (75%)
        col1, col2 = st.columns([1, 3])

        with col1:
            #st.subheader("Overall Summary")
            st.markdown(f"<h5>DataFrame Statistics Report</h5>", unsafe_allow_html=True)
            st.dataframe(overall_summary, width='stretch')

        with col2:
            #st.subheader("Group Summary")
            st.markdown(f"<h5>Combined Statistics Report</h5>", unsafe_allow_html=True)
            st.dataframe(group_summary_df, width='stretch')

        return overall_summary, group_summary_df

    



