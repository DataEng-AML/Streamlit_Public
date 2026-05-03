
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Optional, Dict, Any, Tuple

from timeseries_evaluation_agent import TimeSeriesEvaluationAgent


class TimeSeriesEvaluationWrapper:
    """Wrapper class connecting core agent with Streamlit"""
    
    def __init__(self):
        self.agent = TimeSeriesEvaluationAgent()
        self._initialize_session_state()
    
    def _initialize_session_state(self):
        if 'timeseries_df' not in st.session_state:
            st.session_state.timeseries_df = None
        if 'active_index_col' not in st.session_state:
            st.session_state.active_index_col = None
        if 'change_point_results' not in st.session_state:
            st.session_state.change_point_results = None
        if 'protocol_audit_results' not in st.session_state:
            st.session_state.protocol_audit_results = None
        if 'decomposition_results' not in st.session_state:
            st.session_state.decomposition_results = None
        if 'stationarity_results' not in st.session_state:
            st.session_state.stationarity_results = None
        if 'forecast_results' not in st.session_state:
            st.session_state.forecast_results = None
    
    def get_current_dataframe(self, column_name: str) -> Optional[pd.DataFrame]:
        df = st.session_state.get('timeseries_df')
        if df is None or not isinstance(df.index, pd.DatetimeIndex):
            return None
        return df.dropna()

    def get_current_series(self, column_name: str) -> Optional[pd.Series]:
        df = st.session_state.get('timeseries_df')
        if df is None or not isinstance(df.index, pd.DatetimeIndex):
            return None
        if column_name not in df.columns:
            return None
        return df[column_name].dropna()
        
    def run_change_point_detection(self, column_name: str, model_type: str = "l2",
                                   min_segment_size: int = 10, penalty_factor: float = 2.0,
                                   max_change_points: int = 50) -> Dict[str, Any]:
        series = self.get_current_series(column_name)
        if series is None:
            return {'error': 'No valid time series data'}
        try:
            result = self.agent.detect_change_points(
                series=series, model_type=model_type, min_segment_size=min_segment_size,
                penalty_factor=penalty_factor, max_change_points=max_change_points
            )
            st.session_state.change_point_results = result
            return {
                'success': True, 'num_breaks': result.num_breaks,
                'segment_stats': result.segment_stats, 'change_points': result.change_points
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_protocol_audit(self, column_name: str, window_days: int = 14,
                           overlap_pct: float = 50, detect_encodings: bool = True) -> Dict[str, Any]:
        series = self.get_current_series(column_name)
        if series is None:
            return {'error': 'No valid time series data'}
        try:
            result = self.agent.audit_protocol_changes(
                series=series, window_days=window_days, overlap_pct=overlap_pct,
                detect_encodings=detect_encodings
            )
            st.session_state.protocol_audit_results = result
            return {
                'success': True, 'windows_analyzed': result.windows_analyzed,
                'has_property_changes': result.has_property_changes,
                'has_encoding_changes': result.has_encoding_changes,
                'property_changes': result.property_changes,
                'encoding_changes': result.encoding_changes,
                'window_stats': result.window_stats
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_decomposition(self, column_name: str, period: int = 7,
                          model_type: str = "additive") -> Dict[str, Any]:
        series = self.get_current_series(column_name)
        if series is None:
            return {'error': 'No valid time series data'}
        try:
            result = self.agent.decompose_series(series=series, period=period, model_type=model_type)
            st.session_state.decomposition_results = result
            return {
                'success': True, 'trend_strength': result.trend_strength,
                'seasonal_strength': result.seasonal_strength,
                'anomalies_count': result.anomalies_count, 'anomalies_df': result.anomalies_df,
                'decomposition_components': result.decomposition_components
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_stationarity_test(self, column_name: str, diff_level: int = 0) -> Dict[str, Any]:
        series = self.get_current_series(column_name)
        if series is None:
            return {'error': 'No valid time series data'}
        try:
            result = self.agent.test_stationarity(series=series, diff_level=diff_level)
            st.session_state.stationarity_results = result
            return {
                'success': True, 'adf_pvalue': result.adf_pvalue,
                'adf_stationary': result.adf_is_stationary,
                'kpss_pvalue': result.kpss_pvalue, 'kpss_stationary': result.kpss_is_stationary,
                'verdict': result.final_verdict
            }
        except Exception as e:
            return {'error': str(e)}
    
    def run_forecast(self, column_name: str, steps: int = 10,
                     method: str = "Exponential Smoothing", train_pct: float = 80,
                     confidence_level: float = 95, **kwargs) -> Dict[str, Any]:
        series = self.get_current_series(column_name)
        if series is None:
            return {'error': 'No valid time series data'}
        try:
            result = self.agent.forecast(
                series=series, steps=steps, method=method, train_pct=train_pct,
                confidence_level=confidence_level, **kwargs
            )
            st.session_state.forecast_results = result
            return {
                'success': True, 'forecast_values': result.forecast_values,
                'confidence_interval': result.confidence_interval, 'method': result.method,
                'mae': result.mae, 'rmse': result.rmse
            }
        except Exception as e:
            return {'error': str(e)}
    
    def clear_all_results(self):
        st.session_state.change_point_results = None
        st.session_state.protocol_audit_results = None
        st.session_state.decomposition_results = None
        st.session_state.stationarity_results = None
        st.session_state.forecast_results = None
    
    def get_formatted_change_point_plot(self, column_name: str,
                                        figsize: Tuple[int, int] = (14, 5)) -> Optional[plt.Figure]:
        series = self.get_current_series(column_name)
        results = st.session_state.get('change_point_results')
        if series is None or results is None:
            return None
        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(series.index, series.values, 'b-', alpha=0.7, linewidth=1)
        for cp in results.change_points:
            if 0 < cp < len(series):
                ax.axvline(x=series.index[cp], color='r', linestyle='--', alpha=0.7, linewidth=1)
        ax.set_title(f'Change Point Detection - {column_name} ({results.num_breaks} breaks)')
        ax.set_xlabel('Time')
        ax.set_ylabel(column_name)
        plt.xticks(rotation=45)
        plt.tight_layout()
        return fig


_wrapper_instance = None

def get_timeseries_wrapper() -> TimeSeriesEvaluationWrapper:
    global _wrapper_instance
    if _wrapper_instance is None:
        _wrapper_instance = TimeSeriesEvaluationWrapper()
    return _wrapper_instance