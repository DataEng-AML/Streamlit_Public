import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass
import warnings
warnings.filterwarnings('ignore')


@dataclass
class ChangePointResult:
    """Container for change point detection results"""
    change_points: list
    segments: list
    num_breaks: int
    segment_stats: pd.DataFrame


@dataclass
class ProtocolAuditResult:
    """Container for protocol audit results"""
    windows_analyzed: int
    property_changes: list
    encoding_changes: list
    window_stats: pd.DataFrame
    has_property_changes: bool
    has_encoding_changes: bool


@dataclass
class DecompositionResult:
    """Container for decomposition results"""
    trend_strength: float
    seasonal_strength: float
    anomalies_count: int
    anomalies_df: pd.DataFrame
    decomposition_components: Dict[str, np.ndarray]


@dataclass
class StationarityResult:
    """Container for stationarity test results"""
    adf_statistic: float
    adf_pvalue: float
    adf_is_stationary: bool
    kpss_statistic: Optional[float]
    kpss_pvalue: Optional[float]
    kpss_is_stationary: Optional[bool]
    final_verdict: str
    acf_values: Optional[np.ndarray]
    pacf_values: Optional[np.ndarray]


@dataclass
class ForecastResult:
    """Container for forecasting results"""
    forecast_values: np.ndarray
    confidence_interval: float
    method: str
    mae: Optional[float]
    rmse: Optional[float]
    train_data: pd.Series
    test_data: Optional[pd.Series]


class TimeSeriesEvaluationAgent:
    """
    Core time series analysis engine
    No Streamlit dependencies - pure analysis logic
    """
    
    def __init__(self):
        self.results_cache = {}
    
    def detect_change_points(
        self, 
        series: pd.Series, 
        model_type: str = "l2",
        min_segment_size: int = 10,
        penalty_factor: float = 2.0,
        max_change_points: int = 50
    ) -> ChangePointResult:
        """Detect change points in time series"""
        try:
            import ruptures as rpt
        except ImportError:
            raise ImportError("pip install ruptures")
        
        series_clean = series.dropna()
        if len(series_clean) < 50:
            raise ValueError(f"Need at least 50 data points. Got {len(series_clean)}")
        
        signal = series_clean.values
        algo = rpt.Pelt(model=model_type, min_size=min_segment_size).fit(signal)
        penalty = np.std(signal) * penalty_factor
        change_points = algo.predict(pen=penalty)
        change_points = [cp for cp in change_points if 0 < cp < len(signal)]
        
        if len(change_points) > max_change_points:
            step = len(change_points) // max_change_points
            change_points = change_points[::step][:max_change_points]
        
        segments = []
        start_idx = 0
        for i, cp in enumerate(change_points):
            end_idx = cp
            segment_vals = signal[start_idx:end_idx]
            if len(segment_vals) > 0:
                segments.append({
                    'segment': i + 1,
                    'start': series_clean.index[start_idx],
                    'end': series_clean.index[end_idx - 1] if end_idx > 0 else series_clean.index[start_idx],
                    'mean': np.mean(segment_vals),
                    'std': np.std(segment_vals),
                    'samples': len(segment_vals)
                })
            start_idx = cp
        
        if start_idx < len(signal):
            segment_vals = signal[start_idx:]
            segments.append({
                'segment': len(change_points) + 1,
                'start': series_clean.index[start_idx],
                'end': series_clean.index[-1],
                'mean': np.mean(segment_vals),
                'std': np.std(segment_vals),
                'samples': len(segment_vals)
            })
        
        segment_stats = pd.DataFrame(segments)
        
        return ChangePointResult(
            change_points=change_points,
            segments=segments,
            num_breaks=len(change_points),
            segment_stats=segment_stats
        )
    
    def audit_protocol_changes(
        self,
        series: pd.Series,
        window_days: int = 14,
        overlap_pct: float = 50,
        detect_encodings: bool = True,
        mean_change_threshold: float = 30,
        std_change_threshold: float = 50,
        range_ratio_threshold: float = 2.0
    ) -> ProtocolAuditResult:
        """Audit protocol changes in time series"""
        from scipy import stats
        
        series_clean = series.dropna()
        start_date = series_clean.index.min()
        end_date = series_clean.index.max()
        
        step_days = window_days * (1 - overlap_pct / 100)
        windows = []
        current_start = start_date
        
        while current_start <= end_date:
            window_end = current_start + pd.Timedelta(days=window_days)
            if window_end > end_date:
                window_end = end_date
            windows.append((current_start, window_end))
            current_start += pd.Timedelta(days=step_days)
        
        results = []
        property_changes = []
        encoding_changes = []
        
        for i, (win_start, win_end) in enumerate(windows):
            window_data = series_clean[(series_clean.index >= win_start) & (series_clean.index < win_end)]
            if len(window_data) < 10:
                continue
            
            vals = window_data.values
            unique_vals = np.unique(vals)
            
            stats_dict = {
                'window_start': win_start,
                'window_end': win_end,
                'days': (win_end - win_start).days,
                'count': len(vals),
                'mean': np.mean(vals),
                'std': np.std(vals),
                'min': np.min(vals),
                'max': np.max(vals),
                'median': np.median(vals),
                'q1': np.percentile(vals, 25),
                'q3': np.percentile(vals, 75),
                'iqr': np.percentile(vals, 75) - np.percentile(vals, 25),
                'unique_values': len(unique_vals),
                'zeros_pct': (vals == 0).mean() * 100,
                'negatives_pct': (vals < 0).mean() * 100 if np.any(vals < 0) else 0,
                'cv': np.std(vals) / np.mean(vals) if np.mean(vals) != 0 else 0,
                'skewness': stats.skew(vals) if len(vals) > 2 else 0,
                'kurtosis': stats.kurtosis(vals) if len(vals) > 3 else 0
            }
            
            if i > 0 and results:
                prev = results[-1]
                
                mean_change_pct = abs(stats_dict['mean'] - prev['mean']) / prev['mean'] * 100 if prev['mean'] != 0 else 0
                if mean_change_pct > mean_change_threshold:
                    property_changes.append({
                        'window': i,
                        'date': win_start,
                        'change': f"Mean {'↑' if stats_dict['mean'] > prev['mean'] else '↓'} {mean_change_pct:.0f}%",
                        'type': 'mean_shift'
                    })
                
                std_change_pct = abs(stats_dict['std'] - prev['std']) / prev['std'] * 100 if prev['std'] != 0 else 0
                if std_change_pct > std_change_threshold:
                    property_changes.append({
                        'window': i,
                        'date': win_start,
                        'change': f"Std {'↑' if stats_dict['std'] > prev['std'] else '↓'} {std_change_pct:.0f}%",
                        'type': 'std_shift'
                    })
                
                current_range = stats_dict['max'] - stats_dict['min']
                prev_range = prev['max'] - prev['min']
                if prev_range > 0:
                    range_ratio = current_range / prev_range
                    if range_ratio > range_ratio_threshold:
                        property_changes.append({
                            'window': i,
                            'date': win_start,
                            'change': f"Range expanded {range_ratio:.1f}x",
                            'type': 'range_expansion'
                        })
                    elif range_ratio < 1 / range_ratio_threshold:
                        property_changes.append({
                            'window': i,
                            'date': win_start,
                            'change': f"Range contracted {range_ratio:.1f}x",
                            'type': 'range_contraction'
                        })
            
            if detect_encodings and len(unique_vals) > 10 and i > 0 and results:
                prev = results[-1]
                is_integer = np.all(vals == vals.astype(int))
                stats_dict['is_integer'] = is_integer
                
                if 'is_integer' in prev and prev['is_integer'] != is_integer:
                    encoding_changes.append({
                        'window': i,
                        'date': win_start,
                        'change': f"Encoding: {'Integer→Float' if not is_integer else 'Float→Integer'}",
                        'type': 'type_conversion'
                    })
                
                unique_ratio = stats_dict['unique_values'] / prev['unique_values'] if prev['unique_values'] > 0 else 1
                if unique_ratio > 3:
                    encoding_changes.append({
                        'window': i,
                        'date': win_start,
                        'change': f"Precision increased: {stats_dict['unique_values']} vs {prev['unique_values']}",
                        'type': 'precision_increase'
                    })
                elif unique_ratio < 0.33:
                    encoding_changes.append({
                        'window': i,
                        'date': win_start,
                        'change': f"Precision decreased: {stats_dict['unique_values']} vs {prev['unique_values']}",
                        'type': 'precision_decrease'
                    })
            
            results.append(stats_dict)
        
        window_stats = pd.DataFrame(results)
        
        return ProtocolAuditResult(
            windows_analyzed=len(results),
            property_changes=property_changes,
            encoding_changes=encoding_changes,
            window_stats=window_stats,
            has_property_changes=len(property_changes) > 0,
            has_encoding_changes=len(encoding_changes) > 0
        )
    
    def decompose_series(
        self,
        series: pd.Series,
        period: int = 7,
        model_type: str = "additive"
    ) -> DecompositionResult:
        """Decompose time series into trend, seasonal, and residual components"""
        try:
            from statsmodels.tsa.seasonal import seasonal_decompose
        except ImportError:
            raise ImportError("pip install statsmodels")
        
        series_clean = series.dropna()
        if len(series_clean) < period * 2:
            raise ValueError(f"Need at least {period * 2} data points")
        
        decomposition = seasonal_decompose(series_clean, model=model_type, period=period)
        
        trend_vals = decomposition.trend.dropna()
        seasonal_vals = decomposition.seasonal.dropna()
        resid_vals = decomposition.resid.dropna()
        
        trend_strength = 1 - np.var(resid_vals) / np.var(trend_vals) if len(trend_vals) > 0 and np.var(trend_vals) > 0 else 0
        seasonal_strength = 1 - np.var(resid_vals) / np.var(seasonal_vals) if len(seasonal_vals) > 0 and np.var(seasonal_vals) > 0 else 0
        
        resid_mean = resid_vals.mean()
        resid_std = resid_vals.std()
        anomalies = resid_vals[np.abs(resid_vals - resid_mean) > 3 * resid_std]
        
        anomalies_df = pd.DataFrame({
            'timestamp': anomalies.index,
            'residual_value': anomalies.values,
            'deviation_sigma': np.abs((anomalies.values - resid_mean) / resid_std)
        })
        
        return DecompositionResult(
            trend_strength=trend_strength,
            seasonal_strength=seasonal_strength,
            anomalies_count=len(anomalies),
            anomalies_df=anomalies_df,
            decomposition_components={
                'trend': decomposition.trend.values,
                'seasonal': decomposition.seasonal.values,
                'residual': decomposition.resid.values,
                'original': series_clean.values,
                'timestamps': series_clean.index
            }
        )
    
    def test_stationarity(
        self,
        series: pd.Series,
        diff_level: int = 0
    ) -> StationarityResult:
        """Test stationarity using ADF and KPSS tests"""
        try:
            from statsmodels.tsa.stattools import adfuller, kpss
        except ImportError:
            raise ImportError("pip install statsmodels")
        
        series_clean = series.dropna()
        if diff_level > 0:
            series_clean = series_clean.diff(diff_level).dropna()
        
        if len(series_clean) < 10:
            raise ValueError(f"Not enough data. Got {len(series_clean)}")
        
        adf_result = adfuller(series_clean, autolag='AIC')
        adf_stationary = adf_result[1] < 0.05
        
        try:
            kpss_result = kpss(series_clean, regression='c', nlags='auto')
            kpss_stat = kpss_result[0]
            kpss_pvalue = kpss_result[1]
            kpss_stationary = kpss_pvalue > 0.05
        except Exception:
            kpss_stat = None
            kpss_pvalue = None
            kpss_stationary = None
        
        if kpss_stationary is not None:
            if adf_stationary and kpss_stationary:
                verdict = "stationary"
            elif not adf_stationary and not kpss_stationary:
                verdict = "non_stationary"
            else:
                verdict = "ambiguous"
        else:
            verdict = "stationary" if adf_stationary else "non_stationary"
        
        from statsmodels.tsa.stattools import acf, pacf
        n_lags = min(40, len(series_clean) // 2)
        acf_values = acf(series_clean, nlags=n_lags)
        pacf_values = pacf(series_clean, nlags=n_lags, method='ywm')
        
        return StationarityResult(
            adf_statistic=adf_result[0],
            adf_pvalue=adf_result[1],
            adf_is_stationary=adf_stationary,
            kpss_statistic=kpss_stat,
            kpss_pvalue=kpss_pvalue,
            kpss_is_stationary=kpss_stationary,
            final_verdict=verdict,
            acf_values=acf_values,
            pacf_values=pacf_values
        )
    
    def forecast(
        self,
        series: pd.Series,
        steps: int = 10,
        method: str = "Exponential Smoothing",
        train_pct: float = 80,
        confidence_level: float = 95,
        **kwargs
    ) -> ForecastResult:
        """Generate forecast for time series"""
        series_clean = series.dropna()
        train_size = int(len(series_clean) * train_pct / 100)
        train = series_clean.iloc[:train_size]
        test = series_clean.iloc[train_size:train_size + steps] if len(series_clean) > train_size else None
        
        if method == "Simple Moving Average":
            window = kwargs.get('window', 5)
            last_values = train.iloc[-window:]
            forecast_values = np.array([last_values.mean()] * steps)
            forecast_std = last_values.std()
            
        elif method == "Exponential Smoothing":
            try:
                from statsmodels.tsa.holtwinters import ExponentialSmoothing
                trend = kwargs.get('trend', None)
                seasonal = kwargs.get('seasonal', None)
                seasonal_periods = kwargs.get('seasonal_periods', 7)
                
                if seasonal and seasonal_periods > len(train) // 2:
                    seasonal = None
                
                model = ExponentialSmoothing(
                    train,
                    trend=trend,
                    seasonal=seasonal,
                    seasonal_periods=seasonal_periods if seasonal else None
                )
                fitted_model = model.fit()
                forecast_values = fitted_model.forecast(steps).values
                residuals = fitted_model.resid.dropna()
                forecast_std = np.std(residuals) if len(residuals) > 0 else train.std()
            except Exception:
                forecast_values = np.array([train.iloc[-1]] * steps)
                forecast_std = train.iloc[-10:].std() if len(train) >= 10 else train.std()
        
        else:  # Naive
            forecast_values = np.array([train.iloc[-1]] * steps)
            forecast_std = train.iloc[-10:].std() if len(train) >= 10 else train.std()
        
        from scipy import stats as scipy_stats
        z_score = scipy_stats.norm.ppf(1 - (100 - confidence_level) / 200)
        ci = z_score * forecast_std
        
        mae = None
        rmse = None
        if test is not None and len(test) == len(forecast_values):
            mae = np.mean(np.abs(test.values - forecast_values))
            rmse = np.sqrt(np.mean((test.values - forecast_values)**2))
        
        return ForecastResult(
            forecast_values=forecast_values,
            confidence_interval=ci,
            method=method,
            mae=mae,
            rmse=rmse,
            train_data=train,
            test_data=test
        )
    
    def clear_cache(self):
        self.results_cache = {}