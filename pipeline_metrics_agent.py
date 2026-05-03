"""
ELT-Bench Style Evaluation Framework for DataEng-AML Pipeline
Implements the 4 core metrics for comparing data pipelines
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import json
from dataclasses import dataclass
from copy import deepcopy
import traceback
import re
import sys

@dataclass
class PipelineMetrics:
    """Container for all pipeline evaluation metrics"""
    success_rate: float = 0.0
    data_quality_score: float = 0.0
    execution_time: float = 0.0
    error_recovery_rate: float = 0.0
    details: Dict[str, Any] = None

class PipelineEvaluator:
    """Comprehensive evaluator for data pipeline metrics"""
    
    def __init__(self, ground_truth_data: Dict[str, pd.DataFrame] = None):
        """
        Initialize evaluator with optional ground truth data.
        
        Args:
            ground_truth_data: Dict mapping test_case_name -> expected_dataframe
        """
        self.ground_truth = ground_truth_data or {}
        self.metrics_history = []
        
    # -----------------------------------------------------------------
    # METRIC 1: Pipeline Success Rate
    # -----------------------------------------------------------------
    
    def calculate_success_rate(self, 
                               pipeline_func: callable,
                               test_cases: List[Tuple[str, pd.DataFrame, pd.DataFrame]]) -> Dict[str, Any]:
        """
        Measure if pipeline produces correct output (ELT-Bench's primary metric).
        
        Args:
            pipeline_func: Function that processes data (your pipeline)
            test_cases: List of (test_name, input_data, expected_output)
            
        Returns:
            Dictionary with success rate and detailed results
        """
        results = []
        detailed_results = []
        
        for test_name, input_data, expected_output in test_cases:
            start_time = time.perf_counter()
            
            try:
                # Run the pipeline
                # Note: Adjust based on your pipeline's actual interface
                if hasattr(pipeline_func, 'detect_missing_data') and hasattr(pipeline_func, 'fix_missing_data'):
                    # For PipelineCrewAIPyDEAML style
                    processed = pipeline_func.detect_missing_data(input_data.copy())
                    processed = pipeline_func.fix_missing_data(processed)
                else:
                    # Generic pipeline function
                    processed = pipeline_func(input_data.copy())
                
                execution_time = time.perf_counter() - start_time
                
                # Compare with expected output
                success = self._compare_dataframes(processed, expected_output)
                
                results.append({
                    'test_case': test_name,
                    'success': success,
                    'execution_time': execution_time,
                    'rows_processed': len(input_data),
                    'columns_processed': len(input_data.columns)
                })
                
                detailed_results.append({
                    'test_case': test_name,
                    'input_shape': input_data.shape,
                    'output_shape': processed.shape,
                    'expected_shape': expected_output.shape,
                    'columns_matched': list(processed.columns) == list(expected_output.columns),
                    'success': success
                })
                
            except Exception as e:
                execution_time = time.perf_counter() - start_time
                results.append({
                    'test_case': test_name,
                    'success': False,
                    'execution_time': execution_time,
                    'error': str(e),
                    'error_type': type(e).__name__
                })
        
        # Calculate overall success rate
        success_count = sum(1 for r in results if r.get('success', False))
        total_tests = len(results)
        success_rate = success_count / total_tests if total_tests > 0 else 0.0
        
        return {
            'metric_name': 'Pipeline Success Rate',
            'value': success_rate,
            'details': {
                'success_count': success_count,
                'total_tests': total_tests,
                'test_results': results,
                'detailed_comparisons': detailed_results
            }
        }
    
    def _compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                           tolerance: float = 1e-6) -> bool:
        """
        Robust dataframe comparison with tolerance for floating point differences.
        """
        try:
            # Check shapes
            if df1.shape != df2.shape:
                return False
            
            # Check column names
            if list(df1.columns) != list(df2.columns):
                return False
            
            # Check dtypes for numeric columns
            for col in df1.columns:
                if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
                    # Numeric comparison with tolerance
                    if not np.allclose(df1[col].fillna(0).values, 
                                      df2[col].fillna(0).values, 
                                      rtol=tolerance, 
                                      atol=tolerance,
                                      equal_nan=True):
                        return False
                else:
                    # String/object comparison
                    if not df1[col].fillna('').equals(df2[col].fillna('')):
                        return False
            
            return True
            
        except Exception:
            return False
    
    # -----------------------------------------------------------------
    # METRIC 2: Data Quality Score
    # -----------------------------------------------------------------
    
    def calculate_data_quality_score(self,
                                    original_data: pd.DataFrame,
                                    processed_data: pd.DataFrame,
                                    quality_dimensions: List[str] = None) -> Dict[str, Any]:
        """
        Calculate data quality improvement across multiple dimensions.
        
        Args:
            original_data: Raw input data
            processed_data: Pipeline output data
            quality_dimensions: List of dimensions to measure ['completeness', 'consistency', 'accuracy']
            
        Returns:
            Dictionary with overall score and dimension breakdown
        """
        if quality_dimensions is None:
            quality_dimensions = ['completeness', 'consistency', 'validity']
        
        dimensions_scores = {}
        details = {}
        
        # Dimension 1: Completeness (Missing Data Handling)
        if 'completeness' in quality_dimensions:
            completeness_score, completeness_details = self._measure_completeness(
                original_data, processed_data
            )
            dimensions_scores['completeness'] = completeness_score
            details['completeness'] = completeness_details
        
        # Dimension 2: Consistency (Standardization)
        if 'consistency' in quality_dimensions:
            consistency_score, consistency_details = self._measure_consistency(
                original_data, processed_data
            )
            dimensions_scores['consistency'] = consistency_score
            details['consistency'] = consistency_details
        
        # Dimension 3: Validity (Data Rules)
        if 'validity' in quality_dimensions:
            validity_score, validity_details = self._measure_validity(processed_data)
            dimensions_scores['validity'] = validity_score
            details['validity'] = validity_details
        
        # Overall score (weighted average)
        weights = {'completeness': 0.4, 'consistency': 0.4, 'validity': 0.2}
        overall_score = sum(
            dimensions_scores.get(dim, 0) * weights.get(dim, 1/len(dimensions_scores))
            for dim in dimensions_scores
        )
        
        return {
            'metric_name': 'Data Quality Score',
            'value': overall_score,
            'dimension_scores': dimensions_scores,
            'details': details
        }
    
    def _measure_completeness(self, original: pd.DataFrame, 
                             processed: pd.DataFrame) -> Tuple[float, Dict]:
        """Measure reduction in missing values"""
        original_missing = original.isna().sum().sum()
        processed_missing = processed.isna().sum().sum()
        total_cells = original.size
        
        if total_cells == 0:
            return 0.0, {'error': 'Empty dataframe'}
        
        missing_reduction = (original_missing - processed_missing) / original_missing if original_missing > 0 else 1.0
        
        # Score: 1.0 means all missing values handled, 0.0 means no improvement
        score = max(0.0, min(1.0, missing_reduction))
        
        details = {
            'original_missing_count': int(original_missing),
            'processed_missing_count': int(processed_missing),
            'reduction_percentage': float(missing_reduction * 100),
            'total_cells': int(total_cells)
        }
        
        return score, details
    
    def _measure_consistency(self, original: pd.DataFrame, 
                            processed: pd.DataFrame) -> Tuple[float, Dict]:
        """Measure column name and format standardization"""
        # Check if column names follow consistent pattern (e.g., lowercase, underscores)
        processed_cols = [str(col).strip().lower() for col in processed.columns]
        
        # Score based on naming consistency
        naming_consistency = self._calculate_naming_consistency(processed_cols)
        
        # Check data type consistency per column
        dtype_consistency = self._calculate_dtype_consistency(processed)
        
        # Combined consistency score
        consistency_score = (naming_consistency * 0.6) + (dtype_consistency * 0.4)
        
        details = {
            'naming_consistency_score': naming_consistency,
            'dtype_consistency_score': dtype_consistency,
            'processed_columns': processed.columns.tolist(),
            'standardized_columns': processed_cols
        }
        
        return consistency_score, details
    
    def _calculate_naming_consistency(self, column_names: List[str]) -> float:
        """Calculate how consistent column naming is"""
        if not column_names:
            return 0.0
        
        # Check for special characters (except underscores)
        special_char_pattern = r'[^a-z0-9_]'
        has_special_chars = any(re.search(special_char_pattern, name) for name in column_names)
        
        # Check for spaces
        has_spaces = any(' ' in name for name in column_names)
        
        # Check for consistent case
        is_lowercase = all(name.islower() for name in column_names)
        
        # Score calculation
        score = 1.0
        if has_special_chars:
            score -= 0.3
        if has_spaces:
            score -= 0.3
        if not is_lowercase:
            score -= 0.2
        
        return max(0.0, score)
    
    def _calculate_dtype_consistency(self, df: pd.DataFrame) -> float:
        """Check if columns have consistent data types"""
        if df.empty:
            return 0.0
        
        consistency_score = 1.0
        
        for col in df.columns:
            # Check for mixed types in object columns
            if df[col].dtype == 'object':
                # Sample values to check type consistency
                sample = df[col].dropna().head(100)
                if len(sample) > 0:
                    types = set(type(val).__name__ for val in sample)
                    if len(types) > 1:
                        consistency_score -= 0.1  # Penalty for mixed types
        
        return max(0.0, consistency_score)
    
    def _measure_validity(self, data: pd.DataFrame) -> Tuple[float, Dict]:
        """Check if data follows basic validity rules"""
        if data.empty:
            return 0.0, {'error': 'Empty dataframe'}
        
        validity_checks = {
            'no_negative_amounts': 0,
            'dates_in_range': 0,
            'reasonable_string_lengths': 0,
            'categorical_values_valid': 0
        }
        
        total_checks = 0
        passed_checks = 0
        
        for col in data.columns:
            # Check 1: No negative values in amount columns
            if 'amount' in col.lower() or 'price' in col.lower() or 'cost' in col.lower():
                total_checks += 1
                if pd.api.types.is_numeric_dtype(data[col]):
                    if (data[col].dropna() >= 0).all():
                        passed_checks += 1
                        validity_checks['no_negative_amounts'] += 1
            
            # Check 2: Dates in reasonable range
            if 'date' in col.lower() or 'time' in col.lower():
                total_checks += 1
                if pd.api.types.is_datetime64_any_dtype(data[col]):
                    dates = data[col].dropna()
                    if len(dates) > 0:
                        min_date = dates.min()
                        max_date = dates.max()
                        # Check if dates are within last 10 years
                        if min_date.year >= 2014 and max_date.year <= 2024:
                            passed_checks += 1
                            validity_checks['dates_in_range'] += 1
            
            # Check 3: Reasonable string lengths
            if data[col].dtype == 'object':
                total_checks += 1
                string_lengths = data[col].dropna().astype(str).str.len()
                if (string_lengths <= 255).all():  # Reasonable max length
                    passed_checks += 1
                    validity_checks['reasonable_string_lengths'] += 1
        
        score = passed_checks / total_checks if total_checks > 0 else 1.0
        
        details = {
            'validity_checks_performed': validity_checks,
            'total_checks': total_checks,
            'passed_checks': passed_checks
        }
        
        return score, details
    
    # -----------------------------------------------------------------
    # METRIC 3: Execution Time
    # -----------------------------------------------------------------
    
    def measure_execution_time(self, 
                              pipeline_func: callable,
                              test_data: pd.DataFrame,
                              iterations: int = 3) -> Dict[str, Any]:
        """
        Measure execution time with warm-up and multiple iterations.
        
        Args:
            pipeline_func: Pipeline function to time
            test_data: Input data for testing
            iterations: Number of times to run (for stable measurement)
            
        Returns:
            Dictionary with timing statistics
        """
        execution_times = []
        
        # Warm-up run (not timed)
        try:
            _ = pipeline_func(test_data.copy())
        except:
            pass
        
        # Timed iterations
        for i in range(iterations):
            start_time = time.perf_counter()
            
            try:
                result = pipeline_func(test_data.copy())
                end_time = time.perf_counter()
                
                execution_times.append(end_time - start_time)
                
            except Exception as e:
                execution_times.append(float('inf'))  # Mark as failed
                print(f"Iteration {i+1} failed: {e}")
        
        # Calculate statistics
        valid_times = [t for t in execution_times if t != float('inf')]
        
        if not valid_times:
            return {
                'metric_name': 'Execution Time',
                'value': float('inf'),
                'details': {'error': 'All iterations failed'}
            }
        
        stats = {
            'min': min(valid_times),
            'max': max(valid_times),
            'mean': np.mean(valid_times),
            'median': np.median(valid_times),
            'std': np.std(valid_times),
            'iterations': iterations,
            'failed_iterations': len(execution_times) - len(valid_times)
        }
        
        # Score: Faster is better, normalized by data size
        data_size = len(test_data) * len(test_data.columns)
        normalized_time = stats['median'] / max(1, data_size / 1000)  # Time per 1000 data points
        
        return {
            'metric_name': 'Execution Time',
            'value': stats['median'],  # Primary metric
            'normalized_time': normalized_time,
            'statistics': stats,
            'details': {
                'data_shape': test_data.shape,
                'data_size': data_size,
                'time_per_1000_points': normalized_time
            }
        }
    
    # -----------------------------------------------------------------
    # METRIC 4: Error Recovery Rate
    # -----------------------------------------------------------------
    
    def measure_error_recovery_rate(self,
                                   pipeline_func: callable,
                                   error_test_cases: List[Tuple[str, pd.DataFrame]]) -> Dict[str, Any]:
        """
        Test pipeline's ability to handle errors gracefully.
        
        Args:
            pipeline_func: Pipeline function to test
            error_test_cases: List of (test_name, problematic_data) designed to cause errors
            
        Returns:
            Dictionary with recovery rate and error analysis
        """
        recovery_results = []
        error_analysis = []
        
        for test_name, problematic_data in error_test_cases:
            try:
                # Attempt to run pipeline
                result = pipeline_func(problematic_data.copy())
                
                # If we get here, pipeline recovered
                recovery_results.append({
                    'test_case': test_name,
                    'recovered': True,
                    'result_type': type(result).__name__,
                    'error': None
                })
                
            except Exception as e:
                # Pipeline crashed
                error_trace = traceback.format_exc()
                
                recovery_results.append({
                    'test_case': test_name,
                    'recovered': False,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'error_trace': error_trace[:500]  # First 500 chars
                })
                
                error_analysis.append({
                    'test_case': test_name,
                    'error_type': type(e).__name__,
                    'trigger': self._analyze_error_trigger(problematic_data, str(e))
                })
        
        # Calculate recovery rate
        recovered_count = sum(1 for r in recovery_results if r['recovered'])
        total_cases = len(recovery_results)
        recovery_rate = recovered_count / total_cases if total_cases > 0 else 0.0
        
        # Analyze error patterns
        error_patterns = self._analyze_error_patterns(recovery_results)
        
        return {
            'metric_name': 'Error Recovery Rate',
            'value': recovery_rate,
            'details': {
                'recovered_cases': recovered_count,
                'total_cases': total_cases,
                'recovery_results': recovery_results,
                'error_patterns': error_patterns,
                'error_analysis': error_analysis
            }
        }
    
    def _analyze_error_trigger(self, data: pd.DataFrame, error_msg: str) -> str:
        """Analyze what in the data likely caused the error"""
        triggers = []
        
        # Check for common error triggers
        if data.empty:
            triggers.append('empty_dataframe')
        
        if any(col for col in data.columns if re.search(r'[^\w\s]', str(col))):
            triggers.append('special_chars_in_column_names')
        
        if any(data[col].dtype == 'object' and data[col].str.contains('[^\x00-\x7F]').any() 
               for col in data.columns if data[col].dtype == 'object'):
            triggers.append('non_ascii_characters')
        
        if 'inf' in error_msg.lower() or 'nan' in error_msg.lower():
            triggers.append('inf_or_nan_values')
        
        if 'type' in error_msg.lower() or 'dtype' in error_msg.lower():
            triggers.append('type_mismatch')
        
        return ', '.join(triggers) if triggers else 'unknown'
    
    def _analyze_error_patterns(self, recovery_results: List[Dict]) -> Dict:
        """Analyze patterns in errors"""
        error_types = {}
        
        for result in recovery_results:
            if not result['recovered']:
                error_type = result.get('error_type', 'Unknown')
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'error_type_distribution': error_types,
            'most_common_error': max(error_types.items(), key=lambda x: x[1])[0] if error_types else None,
            'total_unique_errors': len(error_types)
        }
    
    # -----------------------------------------------------------------
    # COMPREHENSIVE EVALUATION
    # -----------------------------------------------------------------
    
    def comprehensive_evaluation(self,
                                pipeline_func: callable,
                                success_test_cases: List[Tuple[str, pd.DataFrame, pd.DataFrame]],
                                error_test_cases: List[Tuple[str, pd.DataFrame]],
                                quality_test_data: Tuple[pd.DataFrame, pd.DataFrame] = None) -> PipelineMetrics:
        """
        Run all four metrics in a comprehensive evaluation.
        
        Args:
            pipeline_func: The pipeline to evaluate
            success_test_cases: Test cases for success rate
            error_test_cases: Test cases for error recovery
            quality_test_data: Tuple of (original, processed) for quality scoring
            
        Returns:
            PipelineMetrics object with all results
        """
        print("=" * 60)
        print("Starting Comprehensive Pipeline Evaluation")
        print("=" * 60)
        
        # 1. Measure Success Rate
        print("\n1. Measuring Pipeline Success Rate...")
        success_metrics = self.calculate_success_rate(pipeline_func, success_test_cases)
        
        # 2. Measure Data Quality
        print("\n2. Measuring Data Quality Improvement...")
        if quality_test_data:
            original, processed = quality_test_data
            quality_metrics = self.calculate_data_quality_score(original, processed)
        else:
            quality_metrics = {'value': 0.0, 'details': 'No quality test data provided'}
        
        # 3. Measure Execution Time (use first success test case)
        print("\n3. Measuring Execution Time...")
        if success_test_cases:
            _, sample_data, _ = success_test_cases[0]
            time_metrics = self.measure_execution_time(pipeline_func, sample_data)
        else:
            time_metrics = {'value': 0.0, 'details': 'No test data for timing'}
        
        # 4. Measure Error Recovery
        print("\n4. Measuring Error Recovery Rate...")
        if error_test_cases:
            recovery_metrics = self.measure_error_recovery_rate(pipeline_func, error_test_cases)
        else:
            recovery_metrics = {'value': 0.0, 'details': 'No error test cases provided'}
        
        # Compile all results
        metrics = PipelineMetrics(
            success_rate=success_metrics['value'],
            data_quality_score=quality_metrics['value'],
            execution_time=time_metrics['value'],
            error_recovery_rate=recovery_metrics['value'],
            details={
                'success_details': success_metrics,
                'quality_details': quality_metrics,
                'time_details': time_metrics,
                'recovery_details': recovery_metrics,
                'evaluation_timestamp': datetime.now().isoformat()
            }
        )
        
        self.metrics_history.append(metrics)
        
        print("\n" + "=" * 60)
        print("Evaluation Complete!")
        print("=" * 60)
        print(f"Success Rate: {metrics.success_rate:.2%}")
        print(f"Data Quality Score: {metrics.data_quality_score:.2%}")
        print(f"Execution Time: {metrics.execution_time:.4f} seconds")
        print(f"Error Recovery Rate: {metrics.error_recovery_rate:.2%}")
        
        return metrics
    
    def generate_report(self, metrics: PipelineMetrics, output_path: str = None) -> str:
        """Generate a comprehensive evaluation report"""
        report = {
            'summary': {
                'success_rate': metrics.success_rate,
                'data_quality_score': metrics.data_quality_score,
                'execution_time': metrics.execution_time,
                'error_recovery_rate': metrics.error_recovery_rate,
                'overall_score': (
                    metrics.success_rate * 0.4 +
                    metrics.data_quality_score * 0.3 +
                    (1.0 / max(0.1, metrics.execution_time)) * 0.2 +
                    metrics.error_recovery_rate * 0.1
                )
            },
            'detailed_metrics': metrics.details,
            'evaluation_date': datetime.now().isoformat(),
            'pipeline_version': 'PipelineCrewAIPyDEAML',  # Update with actual version
            'test_environment': {
                'pandas_version': pd.__version__,
                'numpy_version': np.__version__,
                'python_version': sys.version
            }
        }
        
        report_json = json.dumps(report, indent=2, default=str)
        
        if output_path:
            with open(output_path, 'w') as f:
                f.write(report_json)
        
        return report_json