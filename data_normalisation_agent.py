import pandas as pd
import numpy as np
from sklearn.preprocessing import (
    StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler,
    PowerTransformer, QuantileTransformer,
    OneHotEncoder, OrdinalEncoder, LabelEncoder,
    KBinsDiscretizer, PolynomialFeatures
)
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
from sklearn.feature_selection import (
    SelectKBest, f_classif, f_regression, 
    mutual_info_classif, mutual_info_regression,
    VarianceThreshold
)
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import warnings
from typing import Dict, List, Tuple, Optional, Union
import json
from datetime import datetime
import warnings
from sklearn.preprocessing import (
    LabelEncoder, OneHotEncoder, OrdinalEncoder, 
    KBinsDiscretizer, PolynomialFeatures
)
from sklearn.feature_selection import (
    SelectKBest, f_classif, f_regression, 
    mutual_info_classif, mutual_info_regression
)
from sklearn.pipeline import Pipeline

class DataNormalisationAgent:
    """
    Data normalisation enhanced by AI with multiple techniquesand intelligent method selection
    """
    
    def __init__(self):
        self.normalisation_methods = {
            'standard_scaler': 'StandardScaler (z-score normalisation)',
            'minmax_scaler': 'MinMaxScaler (0-1 normalisation)',
            'robust_scaler': 'RobustScaler (robust to outliers)',
            'maxabs_scaler': 'MaxAbsScaler (scales to [-1, 1])',
            'log_transform': 'Logarithmic Transformation',
            'box_cox': 'Box-Cox Power Transformation',
            'yeo_johnson': 'Yeo-Johnson Power Transformation',
            'quantile_uniform': 'Quantile Transformer (uniform)',
            'quantile_normal': 'Quantile Transformer (normal)',
            'decimal_scaling': 'Decimal Scaling',
            'mean_normalisation': 'Mean Normalisation',
            'unit_vector': 'Unit Vector Normalisation',
            'pca': 'PCA Whitening'
        }


        # Add detailed help descriptions
        self.method_descriptions = {
            'standard_scaler': {
                'short': 'Z-score Standardization',
                'long': 'Centers data to mean=0 and scales to standard deviation=1. Formula: (x - μ) / σ',
                'when_to_use': [
                    'When data is approximately normally distributed',
                    'Before algorithms that assume Gaussian distribution (e.g., linear regression, logistic regression)',
                    'When you need to compare features on the same scale'
                ],
                'pros': ['Preserves outlier information', 'Maintains original distribution shape'],
                'cons': ['Sensitive to outliers', 'Assumes normal distribution'],
                'formula': '(x - mean) / standard_deviation'
            },
            'minmax_scaler': {
                'short': 'Min-Max Scaling',
                'long': 'Scales data to a specified range (default 0 to 1). Formula: (x - min) / (max - min)',
                'when_to_use': [
                    'When you need bounded values (e.g., for neural networks)',
                    'When the exact bounds are important',
                    'For algorithms sensitive to feature scales (e.g., SVM, KNN)'
                ],
                'pros': ['Preserves relationships among original values', 'Easy to interpret'],
                'cons': ['Very sensitive to outliers', 'Loses variance information'],
                'formula': '(x - min(x)) / (max(x) - min(x))'
            },
            'robust_scaler': {
                'short': 'Robust Scaling',
                'long': 'Uses median and IQR (Interquartile Range) instead of mean/std. More robust to outliers.',
                'when_to_use': [
                    'When data contains outliers',
                    'When you want to reduce outlier influence',
                    'For tree-based models with skewed data'
                ],
                'pros': ['Resistant to outliers', 'Uses robust statistics'],
                'cons': ['Not suitable for all algorithms', 'May distort data if no outliers'],
                'formula': '(x - median) / IQR'
            },
            'maxabs_scaler': {
                'short': 'Max Absolute Scaling',
                'long': 'Scales each feature by its maximum absolute value. Range: [-1, 1]',
                'when_to_use': [
                    'When data is sparse (many zeros)',
                    'For data centered around zero',
                    'When preserving sparsity is important'
                ],
                'pros': ['Preserves sparsity', 'Handles zero-centering well'],
                'cons': ['Sensitive to outliers', 'Limited to [-1, 1] range'],
                'formula': 'x / max(|x|)'
            },
            'log_transform': {
                'short': 'Logarithmic Transformation',
                'long': 'Applies natural logarithm to reduce skewness and handle exponential relationships.',
                'when_to_use': [
                    'When data is highly skewed (right-skewed)',
                    'For data with exponential growth patterns',
                    'To handle large value ranges'
                ],
                'pros': ['Reduces skewness', 'Handles large value ranges'],
                'cons': ['Cannot handle zeros or negative values (need adjustment)'],
                'formula': 'log(x + c) where c = 1 for zeros'
            },
            'box_cox': {
                'short': 'Box-Cox Transformation',
                'long': 'Power transformation that finds optimal λ to make data more normal. Requires positive values only.',
                'when_to_use': [
                    'When data is positive and skewed',
                    'When you need to normalize skewed distributions',
                    'For statistical tests requiring normality'
                ],
                'pros': ['Can normalize various distributions', 'Data-driven parameter selection'],
                'cons': ['Only works with positive values', 'More computationally intensive'],
                'formula': '(x^λ - 1)/λ if λ ≠ 0, log(x) if λ = 0'
            },
            'yeo_johnson': {
                'short': 'Yeo-Johnson Transformation',
                'long': 'Extension of Box-Cox that works with both positive and negative values.',
                'when_to_use': [
                    'When data contains zeros or negative values',
                    'For general power transformations',
                    'When Box-Cox fails due to negative values'
                ],
                'pros': ['Works with all real numbers', 'Flexible transformation'],
                'cons': ['More complex than Box-Cox', 'Computationally intensive'],
                'formula': 'Complex piecewise function based on x and λ'
            },
            'quantile_uniform': {
                'short': 'Quantile Transformer (Uniform)',
                'long': 'Non-linear transformation that maps data to a uniform distribution using quantiles.',
                'when_to_use': [
                    'When you need uniform distribution',
                    'For rank-based transformations',
                    'To handle outliers robustly'
                ],
                'pros': ['Robust to outliers', 'Makes distribution uniform'],
                'cons': ['May distort small datasets', 'Computationally expensive'],
                'formula': 'Uses empirical CDF mapping'
            },
            'quantile_normal': {
                'short': 'Quantile Transformer (Normal)',
                'long': 'Non-linear transformation that maps data to a normal distribution using quantiles.',
                'when_to_use': [
                    'When you need normal distribution',
                    'For statistical methods requiring normality',
                    'To handle outliers in non-linear way'
                ],
                'pros': ['Makes any distribution normal', 'Robust to outliers'],
                'cons': ['May create artifacts', 'Computationally expensive'],
                'formula': 'Uses inverse normal CDF of empirical CDF'
            },
            'decimal_scaling': {
                'short': 'Decimal Scaling',
                'long': 'Scales by powers of 10 to bring values into [-1, 1] range. Simple but limited.',
                'when_to_use': [
                    'For simple scaling needs',
                    'When interpretability is key',
                    'For educational purposes'
                ],
                'pros': ['Simple to understand', 'Easy to implement'],
                'cons': ['Not data-adaptive', 'Limited precision'],
                'formula': 'x / 10^j where j = ceil(log10(max(|x|)))'
            },
            'mean_normalisation': {
                'short': 'Mean Normalisation',
                'long': 'Centers data around 0 and scales by range. Similar to min-max but centered.',
                'when_to_use': [
                    'When you need centered data with fixed range',
                    'For simple standardization needs',
                    'When data range is known'
                ],
                'pros': ['Centers data at 0', 'Simple to implement'],
                'cons': ['Sensitive to outliers', 'Loses variance information'],
                'formula': '(x - mean) / (max - min)'
            },
            'unit_vector': {
                'short': 'Unit Vector Normalisation',
                'long': 'Scales vectors to unit length (norm = 1). Used in text mining and cosine similarity.',
                'when_to_use': [
                    'For cosine similarity calculations',
                    'In text mining (TF-IDF vectors)',
                    'When direction matters more than magnitude'
                ],
                'pros': ['Useful for angle-based metrics', 'Standard in text mining'],
                'cons': ['Not suitable for all ML algorithms', 'Loses magnitude information'],
                'formula': 'x / ||x||'
            },
            'pca': {
                'short': 'PCA Whitening',
                'long': 'Principal Component Analysis with scaling (whitening). Reduces dimensions and decorrelates.',
                'when_to_use': [
                    'For dimensionality reduction',
                    'When features are highly correlated',
                    'Before algorithms sensitive to multicollinearity'
                ],
                'pros': ['Reduces dimensionality', 'Removes correlation', 'May improve performance'],
                'cons': ['Loses interpretability', 'Computationally expensive'],
                'formula': 'X_whitened = X_rotated / sqrt(eigenvalues + epsilon)'
            }
        }

        # Techniques categorisation

        self.technique_categories = {
            'statistical': ['standard_scaler', 'minmax_scaler', 'robust_scaler', 'mean_normalisation'],
            'robust': ['robust_scaler', 'quantile_uniform', 'quantile_normal'],
            'non_linear': ['log_transform', 'box_cox', 'yeo_johnson'],
            'dimensionality_reduction': ['pca'],
            'simple': ['decimal_scaling', 'unit_vector', 'maxabs_scaler']
        }
        
        # AI recommendations based on data characteristics
        self.recommendation_rules = {
            'normal_distribution': ['standard_scaler', 'mean_normalisation'],
            'outliers_present': ['robust_scaler', 'quantile_normal'],
            'positive_values_only': ['log_transform', 'box_cox', 'minmax_scaler'],
            'negative_values': ['yeo_johnson', 'standard_scaler', 'robust_scaler'],
            'sparse_data': ['maxabs_scaler'],
            'dimension_reduction_needed': ['pca'],
            'preserve_zeros': ['minmax_scaler', 'maxabs_scaler'],
            'non_linear_relationships': ['quantile_normal', 'yeo_johnson']
        }
    

        # ML-specific normalisation methods
        self.ml_preprocessing_methods = {
            # Categorical encoding
            'label_encoding': 'Label Encoding (for ordinal categories)',
            'one_hot_encoding': 'One-Hot Encoding (for nominal categories)',
            'target_encoding': 'Target Encoding (for classification/regression)',
            'frequency_encoding': 'Frequency Encoding',
            'binary_encoding': 'Binary Encoding',
            
            # Feature engineering
            'polynomial_features': 'Polynomial Feature Generation',
            'binning': 'Binning/Discretization',
            'interaction_terms': 'Interaction Terms',
            
            # Feature selection
            'select_k_best': 'Select K Best Features',
            'variance_threshold': 'Remove Low Variance Features',
            'correlation_filter': 'Remove Highly Correlated Features'
        }
        
        # Additional normalisation_methods
        self.normalisation_methods.update(self.ml_preprocessing_methods)
        
        # ML algorithm compatibility guide
        self.ml_compatibility = {
            'linear_regression': ['standard_scaler', 'minmax_scaler', 'robust_scaler'],
            'logistic_regression': ['standard_scaler', 'minmax_scaler'],
            'svm': ['standard_scaler', 'minmax_scaler'],
            'knn': ['standard_scaler', 'minmax_scaler', 'unit_vector'],
            'neural_network': ['standard_scaler', 'minmax_scaler'],
            'decision_tree': ['robust_scaler', 'decimal_scaling'],
            'random_forest': ['robust_scaler'],
            'xgboost': ['robust_scaler'],
            'kmeans': ['standard_scaler', 'minmax_scaler', 'unit_vector'],
            'pca': ['standard_scaler']
        }
    


    def analyse_data_characteristics(self, data: pd.DataFrame, numeric_columns: List[str]) -> Dict:
        """
        AI-based analysis of data characteristics for intelligent normalisation recommendations
        """
        analysis = {
            'has_outliers': False,
            'normal_distribution': [],
            'has_negative': False,
            'has_zeros': False,
            'skewed_columns': [],
            'sparse_columns': [],
            'recommended_methods': []
        }
        
        for col in numeric_columns:
            col_data = data[col].dropna()
            
            # Check for outliers using IQR
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((col_data < (Q1 - 1.5 * IQR)) | (col_data > (Q3 + 1.5 * IQR))).sum()
            if outliers > len(col_data) * 0.05:  # More than 5% outliers
                analysis['has_outliers'] = True
            
            # Check normality using skewness
            skewness = col_data.skew()
            if abs(skewness) < 0.5:
                analysis['normal_distribution'].append(col)
            elif abs(skewness) > 1:
                analysis['skewed_columns'].append((col, skewness))
            
            # Check for negative values
            if (col_data < 0).any():
                analysis['has_negative'] = True
            
            # Check for zeros
            if (col_data == 0).any():
                analysis['has_zeros'] = True
            
            # Check sparsity
            zero_percentage = (col_data == 0).sum() / len(col_data)
            if zero_percentage > 0.9:  # 90% zeros
                analysis['sparse_columns'].append(col)
        
        # Generate recommendations based on analysis
        recommendations = []
        
        if analysis['normal_distribution']:
            recommendations.extend(self.recommendation_rules['normal_distribution'])
        
        if analysis['has_outliers']:
            recommendations.extend(self.recommendation_rules['outliers_present'])
        
        if not analysis['has_negative']:
            recommendations.extend(self.recommendation_rules['positive_values_only'])
        else:
            recommendations.extend(self.recommendation_rules['negative_values'])
        
        if analysis['sparse_columns']:
            recommendations.append('maxabs_scaler')
        
        if len(numeric_columns) > 10:  # High dimensionality
            recommendations.append('pca')
        
        # Remove duplicates and keep unique recommendations
        analysis['recommended_methods'] = list(set(recommendations))
        
        return analysis
    
    def normalise_with_method(self, data: pd.DataFrame, columns: List[str], 
                            method: str, **kwargs) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply specific normalisation method to selected columns
        Returns: (normalised_data, transformation_metadata)
        """
        data_copy = data.copy()
        metadata = {
            'method': method,
            'columns': columns,
            'parameters': kwargs,
            'transformers': {},
            'applied_at': datetime.now().isoformat()
        }
        
        if method == 'standard_scaler':
            for col in columns:
                scaler = StandardScaler()
                data_copy[col] = scaler.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'StandardScaler',
                    'mean': float(scaler.mean_[0]),
                    'scale': float(scaler.scale_[0])
                }
        
        elif method == 'minmax_scaler':
            feature_range = kwargs.get('feature_range', (0, 1))
            for col in columns:
                scaler = MinMaxScaler(feature_range=feature_range)
                data_copy[col] = scaler.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'MinMaxScaler',
                    'data_min': float(scaler.data_min_[0]),
                    'data_max': float(scaler.data_max_[0]),
                    'feature_range': feature_range
                }
        
        elif method == 'robust_scaler':
            for col in columns:
                scaler = RobustScaler()
                data_copy[col] = scaler.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'RobustScaler',
                    'center': float(scaler.center_[0]),
                    'scale': float(scaler.scale_[0])
                }
        
        elif method == 'maxabs_scaler':
            for col in columns:
                scaler = MaxAbsScaler()
                data_copy[col] = scaler.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'MaxAbsScaler',
                    'max_abs': float(scaler.max_abs_[0]),
                    'scale': float(scaler.scale_[0])
                }
        
        elif method == 'log_transform':
            for col in columns:
                # Add small constant to handle zeros
                min_val = data_copy[col].min()
                if min_val <= 0:
                    constant = abs(min_val) + 1
                    data_copy[col] = np.log(data_copy[col] + constant)
                    metadata['transformers'][col] = {
                        'type': 'LogTransform',
                        'added_constant': constant
                    }
                else:
                    data_copy[col] = np.log(data_copy[col])
                    metadata['transformers'][col] = {'type': 'LogTransform'}
        
        elif method == 'box_cox':
            for col in columns:
                transformer = PowerTransformer(method='box-cox')
                # Box-Cox requires positive values
                if (data_copy[col] > 0).all():
                    data_copy[col] = transformer.fit_transform(data_copy[[col]])
                    metadata['transformers'][col] = {
                        'type': 'BoxCox',
                        'lambdas': float(transformer.lambdas_[0])
                    }
                else:
                    warnings.warn(f"Column {col} has non-positive values. Skipping Box-Cox.")
        
        elif method == 'yeo_johnson':
            for col in columns:
                transformer = PowerTransformer(method='yeo-johnson')
                data_copy[col] = transformer.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'YeoJohnson',
                    'lambdas': float(transformer.lambdas_[0])
                }
        
        elif method == 'quantile_normal':
            n_quantiles = kwargs.get('n_quantiles', 1000)
            output_distribution = 'normal'
            for col in columns:
                transformer = QuantileTransformer(n_quantiles=n_quantiles, 
                                                output_distribution=output_distribution,
                                                random_state=42)
                data_copy[col] = transformer.fit_transform(data_copy[[col]])
                metadata['transformers'][col] = {
                    'type': 'QuantileTransformer',
                    'n_quantiles': n_quantiles,
                    'output_distribution': output_distribution
                }
        
        elif method == 'decimal_scaling':
            for col in columns:
                max_abs = data_copy[col].abs().max()
                j = np.ceil(np.log10(max_abs))
                data_copy[col] = data_copy[col] / (10 ** j)
                metadata['transformers'][col] = {
                    'type': 'DecimalScaling',
                    'j': float(j),
                    'divisor': float(10 ** j)
                }
        
        elif method == 'mean_normalisation':
            for col in columns:
                mean_val = data_copy[col].mean()
                range_val = data_copy[col].max() - data_copy[col].min()
                if range_val != 0:
                    data_copy[col] = (data_copy[col] - mean_val) / range_val
                    metadata['transformers'][col] = {
                        'type': 'MeanNormalisation',
                        'mean': float(mean_val),
                        'range': float(range_val)
                    }
        
        elif method == 'unit_vector':
            for col in columns:
                norm = np.linalg.norm(data_copy[col])
                if norm != 0:
                    data_copy[col] = data_copy[col] / norm
                    metadata['transformers'][col] = {
                        'type': 'UnitVector',
                        'norm': float(norm)
                    }
        
        elif method == 'pca':
            # PCA on selected columns
            pca_data = data_copy[columns].dropna()
            n_components = kwargs.get('n_components', min(len(columns), len(pca_data)))
            pca = PCA(n_components=n_components, random_state=42)
            pca_result = pca.fit_transform(pca_data)
            
            # Create new column names
            pca_columns = [f'PCA_{i+1}' for i in range(n_components)]
            
            # Add PCA results to dataframe
            for i, pca_col in enumerate(pca_columns):
                data_copy[pca_col] = np.nan
                data_copy.loc[pca_data.index, pca_col] = pca_result[:, i]
            
            metadata['transformers']['pca'] = {
                'type': 'PCA',
                'n_components': n_components,
                'explained_variance_ratio': pca.explained_variance_ratio_.tolist(),
                'components': pca.components_.tolist()
            }
        
        return data_copy, metadata
    
    def batch_normalise(self, data: pd.DataFrame, 
                       column_method_map: Dict[str, List[str]]) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply different normalisation methods to different columns
        column_method_map: {method_name: [list_of_columns]}
        """
        normalised_data = data.copy()
        all_metadata = {
            'batch_normalisation': True,
            'applied_methods': {},
            'timestamp': datetime.now().isoformat()
        }
        
        for method, columns in column_method_map.items():
            if columns and method in self.normalisation_methods:
                normalised_data, metadata = self.normalise_with_method(
                    normalised_data, columns, method
                )
                all_metadata['applied_methods'][method] = metadata
        
        return normalised_data, all_metadata
    
    def inverse_transform(self, data: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """
        Inverse transformation using stored metadata
        """
        data_copy = data.copy()
        
        for method_info in metadata.get('applied_methods', {}).values():
            if 'transformers' in method_info:
                for col, transformer_info in method_info['transformers'].items():
                    if col in data_copy.columns:
                        if transformer_info['type'] == 'StandardScaler':
                            data_copy[col] = (data_copy[col] * transformer_info['scale'] + 
                                            transformer_info['mean'])
                        elif transformer_info['type'] == 'MinMaxScaler':
                            data_min = transformer_info['data_min']
                            data_max = transformer_info['data_max']
                            feature_range = transformer_info['feature_range']
                            scaled = (data_copy[col] - feature_range[0]) / (feature_range[1] - feature_range[0])
                            data_copy[col] = scaled * (data_max - data_min) + data_min
                        # Add other inverse transformations as needed
        
        return data_copy
    
    def get_normalisation_summary(self, data_before: pd.DataFrame, 
                                data_after: pd.DataFrame, 
                                columns: List[str]) -> pd.DataFrame:
        """
        Generate summary statistics before and after normalisation
        """
        summary = []
        
        for col in columns:
            if col in data_before.columns and col in data_after.columns:
                before = data_before[col].dropna()
                after = data_after[col].dropna()
                
                summary.append({
                    'column': col,
                    'before_mean': before.mean(),
                    'after_mean': after.mean(),
                    'before_std': before.std(),
                    'after_std': after.std(),
                    'before_min': before.min(),
                    'after_min': after.min(),
                    'before_max': before.max(),
                    'after_max': after.max(),
                    'before_range': before.max() - before.min(),
                    'after_range': after.max() - after.min()
                })
        
        return pd.DataFrame(summary)
    

    # ML-specific normalisation recommendations
        
    def analyse_for_ml(self, data: pd.DataFrame, target_column: str = None) -> Dict:
        """
        Advanced analysis for ML preparation
        """
        analysis = {
            'ml_issues': [],
            'suggestions': [],
            'feature_types': {},
            'missing_values': {},
            'correlation_issues': []
        }
        
        # Analyse each column
        for col in data.columns:
            dtype = str(data[col].dtype)
            
            if dtype in ['object', 'category']:
                analysis['feature_types'][col] = 'categorical'
                unique_count = data[col].nunique()
                if unique_count > 50:
                    analysis['ml_issues'].append(f"High cardinality in '{col}' ({unique_count} unique values)")
                    analysis['suggestions'].append(f"Consider target encoding or frequency encoding for '{col}'")
                elif unique_count == 2:
                    analysis['suggestions'].append(f"Use binary encoding for '{col}'")
                else:
                    analysis['suggestions'].append(f"Use one-hot encoding for '{col}' (if <10 categories) or target encoding")
            
            elif dtype in ['int64', 'float64']:
                analysis['feature_types'][col] = 'numerical'
                
                # Check for constant or quasi-constant features
                if data[col].nunique() == 1:
                    analysis['ml_issues'].append(f"Constant feature: '{col}'")
                elif data[col].nunique() == 2:
                    analysis['suggestions'].append(f"'{col}' is binary - consider keeping as is or label encoding")
            
            # Missing values
            missing_pct = data[col].isnull().mean() * 100
            if missing_pct > 0:
                analysis['missing_values'][col] = missing_pct
                if missing_pct > 30:
                    analysis['ml_issues'].append(f"High missing values in '{col}' ({missing_pct:.1f}%)")
                    analysis['suggestions'].append(f"Consider imputation or dropping '{col}'")
        
        # Check correlations if enough numeric columns
        numeric_cols = data.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 1:
            corr_matrix = data[numeric_cols].corr().abs()
            high_corr = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    if corr_matrix.iloc[i, j] > 0.9:
                        col1, col2 = corr_matrix.columns[i], corr_matrix.columns[j]
                        high_corr.append((col1, col2, corr_matrix.iloc[i, j]))
            
            if high_corr:
                analysis['correlation_issues'] = high_corr
                analysis['suggestions'].append("Remove highly correlated features to reduce multicollinearity")
        
        return analysis
    
    def prepare_ml_pipeline(self, data: pd.DataFrame, 
                           numeric_method: str = 'standard_scaler',
                           categorical_method: str = 'one_hot_encoding',
                           target_column: str = None,
                           task_type: str = 'classification') -> Tuple[pd.DataFrame, Pipeline]:
        """
        Create a complete ML preprocessing pipeline
        """
        # Identify column types
        numeric_features = data.select_dtypes(include=['int64', 'float64']).columns.tolist()
        if target_column and target_column in numeric_features:
            numeric_features.remove(target_column)
        
        categorical_features = data.select_dtypes(include=['object', 'category']).columns.tolist()
        if target_column and target_column in categorical_features:
            categorical_features.remove(target_column)
        
        # Build transformers
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', self._get_scaler(numeric_method))
        ])
        
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('encoder', self._get_encoder(categorical_method, target_column, task_type))
        ])
        
        # Combine transformers
        preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_features),
                ('cat', categorical_transformer, categorical_features)
            ])
        
        # Create pipeline
        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('feature_selector', SelectKBest(score_func=f_classif if task_type == 'classification' else f_regression, k='all'))
        ])
        
        # Fit and transform
        if target_column:
            X = data.drop(columns=[target_column])
        else:
            X = data
        
        X_transformed = pipeline.fit_transform(X, data[target_column] if target_column else None)
        
        # Create new DataFrame with transformed features
        feature_names = []
        # Get feature names from transformers
        # (This is simplified - actual implementation would need to handle naming properly)
        
        return X_transformed, pipeline
    
    def _get_scaler(self, method: str):
        """Get scaler instance based on method"""
        scalers = {
            'standard_scaler': StandardScaler(),
            'minmax_scaler': MinMaxScaler(),
            'robust_scaler': RobustScaler(),
            'maxabs_scaler': MaxAbsScaler()
        }
        return scalers.get(method, StandardScaler())
    
    def _get_encoder(self, method: str, target_column: str = None, task_type: str = 'classification'):
        """Get encoder instance based on method"""
        if method == 'one_hot_encoding':
            return OneHotEncoder(handle_unknown='ignore', sparse_output=False)
        elif method == 'label_encoding':
            return OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)
        elif method == 'target_encoding' and target_column:
            return ce.TargetEncoder()
        else:
            return OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    
    def get_ml_recommendations(self, algorithm: str, data_shape: tuple) -> Dict:
        """
        Get ML-specific preprocessing recommendations
        """
        recommendations = {
            'normalisation': self.ml_compatibility.get(algorithm, ['standard_scaler']),
            'categorical_encoding': 'one_hot_encoding' if data_shape[1] < 100 else 'target_encoding',
            'feature_selection': True if data_shape[1] > 50 else False,
            'dimensionality_reduction': True if data_shape[1] > 100 else False
        }
        
        # Add algorithm-specific notes
        algorithm_notes = {
            'linear_regression': 'Ensure features are normally distributed, check for multicollinearity',
            'logistic_regression': 'Use regularization (L1/L2) for feature selection',
            'svm': 'Scale features to [0,1] or use z-score normalisation',
            'knn': 'Use MinMaxScaler for distance-based algorithms',
            'neural_network': 'Batch normalisation may help convergence',
            'decision_tree': 'No scaling needed, but handle outliers',
            'random_forest': 'Handle imbalanced classes if present'
        }
        
        recommendations['notes'] = algorithm_notes.get(algorithm, 'Standard preprocessing recommended')
        return recommendations
    
    
    def analyse_for_ml(self, data: pd.DataFrame, target_column: str = None) -> Dict:
        """
        Advanced analysis for ML preparation
        (Add this method to your existing class)
        """
        analysis = {
            'ml_issues': [],
            'suggestions': [],
            'feature_types': {},
            'missing_values': {},
            'correlation_issues': []
        }
        
        # Analyse each column
        for col in data.columns:
            if col == target_column:
                continue  # Skip target column
                
            dtype = str(data[col].dtype)
            
            if dtype in ['object', 'category']:
                analysis['feature_types'][col] = 'categorical'
                unique_count = data[col].nunique()
                if unique_count > 50:
                    analysis['ml_issues'].append(f"High cardinality in '{col}' ({unique_count} unique values)")
                elif unique_count == 2:
                    analysis['suggestions'].append(f"Use binary encoding for '{col}'")
                else:
                    analysis['suggestions'].append(f"Use one-hot encoding for '{col}' (if <10 categories)")
            
            elif dtype in ['int64', 'float64']:
                analysis['feature_types'][col] = 'numerical'
                
                # Check for constant features
                if data[col].nunique() == 1:
                    analysis['ml_issues'].append(f"Constant feature: '{col}'")
            
            # Missing values
            missing_pct = data[col].isnull().mean() * 100
            if missing_pct > 0:
                analysis['missing_values'][col] = missing_pct
                if missing_pct > 30:
                    analysis['ml_issues'].append(f"High missing values in '{col}' ({missing_pct:.1f}%)")
        
        # Check correlations
        numeric_cols = data.select_dtypes(include=['int64', 'float64']).columns.tolist()
        if target_column and target_column in numeric_cols:
            numeric_cols.remove(target_column)
            
        if len(numeric_cols) > 1:
            corr_matrix = data[numeric_cols].corr().abs()
            high_corr = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    if corr_matrix.iloc[i, j] > 0.9:
                        col1, col2 = corr_matrix.columns[i], corr_matrix.columns[j]
                        high_corr.append((col1, col2, corr_matrix.iloc[i, j]))
            
            if high_corr:
                analysis['correlation_issues'] = high_corr
                analysis['suggestions'].append("Remove highly correlated features")
        
        return analysis
    

    def auto_decide_feature_selection(self, df: pd.DataFrame, target_column: str = None) -> Dict:
        """
        Automatically decide if feature selection is needed and suggest settings
        
        Returns:
            Dict with recommendation and reasoning
        """
        n_features = df.shape[1]
        n_samples = df.shape[0]
        
        # Initialize recommendation
        recommendation = {
            'recommended': False,
            'reason': [],
            'confidence': 'low',
            'suggested_k': None,
            'issues_found': []
        }
        
        # Rule 1: Sample-to-feature ratio (critical for overfitting)
        sample_to_feature_ratio = n_samples / n_features if n_features > 0 else float('inf')
        
        if sample_to_feature_ratio < 3:
            recommendation['issues_found'].append({
                'type': 'critical',
                'message': f'Very low samples per feature ({sample_to_feature_ratio:.1f} samples per feature)',
                'suggestion': 'Highly recommended'
            })
            recommendation['recommended'] = True
            recommendation['confidence'] = 'high'
            
        elif sample_to_feature_ratio < 10:
            recommendation['issues_found'].append({
                'type': 'warning',
                'message': f'Low samples per feature ({sample_to_feature_ratio:.1f} samples per feature)',
                'suggestion': 'Recommended'
            })
            recommendation['recommended'] = True
            recommendation['confidence'] = 'medium'
        
        # Rule 2: Check for constant features
        constant_features = []
        for col in df.columns:
            if col != target_column and df[col].nunique() <= 1:
                constant_features.append(col)
        
        if constant_features:
            recommendation['issues_found'].append({
                'type': 'warning',
                'message': f'Found {len(constant_features)} constant/near-constant features',
                'suggestion': 'Remove these features',
                'details': constant_features[:5]  # Show first 5
            })
            recommendation['recommended'] = True
        
        # Rule 3: Check for highly correlated features (>0.95)
        numeric_df = df.select_dtypes(include=[np.number])
        if target_column and target_column in numeric_df.columns:
            numeric_df = numeric_df.drop(columns=[target_column])
        
        if len(numeric_df.columns) > 1:
            corr_matrix = numeric_df.corr().abs()
            high_corr_pairs = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    if corr_matrix.iloc[i, j] > 0.95:
                        col1, col2 = corr_matrix.columns[i], corr_matrix.columns[j]
                        high_corr_pairs.append((col1, col2, corr_matrix.iloc[i, j]))
            
            if high_corr_pairs:
                recommendation['issues_found'].append({
                    'type': 'info',
                    'message': f'Found {len(high_corr_pairs)} highly correlated feature pairs (r > 0.95)',
                    'suggestion': 'Consider removing redundant features',
                    'details': [f"{col1} ↔ {col2} ({corr:.2f})" for col1, col2, corr in high_corr_pairs[:3]]
                })
                if len(high_corr_pairs) > 5:  # Only recommend if many correlations
                    recommendation['recommended'] = True
        
        # Rule 4: Feature count thresholds
        if n_features > 100:
            recommendation['issues_found'].append({
                'type': 'info',
                'message': f'Large number of features ({n_features})',
                'suggestion': 'Feature selection can speed up training'
            })
            recommendation['recommended'] = True
            recommendation['confidence'] = 'medium'
        
        # Suggest optimal k based on rules
        if recommendation['recommended']:
            if sample_to_feature_ratio < 3:
                # Critical - aggressive reduction
                recommendation['suggested_k'] = min(20, max(5, n_samples // 3))
            elif sample_to_feature_ratio < 10:
                # Warning - moderate reduction
                recommendation['suggested_k'] = min(30, max(10, n_samples // 2))
            elif n_features > 50:
                # Large feature set
                recommendation['suggested_k'] = min(40, n_features // 2)
            else:
                # Default
                recommendation['suggested_k'] = min(30, n_features - 2)
        
        # Build reason string
        reasons = []
        if recommendation['issues_found']:
            for issue in recommendation['issues_found']:
                reasons.append(f"{issue['message']} → {issue['suggestion']}")
        else:
            reasons.append("No significant issues detected")
        
        recommendation['reason'] = reasons
        
        return recommendation



    def inverse_transform(self, data: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """
        Inverse transformation using stored metadata
        (Make sure this exists in your class - add if missing)
        """
        data_copy = data.copy()
        
        # Check if it's batch normalization metadata
        if metadata.get('batch_normalization', False):
            for method_info in metadata.get('applied_methods', {}).values():
                if 'transformers' in method_info:
                    for col, transformer_info in method_info['transformers'].items():
                        if col in data_copy.columns:
                            self._apply_inverse_transform(data_copy, col, transformer_info)
        else:
            # Single method metadata
            if 'transformers' in metadata:
                for col, transformer_info in metadata['transformers'].items():
                    if col in data_copy.columns:
                        self._apply_inverse_transform(data_copy, col, transformer_info)
        
        return data_copy
    
    def _apply_inverse_transform(self, data: pd.DataFrame, col: str, transformer_info: Dict):
        """Helper for inverse transformation"""
        if transformer_info['type'] == 'StandardScaler':
            data[col] = (data[col] * transformer_info['scale'] + transformer_info['mean'])
        elif transformer_info['type'] == 'MinMaxScaler':
            data_min = transformer_info['data_min']
            data_max = transformer_info['data_max']
            feature_range = transformer_info.get('feature_range', (0, 1))
            scaled = (data[col] - feature_range[0]) / (feature_range[1] - feature_range[0])
            data[col] = scaled * (data_max - data_min) + data_min
        # Add other inverse transformations as needed