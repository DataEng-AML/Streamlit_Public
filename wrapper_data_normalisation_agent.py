
import streamlit as st
import pandas as pd
import numpy as np
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Import your normalisation agent
from data_normalisation_agent import DataNormalisationAgent

# Import sklearn components for ML pipeline
from sklearn.preprocessing import (
    StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler,
    OneHotEncoder, OrdinalEncoder, LabelEncoder,
    KBinsDiscretizer, PolynomialFeatures
)
from sklearn.impute import SimpleImputer
from sklearn.feature_selection import SelectKBest, f_classif, f_regression
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import warnings
from sklearn.feature_selection import (
    SelectKBest, f_classif, f_regression, 
    mutual_info_classif, mutual_info_regression,
    VarianceThreshold
)

class MLConfigManager:
    """Manages ML preprocessing configuration with defaults"""
    
    DEFAULT_CONFIG = {
        # Preprocessing methods
        'numeric_method': 'standard_scaler',
        'categorical_method': 'one_hot_encoding',
        'numeric_missing': 'median',
        'categorical_missing': 'constant',
        
        # Feature engineering
        'polynomial': False,
        'poly_degree': 2,
        'interactions': False,
        
        # Feature selection
        'feature_selection': False,
        'k_features': None,
        'feature_selection_method': None,
        
        # Target & task
        'target_column': None,
        'task_type': 'classification',
        'algorithm': 'random_forest',
        
        # Pipeline flags
        'create_pipeline': True,
        'return_transformed': True
    }
    
    @classmethod
    def ensure_complete(cls, user_config: Dict) -> Dict:
        """Merge user config with defaults"""
        config = cls.DEFAULT_CONFIG.copy()
        config.update({k: v for k, v in user_config.items() if v is not None})
        return config
    
    @classmethod
    def validate(cls, config: Dict) -> Tuple[bool, List[str]]:
        """Validate configuration"""
        errors = []
        
        # Check numeric method
        if config['numeric_method'] not in ['standard_scaler', 'minmax_scaler', 
                                           'robust_scaler', 'maxabs_scaler', 'none']:
            errors.append(f"Invalid numeric_method: {config['numeric_method']}")
        
        # Check k_features if feature_selection is True
        if config['feature_selection'] and config['k_features'] is None:
            errors.append("k_features required when feature_selection is True")
        
        return len(errors) == 0, errors
    
class WrapperDataNormalisationAgent:
    """
    Streamlit wrapper for ML-ready data preparation
    """
    
    def __init__(self):
        self.agent = DataNormalisationAgent()
        self.setup_session_state()
    
    def setup_session_state(self):
        """Initialize session state variables"""
        defaults = {
            'normalised_df': None,
            'normalisation_metadata': None,
            'ml_ready_df': None,
            'ml_pipeline': None,
            'ml_analysis': None,
            'target_column': None,
            'task_type': 'classification'
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    def normalisation_workflow(self, df: pd.DataFrame) -> pd.DataFrame:

        #st.header("Data normalisation Agent")
        st.markdown("#### Data normalisation Agent &nbsp;- &nbsp;AI Analysis&nbsp;&&nbsp;Recommendations")
        
        # Display original data stats
        #st.subheader("Original Data Statistics")
        #st.markdown("##### Original Data Statistics")
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        
        if len(numeric_columns) == 0:
            st.warning("No numeric columns found for normalisation!")
            return df
        
        # AI Analysis and Recommendations
        #st.subheader("AI Analysis & Recommendations")
        #st.markdown("##### AI Analysis & Recommendations")

        with st.expander("View Data Characteristics Analysis"):
            analysis = self.agent.analyse_data_characteristics(df, numeric_columns)
            
            #st.write("**Data Characteristics:**")
            st.markdown(f"##### **:green[Data Characteristics:]**")
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"Has Outliers: {'✅' if analysis['has_outliers'] else '❌'}")
                st.write(f"Has Negative Values: {'✅' if analysis['has_negative'] else '❌'}")
            with col2:
                st.write(f"Normally Distributed Columns: {len(analysis['normal_distribution'])}")
                st.write(f"Skewed Columns: {len(analysis['skewed_columns'])}")
            
            if analysis['recommended_methods']:
                #st.write("**AI Recommendations:**")
                st.markdown(f"##### **:green[AI Recommendations:]**")
                #for method in analysis['recommended_methods']:
                    #st.write(f"•&nbsp;&nbsp;&nbsp; {self.agent.normalisation_methods[method]}")

                for method in analysis['recommended_methods']:
                    st.markdown(
                        f"<div style='margin-bottom: 3px; line-height: 1.5;'>•&nbsp;&nbsp;&nbsp;{self.agent.normalisation_methods[method]}</div>",
                        unsafe_allow_html=True
    )
        

        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

        # Method Selection (simplified version)
        #st.subheader("⚙️ normalisation Configuration")
        st.markdown("##### Normalisation Configuration")
        
        selected_method = st.selectbox(
            "Select Normalisation Method:",
            options=list(self.agent.normalisation_methods.keys()),
            format_func=lambda x: self.agent.normalisation_methods[x]
        )
            
        # Descriptions and help about each selected method

        if selected_method:
            # Show help in an expander
            # Get the display name from your methods dict
            method_display_name = self.agent.normalisation_methods.get(selected_method, selected_method)

            with st.expander(f"ℹ️ About **:green[{method_display_name}]**", expanded=True):
                if hasattr(self.agent, 'method_descriptions') and selected_method in self.agent.method_descriptions:
                    info = self.agent.method_descriptions[selected_method]
                    
                    # Display all information
                    st.markdown(f"### {info['short']}")
                    st.markdown(f"**Description:** {info['long']}")
                    
                    st.markdown(f"**When to use {method_display_name}:**")
                    for item in info['when_to_use']:
                        st.markdown(f"•&nbsp;&nbsp;&nbsp; {item}")
                    
                    if 'formula' in info and info['formula']:
                        st.markdown(f"**Formula:** `{info['formula']}`")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Pros:**")
                        for pro in info['pros']:
                            #st.markdown(f"✅ {pro}")
                            st.markdown(f"<span style='font-size: 0.7em;'>✅</span> &nbsp; {pro}", unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown("**Cons:**")
                        for con in info['cons']:
                            #st.markdown(f"❌ {con}")
                            st.markdown(f"<span style='font-size: 0.7em;'>❌</span> &nbsp; {con}", unsafe_allow_html=True)
                else:
                    st.info(f"Using: {self.agent.normalisation_methods.get(selected_method, selected_method)}")


        # Column Selection
        columns_to_normalise = st.multiselect(
            "Select columns to normalise:",
            options=numeric_columns,
            default=numeric_columns
        )
        

        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

        if st.button("Apply Normalisation", key="apply_norm"):
            with st.spinner("Normalising data..."):
                normalised_data, metadata = self.agent.normalise_with_method(
                    df, columns_to_normalise, selected_method
                )
                
                st.session_state.normalised_df = normalised_data
                st.session_state.normalisation_metadata = metadata
        
        # Display results if normalised
        if st.session_state.normalised_df is not None:
            self._display_normalisation_results(df, st.session_state.normalised_df, numeric_columns)
        
        return st.session_state.normalised_df if st.session_state.normalised_df is not None else df
    

    def _configure_feature_selection(self, use_feature_selection, auto_recommendation, df, target_col):
        """Configure feature selection parameters"""
        
        n_features = df.shape[1]
        
        if not use_feature_selection:
            return {'enabled': False, 'k_features': None, 'method': None}
        
        st.markdown("###### Configure Feature Selection")
        
        # Early exit if insufficient features
        if n_features <= 1:
            st.warning("Only 1 feature available - feature selection skipped")
            return {'enabled': False, 'k_features': None, 'method': None}
        
        # Create configuration interface
        config_tab1, config_tab2 = st.tabs(["🎯 Smart Configuration", "⚙️ Manual Configuration"])
        
        k_features = None
        method = None
        
        with config_tab1:
            st.info("AI-optimized settings")
            
            # Calculate safe suggested value
            raw_suggested = auto_recommendation.get('suggested_k', min(10, n_features))
            suggested_k = max(1, min(raw_suggested, n_features))
            
            # Calculate safe range
            min_k = max(1, int(suggested_k * 0.5))
            max_k = min(n_features, int(suggested_k * 1.5))
            
            # Ensure range is valid
            if min_k >= max_k:
                min_k, max_k = 1, n_features
            
            st.write(f"**Suggestion:** Keep {suggested_k} of {n_features} features")
            
            k_features = st.slider(
                "Features to keep",
                min_value=min_k,
                max_value=max_k,
                value=suggested_k,
                key="smart_k_features"
            )
        
        with config_tab2:
            st.warning("Advanced manual settings")
            
            # Safe default
            default_k = min(auto_recommendation.get('suggested_k', min(10, n_features)), n_features)
            
            k_features = st.number_input(
                "Features to select (k)",
                min_value=1,
                max_value=n_features,
                value=default_k,
                step=1,
                key="manual_k_features"
            )
            
            # Method selection
            if target_col:
                method_options = ['f_classif', 'f_regression']
                method_labels = ['ANOVA F-value (classification)', 'F-value (regression)']
                selected_idx = 0 if st.session_state.get('task_type') == 'classification' else 1
                
                selected_method = st.selectbox(
                    "Selection algorithm",
                    options=method_options,
                    format_func=lambda x: method_labels[method_options.index(x)],
                    index=selected_idx
                )
                method = selected_method
            else:
                st.info("Unsupervised selection (no target)")
                method = 'variance'
        
        return {
            'enabled': True,
            'k_features': k_features,
            'method': method,
            'n_features_total': n_features
        }



    #     with st.expander("Auto - Feature Selection Status"):
    #         st.markdown(f"""
    #         | | |
    #         |-|-|
    #         | **AI Recommends** | **{"✅ USE" if auto_recommendation['recommended'] else "⏭️ SKIP"}** |
    #         | **Confidence Level** | {confidence_map.get(auto_recommendation['confidence'], 'Unknown')} |
    #         | **Your Decision** | **{"✅ Enabled" if use_feature_selection else "❌ Disabled"}** |
    #         | **Match Status** | {"✓ Following AI" if use_feature_selection == auto_recommendation['recommended'] else "⚠️ Overriding AI"} |
    #         """)



    #         st.markdown(f"""
    #         | **Metric** | **Value** |
    #         |------------|-----------|
    #         | Total Features | `{str(n_features)}` |
    #         | Total Samples| `{str(n_samples)}` |
    #         | Samples/Feature | `{ratio:.1f}` |
    #         """)

    def _display_normalisation_results(self, original_df: pd.DataFrame, 
                                      normalised_df: pd.DataFrame, 
                                      numeric_columns: List[str]):
        
        """Normalisation Results"""
        #st.subheader("✅ Normalisation Results")
        st.markdown("##### Normalisation Results")
        
        # Summary statistics
        summary = self.agent.get_normalisation_summary(original_df, normalised_df, numeric_columns)
        
        st.dataframe(summary.style.format(precision=4))
        
        # Preview
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Original Data**")
            st.dataframe(original_df[numeric_columns].head())
        with col2:
            st.write("**Normalised Data**")
            st.dataframe(normalised_df[numeric_columns].head())
        
        # Download
        st.download_button(
            label="📥 Download Normalised Data",
            data=normalised_df.to_csv(index=False),
            file_name="normalised_data.csv",
            mime="text/csv"
        )
