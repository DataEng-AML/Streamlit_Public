import streamlit as st
import pandas as pd
from typing import Dict, List, Any, Optional
import json
import os
from datetime import datetime

class EnhancedPipelineManager:
    """Enhanced pipeline manager with sub-steps for all major steps"""
    
    def __init__(self):
        self.domains_file = "domain_pipelines.json"
        self.general_file = "saved_pipelines.json"
        self.domains = self.load_domains()
        self.general_pipelines = self.load_general_pipelines()
        
        # Define sub-steps for each major step
        self.step_substeps = {
            "Data Standardization with AI": [
                "Auto Standardize All Columns", "Manual Column Selection"
            ],
            "Feature Engineering": [
                "Add Headers", "Remove Duplicates", "Assign Data Types", 
                "Create New Feature", "Rename Features", "View Interval Records", 
                "Subset Dataframe", "Delete Records", "Delete Columns", "Edit Values", "Feature Engineering with LLM"
     
            ],
            "Missing Data": [
                "Detect Missing Data", "Display Missing Data", 
                "Fix Missing Data with AI", "Fix Missing Data without AI",
                "Replace Missing Data", "Smart Contextual Fill"
            ],
            "Standardize Column Data": [
                "Standardize Single Column", "Standardize Multiple Columns",
                "Missing Data Standardization"
            ],
            "Inflection AI Standard Feature Name": [
                "Underscore Conversion", "Camelize Conversion", 
                "Dasherize Conversion", "Humanize Conversion",
                "Titleize Conversion", "Pluralize/Singularize"
            ],
            "Human AI Standard Feature Values": [
                "Human-AI Standardization", "Manual Value Standardization"
            ],
            "Data Anomaly Evaluation": [
                "Anomaly Detection", "Anomaly Dictionary", 
                "Model Anomaly Dictionary", "Batch Anomaly Fix",
                "Human-AI Anomaly Review", "Anomaly Visualization"
            ],
            "Time Series Evaluation": [
                "Time Series Data Review", "Time Series Analysis",
                    "Time Series Change Point Detection", "Time Series Protocol Change Analysis", 
                    "Time Series Decomposition", "Time Series Stationarity",
                    "Time Series Forecasting"
            ],
            "Data Normalisation": [
                "Min-Max Normalization", "Z-Score Normalization",
                "Decimal Scaling", "Custom Normalization Rules"
            ]
        }
        
        # Initialize session state for all sub-steps
        self.initialize_substep_states()
    
    def initialize_substep_states(self):
        """Initialize session state for all sub-steps"""
        for step, substeps in self.step_substeps.items():
            step_key = step.replace(" ", "_").lower()
            
            if f'{step_key}_sub_steps' not in st.session_state:
                st.session_state[f'{step_key}_sub_steps'] = substeps
            
            if f'{step_key}_checkbox_states' not in st.session_state:
                st.session_state[f'{step_key}_checkbox_states'] = {sub_step: False for sub_step in substeps}
            
            if f'{step_key}_active_order' not in st.session_state:
                st.session_state[f'{step_key}_active_order'] = []
    
    def load_domains(self) -> Dict[str, Any]:
        """Load domain-specific pipeline configurations"""
        if os.path.exists(self.domains_file):
            try:
                with open(self.domains_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def load_general_pipelines(self) -> Dict[str, Any]:
        """Load general pipeline configurations"""
        if os.path.exists(self.general_file):
            try:
                with open(self.general_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def save_domains(self):
        """Save domain configurations to file"""
        with open(self.domains_file, 'w') as f:
            json.dump(self.domains, f, indent=2)
    
    def save_general_pipelines(self):
        """Save general pipeline configurations to file"""
        with open(self.general_file, 'w') as f:
            json.dump(self.general_pipelines, f, indent=2)
    
    def get_current_pipeline_config(self) -> Dict[str, Any]:
        """Extract current pipeline configuration from session state"""
        config = {
            'timestamp': datetime.now().isoformat(),
            'active_steps': st.session_state.get('active_steps_order', []),
            'checkbox_states': st.session_state.get('checkbox_states', {}),
            'radio_selections': st.session_state.get('radio_selections', {}),
        }
        
        # Add all sub-step configurations
        for step, substeps in self.step_substeps.items():
            step_key = step.replace(" ", "_").lower()
            
            config[f'{step_key}_active_order'] = st.session_state.get(f'{step_key}_active_order', [])
            config[f'{step_key}_checkbox_states'] = st.session_state.get(f'{step_key}_checkbox_states', {})
            
            # Add step-specific configurations
            if step == "Data Standardization with AI":
                config['standardization_config'] = {
                    'selected_columns': st.session_state.get('selected_columns', []),
                    'user_prompt': st.session_state.get('user_prompt', ''),
                    'auto_standardize': st.session_state.get('auto_standardize', True)
                }

            elif step == "Feature Engineering":
                config['feature_engineering_config'] = {
                    'active_features': st.session_state.get('fe_active_features', []),
                    'feature_params': st.session_state.get('fe_feature_params', {}),
                    'selected_columns': st.session_state.get('fe_selected_columns', [])
                }
            elif step == "Missing Data":
                config['missing_data_config'] = {
                    'target_columns': st.session_state.get('target_columns', []),
                    'process_method': st.session_state.get('process_method', 'replace'),
                    'custom_value': st.session_state.get('custom_value', '')
                }
            elif step == "Standardize Column Data":
                config['column_standardization_config'] = {
                    'column': st.session_state.get('column', ''),
                    'reference_column': st.session_state.get('reference_column', ''),
                    'prompt_template': st.session_state.get('prompt_template', '')
                }
            elif step == "Inflection AI Standard Feature Name":
                config['inflection_config'] = {
                    'inflection_method': st.session_state.get('inflection_method', 'underscore'),
                    'prompt_template': st.session_state.get('infl_prompt_template', ''),
                    'standard_names_df': st.session_state.get('standard_names_df', {}).to_dict() if 'standard_names_df' in st.session_state else {}
                }
            elif step == "Human AI Standard Feature Values":
                config['human_ai_config'] = {
                    'selected_column': st.session_state.get('selected_column', ''),
                    'reference_column': st.session_state.get('reference_column', ''),
                    'prompt_template': st.session_state.get('prompt_template', '')
                }
            elif step == "Data Anomaly Evaluation":
                config['anomaly_config'] = {
                    'features': st.session_state.get('anomaly_features_prev', []),
                    'models': st.session_state.get('anomaly_models_prev', []),
                    'anomaly_dict': st.session_state.get('anomaly_dict', {}),
                    'model_anomaly_dict': st.session_state.get('model_anomaly_dict', {})
                }
            elif step == "Time Series Evaluation":
                config['timeseries_config'] = {
                    'selected_features': st.session_state.get('timeseries_selected_features', []),
                    'analysis_type': st.session_state.get('timeseries_analysis_type', 'statistics'),
                    'structural_model': st.session_state.get('timeseries_structural_model', 'l2'),
                    'decomposition_model': st.session_state.get('timeseries_decomposition_model', 'additive'),
                    'stationarity_test': st.session_state.get('timeseries_stationarity_test', 'adfuller'),
                    'stationarity_threshold': st.session_state.get('timeseries_stationarity_threshold', 0.05),
                    'seasonality': st.session_state.get('timeseries_seasonality', 0),
                    'seasonality_threshold': st.session_state.get('timeseries_seasonality_threshold', 0.05),
                    'autocorrelation_threshold': st.session_state.get('timeseries_autocorrelation_threshold', 0.05),
                    'seasonal_autocorrelation_threshold': st.session_state.get('timeseries_seasonal_autocorrelation_threshold', 0.05),
                    'seasonal_period': st.session_state.get('timeseries_seasonal_period', 0),
                    'seasonal_period_threshold': st.session_state.get('timeseries_seasonal_period_threshold', 0.05)
                }


            elif step == "Data Normalisation":
                config['normalization_config'] = {
                    'normalization_type': st.session_state.get('normalization_type', 'minmax'),
                    'columns_to_normalize': st.session_state.get('columns_to_normalize', []),
                    'custom_rules': st.session_state.get('custom_rules', {})
                }
        
        return config
    
    def save_domain_pipeline(self, domain_name: str, description: str = ""):
        """Save current pipeline configuration for a specific domain"""
        config = self.get_current_pipeline_config()
        config['description'] = description
        config['domain'] = domain_name
        
        if domain_name not in self.domains:
            self.domains[domain_name] = []
        
        # Check if this domain already has a configuration
        existing_index = None
        for i, existing_config in enumerate(self.domains[domain_name]):
            if existing_config.get('domain') == domain_name:
                existing_index = i
                break
        
        if existing_index is not None:
            # Update existing configuration
            self.domains[domain_name][existing_index] = config
            st.success(f"✅ Updated pipeline configuration for domain: **{domain_name}**")
        else:
            # Add new configuration
            self.domains[domain_name].append(config)
            st.success(f"✅ Saved pipeline configuration for domain: **{domain_name}**")

        def convert_df(obj):
            if isinstance(obj, pd.DataFrame):
                return obj.to_dict('records')  # or obj.to_json()
            elif isinstance(obj, dict):
                return {k: convert_df(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_df(item) for item in obj]
            return obj
        
        pipeline_data = convert_df(pipeline_data)
        
        self.domains[domain_name] = pipeline_data


        self.save_domains() 
    
    def save_general_pipeline(self, pipeline_name: str, description: str = ""):
        """Save current pipeline configuration as a general pipeline"""
        config = self.get_current_pipeline_config()
        config['description'] = description
        config['name'] = pipeline_name
        
        self.general_pipelines[pipeline_name] = config
        st.success(f"✅ Saved general pipeline: **{pipeline_name}**")
        self.save_general_pipelines()
    
    def load_domain_pipeline(self, domain_name: str) -> Optional[Dict[str, Any]]:
        """Load pipeline configuration for a specific domain"""
        if domain_name in self.domains and self.domains[domain_name]:
            # Return the latest configuration for this domain
            return self.domains[domain_name][-1]
        return None
    
    def load_general_pipeline(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """Load a specific general pipeline configuration"""
        return self.general_pipelines.get(pipeline_name)
    
    def apply_pipeline_config(self, config: Dict[str, Any]):
        """Apply a saved pipeline configuration to current session state"""
        if not config:
            return False
        
        # Mapping of step names to their actual session state keys used in test101.py
        step_key_mapping = {
            "Feature Engineering": "fe_active_order",
            "Data Standardization with AI": "standardization_active_order",
            "Standardize Column Data": "column_standardization_active_order",
            "Inflection AI Standard Feature Name": "inflection_standardization_active_order",
            "Human AI Standard Feature Values": "human_ai_standardization_active_order",
            "Data Anomaly Evaluation": "anomaly_active_order",
            "Time Series Evaluation": "timeseries_active_order",
            "Data Normalisation": "normalization_active_order",
            "Missing Data": "missing_data_active_order",  # This one matches the generated key
        }
        
        # Also mapping for checkbox states
        checkbox_key_mapping = {
            "Feature Engineering": "fe_checkbox_states",
            "Data Standardization with AI": "standardization_checkbox_states",
            "Standardize Column Data": "column_standardization_checkbox_states",
            "Inflection AI Standard Feature Name": "inflection_standardization_checkbox_states",
            "Human AI Standard Feature Values": "human_ai_standardization_checkbox_states",
            "Data Anomaly Evaluation": "anomaly_checkbox_states",
            "Time Series Evaluation": "timeseries_checkbox_states",
            "Data Normalisation": "normalization_checkbox_states",
            "Missing Data": "missing_data_checkbox_states",
        }
        
        # Apply main pipeline steps
        if 'active_steps' in config:
            st.session_state.active_steps_order = config['active_steps']
        
        if 'checkbox_states' in config:
            st.session_state.checkbox_states = config['checkbox_states']
        
        if 'radio_selections' in config:
            st.session_state.radio_selections = config['radio_selections']
        
        # Apply all sub-step configurations using the correct keys
        for step, substeps in self.step_substeps.items():
            step_key = step.replace(" ", "_").lower()
            
            # Get the actual key used in test101.py
            actual_active_key = step_key_mapping.get(step, f'{step_key}_active_order')
            actual_checkbox_key = checkbox_key_mapping.get(step, f'{step_key}_checkbox_states')
            
            # Apply active order
            if f'{step_key}_active_order' in config:
                st.session_state[actual_active_key] = config[f'{step_key}_active_order']
            
            # Apply checkbox states
            if f'{step_key}_checkbox_states' in config:
                st.session_state[actual_checkbox_key] = config[f'{step_key}_checkbox_states']
            
            # Apply step-specific configurations
            if step == "Data Standardization with AI" and 'standardization_config' in config:
                std_config = config['standardization_config']
                st.session_state.selected_columns = std_config.get('selected_columns', [])
                st.session_state.user_prompt = std_config.get('user_prompt', '')
                st.session_state.auto_standardize = std_config.get('auto_standardize', True)


            elif step == "Missing Data" and 'missing_data_config' in config:
                md_config = config['missing_data_config']
                st.session_state.target_columns = md_config.get('target_columns', [])
                st.session_state.process_method = md_config.get('process_method', 'replace')
                st.session_state.custom_value = md_config.get('custom_value', '')
            
            elif step == "Standardize Column Data" and 'column_standardization_config' in config:
                cs_config = config['column_standardization_config']
                st.session_state.column = cs_config.get('column', '')
                st.session_state.reference_column = cs_config.get('reference_column', '')
                st.session_state.prompt_template = cs_config.get('prompt_template', '')
            
            elif step == "Inflection AI Standard Feature Name" and 'inflection_config' in config:
                inf_config = config['inflection_config']
                st.session_state.inflection_method = inf_config.get('inflection_method', 'underscore')
                st.session_state.infl_prompt_template = inf_config.get('prompt_template', '')
                if 'standard_names_df' in inf_config:
                    st.session_state.standard_names_df = pd.DataFrame(inf_config['standard_names_df'])
            
            elif step == "Human AI Standard Feature Values" and 'human_ai_config' in config:
                hai_config = config['human_ai_config']
                st.session_state.selected_column = hai_config.get('selected_column', '')
                st.session_state.reference_column = hai_config.get('reference_column', '')
                st.session_state.prompt_template = hai_config.get('prompt_template', '')
            
            elif step == "Data Anomaly Evaluation" and 'anomaly_config' in config:
                anom_config = config['anomaly_config']
                st.session_state.anomaly_features_prev = anom_config.get('features', [])
                st.session_state.anomaly_models_prev = anom_config.get('models', [])
                if 'anomaly_dict' in anom_config:
                    st.session_state.anomaly_dict = anom_config['anomaly_dict']
                if 'model_anomaly_dict' in anom_config:
                    st.session_state.model_anomaly_dict = anom_config['model_anomaly_dict']

            elif step == "Time Series Evaluation" and 'timeseries_config' in config:
                ts_config = config['timeseries_config']
                st.session_state.timeseries_selected_features = ts_config.get('selected_features', [])
                st.session_state.timeseries_analysis_type = ts_config.get('analysis_type', 'statistics')
                st.session_state.timeseries_structural_model = ts_config.get('structural_model', 'l2')
                st.session_state.timeseries_decomposition_model = ts_config.get('decomposition_model', 'additive')
                st.session_state.timeseries_stationarity_test = ts_config.get('stationarity_test', 'adfuller')
                st.session_state.timeseries_stationarity_threshold = ts_config.get('stationarity_threshold', 0.05)
                st.session_state.timeseries_seasonality = ts_config.get('seasonality', 0)
                st.session_state.timeseries_seasonality_threshold = ts_config.get('seasonality_threshold', 0.05)
                st.session_state.timeseries_autocorrelation_threshold = ts_config.get('autocorrelation_threshold', 0.05)
                st.session_state.timeseries_seasonal_autocorrelation_threshold = ts_config.get('seasonal_autocorrelation_threshold', 0.05)
                st.session_state.timeseries_seasonal_period = ts_config.get('seasonal_period', 0)
                st.session_state.timeseries_seasonal_period_threshold = ts_config.get('seasonal_period_threshold', 0.05)

            elif step == "Data Normalisation" and 'normalization_config' in config:
                norm_config = config['normalization_config']
                st.session_state.normalization_type = norm_config.get('normalization_type', 'minmax')
                st.session_state.columns_to_normalize = norm_config.get('columns_to_normalize', [])
                st.session_state.custom_rules = norm_config.get('custom_rules', {})
        
        st.success("✅ Pipeline configuration applied successfully!")
        return True
    
    def list_domains(self) -> List[str]:
        """Get list of all saved domains"""
        return list(self.domains.keys())
    
    def list_general_pipelines(self) -> List[str]:
        """Get list of all saved general pipelines"""
        return list(self.general_pipelines.keys())
    
    def get_domain_info(self, domain_name: str) -> str:
        """Get information about a domain's pipeline"""
        if domain_name in self.domains and self.domains[domain_name]:
            config = self.domains[domain_name][-1]
            timestamp = config.get('timestamp', 'Unknown')
            description = config.get('description', 'No description')
            steps = len(config.get('active_steps', []))
            return f"**{domain_name}** - {description} (Updated: {timestamp.split('T')[0]}, {steps} steps)"
        return f"**{domain_name}** - No configuration saved"
    
    def get_general_pipeline_info(self, pipeline_name: str) -> str:
        """Get information about a general pipeline"""
        if pipeline_name in self.general_pipelines:
            config = self.general_pipelines[pipeline_name]
            timestamp = config.get('timestamp', 'Unknown')
            description = config.get('description', 'No description')
            steps = len(config.get('active_steps', []))
            return f"**{pipeline_name}** - {description} (Updated: {timestamp.split('T')[0]}, {steps} steps)"
        return f"**{pipeline_name}** - No configuration saved"
    
    def delete_domain(self, domain_name: str):
        """Delete a domain and all its configurations"""
        if domain_name in self.domains:
            del self.domains[domain_name]
            self.save_domains()
            st.success(f"✅ Deleted domain: **{domain_name}**")
    
    def delete_general_pipeline(self, pipeline_name: str):
        """Delete a general pipeline"""
        if pipeline_name in self.general_pipelines:
            del self.general_pipelines[pipeline_name]
            self.save_general_pipelines()
            st.success(f"✅ Deleted pipeline: **{pipeline_name}**")
    
    def apply_pipeline_to_dataframe(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply pipeline configuration to a DataFrame"""
        if not config:
            return df
        
        processed_df = df.copy()
        
        # Apply main steps in order
        active_steps = config.get('active_steps', [])
        
        for step in active_steps:
            step_key = step.replace(" ", "_").lower()
            active_substeps = config.get(f'{step_key}_active_order', [])
            
            if not active_substeps:
                continue
            
            st.info(f"Processing: {step}")
            
            # Apply sub-steps for each major step
            if step == "Data Standardization with AI":
                for substep in active_substeps:
                    if substep == "Auto Standardize All Columns":
                        # Apply auto standardization
                        try:
                            from crewai_data_standardization_agent import DataStandardizerAI
                            data_standardizer = DataStandardizerAI(use_ai=True)
                            processed_df = data_standardizer.auto_standardize_dataframe(processed_df, processed_df.columns.tolist())
                        except Exception as e:
                            st.warning(f"Auto standardization failed: {e}")
                    
                    elif substep == "Manual Column Selection":
                        # Apply manual column selection
                        selected_cols = config.get('standardization_config', {}).get('selected_columns', [])
                        if selected_cols:
                            try:
                                from crewai_data_standardization_agent import DataStandardizerAI
                                data_standardizer = DataStandardizerAI(use_ai=True)
                                processed_df = data_standardizer.auto_standardize_dataframe(processed_df, selected_cols)
                            except Exception as e:
                                st.warning(f"Manual column standardization failed: {e}")

            elif step == "Feature Engineering":
                for substep in active_substeps:
                    if substep == "Add Headers":
                        # Add headers as first row
                        try:
                            from feature_engineering_agent import FeatureEngineeringAgent
                            fe_agent = FeatureEngineeringAgent()
                            processed_df = fe_agent.add_headers_as_row(processed_df)
                        except Exception as e:
                            st.warning(f"Adding headers failed: {e}")
                    
                    elif substep == "Remove Duplicates":
                        processed_df = processed_df.drop_duplicates()
                    
                    elif substep == "Assign Data Types":
                        try:
                            from feature_engineering_agent import FeatureEngineeringAgent
                            fe_agent = FeatureEngineeringAgent()
                            processed_df = fe_agent.assign_data_types(processed_df)
                        except Exception as e:
                            st.warning(f"Assigning data types failed: {e}")
                    
                    elif substep == "Create New Feature":
                        try:
                            from feature_engineering_agent import FeatureEngineeringAgent
                            fe_agent = FeatureEngineeringAgent()
                            processed_df = fe_agent.create_new_feature_ui(processed_df)
                        except Exception as e:
                            st.warning(f"Creating new feature failed: {e}")
                    
                    elif substep == "Rename Features":
                        try:
                            from feature_engineering_agent import FeatureEngineeringAgent
                            fe_agent = FeatureEngineeringAgent()
                            processed_df = fe_agent.rename_features_ui(processed_df)
                        except Exception as e:
                            st.warning(f"Renaming features failed: {e}")



            elif step == "Missing Data":
                for substep in active_substeps:
                    if substep == "Detect Missing Data":
                        # Detect missing data
                        missing_count = processed_df.isnull().sum()
                        st.write(f"Missing data detected: {missing_count.sum()} total missing values")
                    
                    elif substep == "Fix Missing Data with AI":
                        # Apply AI-based missing data fixing
                        try:
                            from smart_missing_data_filler_agent import SmartMissingDataFiller
                            filler = SmartMissingDataFiller()
                            processed_df = filler.smart_contextual_fill_ui(processed_df)
                        except Exception as e:
                            st.warning(f"AI missing data fixing failed: {e}")
                    
                    elif substep == "Replace Missing Data":
                        # Apply replacement
                        replace_value = config.get('missing_data_config', {}).get('custom_value', 'NoData')
                        processed_df = processed_df.fillna(replace_value)
            
            elif step == "Standardize Column Data":
                for substep in active_substeps:
                    if substep == "Standardize Single Column":
                        column = config.get('column_standardization_config', {}).get('column', '')
                        if column and column in processed_df.columns:
                            try:
                                from column_standardization_agent import ColumnStandardizationAgent
                                col_std = ColumnStandardizationAgent()
                                processed_df[column] = col_std.standardize_column(
                                    processed_df, column, column, 'Provide standardized value'
                                )
                            except Exception as e:
                                st.warning(f"Column standardization failed: {e}")
            
            elif step == "Inflection AI Standard Feature Name":
                for substep in active_substeps:
                    if substep == "Underscore Conversion":
                        import inflection
                        processed_df.columns = [inflection.underscore(col) for col in processed_df.columns]
                    elif substep == "Camelize Conversion":
                        import inflection
                        processed_df.columns = [inflection.camelize(col) for col in processed_df.columns]

            elif step == "Time Series Evaluation":
                for substep in active_substeps:
                    if substep == "Time Series Data Review":
                        try:
                            from timeseries_evaluation_agent import TimeSeriesEvaluationAgent
                            ts_agent = TimeSeriesEvaluationAgent()
                            ts_agent.time_series_data_review(processed_df)
                        except Exception as e:
                            st.warning(f"Time series data review failed: {e}")

            elif step == "Data Anomaly Evaluation":
                for substep in active_substeps:
                    if substep == "Anomaly Detection":
                        try:
                            from wrapper_data_anomaly_agent import WrapperDataAnomalyAgent
                            if "anomaly_agent" in st.session_state:
                                del st.session_state["anomaly_agent"]

                            anomaly_agent = WrapperDataAnomalyAgent()
                            features = config.get('anomaly_config', {}).get('features', [])
                            models = config.get('anomaly_config', {}).get('models', ['zscore'])
                            if features:
                                processed_df = anomaly_agent.detect_anomalies(processed_df, features, models)
                        except Exception as e:
                            st.warning(f"Anomaly detection failed: {e}")
            
            elif step == "Data Normalisation":
                for substep in active_substeps:
                    if substep == "Min-Max Normalization":
                        from sklearn.preprocessing import MinMaxScaler
                        scaler = MinMaxScaler()
                        numeric_cols = processed_df.select_dtypes(include=['float64', 'int64']).columns
                        if len(numeric_cols) > 0:
                            processed_df[numeric_cols] = scaler.fit_transform(processed_df[numeric_cols])
                    elif substep == "Z-Score Normalization":
                        from sklearn.preprocessing import StandardScaler
                        scaler = StandardScaler()
                        numeric_cols = processed_df.select_dtypes(include=['float64', 'int64']).columns
                        if len(numeric_cols) > 0:
                            processed_df[numeric_cols] = scaler.fit_transform(processed_df[numeric_cols])
        
        return processed_df


def display_current_pipeline_status():
    """Display current pipeline status with sub-steps - can be called from anywhere"""
    st.markdown("---")
    st.markdown("### Current Pipeline Status")
    
    # Mapping of step names to their actual session state keys used in test101.py
    step_key_mapping = {
        "Feature Engineering": "fe_active_order",
        "Data Standardization with AI": "standardization_active_order",
        "Standardize Column Data": "column_standardization_active_order",
        "Inflection AI Standard Feature Name": "inflection_standardization_active_order",
        "Human AI Standard Feature Values": "human_ai_standardization_active_order",
        "Data Anomaly Evaluation": "anomaly_active_order",
        "Time Series Evaluation": "timeseries_active_order",
        "Data Normalisation": "normalization_active_order",
        "Missing Data": "missing_data_active_order",  # This one matches the generated key
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Active Steps:**")
        active_steps = st.session_state.get('active_steps_order', [])
        if active_steps:
            for i, step in enumerate(active_steps, 1):
                st.markdown(f"{i}. {step}")
                # Show sub-steps if any
                # Use the mapping if available, otherwise generate the key
                if step in step_key_mapping:
                    session_key = step_key_mapping[step]
                else:
                    step_key = step.replace(" ", "_").lower()
                    session_key = f'{step_key}_active_order'
                
                active_substeps = st.session_state.get(session_key, [])
                if active_substeps:
                    for j, substep in enumerate(active_substeps, 1):
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;{j}. {substep}", unsafe_allow_html=True)
        else:
            st.info("No steps active")
    
    with col2:
        st.markdown("**Sub-step Configuration:**")
        if 'enhanced_domain_manager' in st.session_state:
            manager = st.session_state.enhanced_domain_manager
            for step, substeps in manager.step_substeps.items():
                # Use the mapping if available, otherwise generate the key
                if step in step_key_mapping:
                    session_key = step_key_mapping[step]
                else:
                    step_key = step.replace(" ", "_").lower()
                    session_key = f'{step_key}_active_order'
                
                active_substeps = st.session_state.get(session_key, [])
                st.markdown(f"**{step}:** {len(active_substeps)}/{len(substeps)} sub-steps active")
                if active_substeps:
                    for substep in active_substeps:
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;✓ {substep}", unsafe_allow_html=True)



def enhanced_domain_pipeline_ui():
    """Enhanced Streamlit UI for domain pipeline management"""
    
    st.markdown("### Enhanced Domain Pipeline Management")
    st.markdown("Save and load pipeline configurations by domain with full sub-step support")
    
    # Initialize manager
    if 'enhanced_domain_manager' not in st.session_state:
        st.session_state.enhanced_domain_manager = EnhancedPipelineManager()
    
    manager = st.session_state.enhanced_domain_manager
    
    # *** SHOW CURRENT PIPELINE STATUS FIRST (ALWAYS VISIBLE) ***
    display_current_pipeline_status()
    
    # Quick actions (moved up for better visibility)
    st.markdown("---")
    st.markdown("### Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Reset Pipeline", type="secondary"):
            # Reset all pipeline state
            st.session_state.active_steps_order = []
            st.session_state.checkbox_states = {step: False for step in st.session_state.get('steps', [])}
            st.session_state.radio_selections = {step: "Idle" for step in st.session_state.get('steps', [])}
            
            # Reset all sub-step states
            for step, substeps in manager.step_substeps.items():
                step_key = step.replace(" ", "_").lower()
                st.session_state[f'{step_key}_active_order'] = []
                st.session_state[f'{step_key}_checkbox_states'] = {sub_step: False for sub_step in substeps}
            
            st.success("Pipeline reset to initial state")
    
    with col2:
        if st.button("📋 View Current Config"):
            config = manager.get_current_pipeline_config()
            with st.expander("Current Pipeline Configuration"):
                st.json(config)
    
    with col3:
        if st.button("💾 Save as 'Default'", type="primary"):
            manager.save_general_pipeline("Default", "Default pipeline configuration")
    
    # Tabs for different operations (after status and quick actions)
    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs(["💾 Save Pipeline", "📂 Load Pipeline", "🗂️ Manage Domains", "⚙️ Manage General"])
    
    with tab1:
        st.markdown("### Save Current Pipeline Configuration")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("#### Domain-Specific Pipeline")
            domain_name = st.text_input("Domain Name (e.g., Medical, Finance, Retail)", 
                                      placeholder="Enter domain name",
                                      key="save_domain_name")
            domain_desc = st.text_area("Description", placeholder="Describe this domain's processing needs",
                                      key="save_domain_desc")
            
            if st.button("💾 Save Domain Pipeline", type="primary", key="btn_save_domain"):
                if domain_name.strip():
                    manager.save_domain_pipeline(domain_name.strip(), domain_desc.strip())
                else:
                    st.error("Please enter a domain name")
        
        with col2:
            st.markdown("#### General Pipeline Template")
            general_name = st.text_input("Template Name", placeholder="Enter template name",
                                        key="save_general_name")
            general_desc = st.text_area("Description", placeholder="Describe this template",
                                       key="save_general_desc")
            
            if st.button("💾 Save General Template", key="btn_save_general"):
                if general_name.strip():
                    manager.save_general_pipeline(general_name.strip(), general_desc.strip())
                else:
                    st.error("Please enter a template name")
    
    with tab2:
        st.markdown("### Load Saved Pipeline Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Load Domain Pipeline")
            domains = manager.list_domains()
            if domains:
                selected_domain = st.selectbox("Select Domain", domains, format_func=manager.get_domain_info,
                                              key="load_domain_select")
                
                if st.button("📂 Load Domain Pipeline", type="primary", key="btn_load_domain"):
                    config = manager.load_domain_pipeline(selected_domain)
                    if config:
                        manager.apply_pipeline_config(config)
                    else:
                        st.error("No configuration found for this domain")
            else:
                st.info("No domain pipelines saved yet. Go to 'Save Pipeline' tab to create one.")
        
        with col2:
            st.markdown("#### Load General Template")
            general_pipelines = manager.list_general_pipelines()
            if general_pipelines:
                selected_pipeline = st.selectbox("Select Template", general_pipelines, format_func=manager.get_general_pipeline_info,
                                               key="load_general_select")
                
                if st.button("📂 Load General Template", key="btn_load_general"):
                    config = manager.load_general_pipeline(selected_pipeline)
                    if config:
                        manager.apply_pipeline_config(config)
                    else:
                        st.error("No configuration found for this template")
            else:
                st.info("No general templates saved yet. Go to 'Save Pipeline' tab to create one.")
    
    with tab3:
        st.markdown("### Manage Domain Pipelines")
        
        domains = manager.list_domains()
        if domains:
            for domain in domains:
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.markdown(manager.get_domain_info(domain))
                
                with col2:
                    if st.button("📂 Load", key=f"load_domain_{domain}"):
                        config = manager.load_domain_pipeline(domain)
                        if config:
                            manager.apply_pipeline_config(config)
                
                with col3:
                    if st.button("🗑️ Delete", key=f"delete_domain_{domain}", type="secondary"):
                        manager.delete_domain(domain)
                        st.rerun()
        else:
            st.info("No domain pipelines saved yet.")
    
    with tab4:
        st.markdown("### Manage General Pipeline Templates")
        
        general_pipelines = manager.list_general_pipelines()
        if general_pipelines:
            for pipeline in general_pipelines:
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.markdown(manager.get_general_pipeline_info(pipeline))
                
                with col2:
                    if st.button("📂 Load", key=f"load_general_{pipeline}"):
                        config = manager.load_general_pipeline(pipeline)
                        if config:
                            manager.apply_pipeline_config(config)
                
                with col3:
                    if st.button("🗑️ Delete", key=f"delete_general_{pipeline}", type="secondary"):
                        manager.delete_general_pipeline(pipeline)
                        st.rerun()
        else:
            st.info("No general templates saved yet.")

# Add to the main app
def add_enhanced_domain_pipeline_section():
    """Add enhanced domain pipeline management to the main app"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Enhanced Pipeline Management")
    
    if st.sidebar.button("Enhanced Pipeline Manager", type="primary"):
        st.session_state.show_enhanced_pipeline_manager = True
    
    if st.sidebar.button("Reset Pipeline", type="secondary"):
        # Reset pipeline state
        if 'active_steps_order' in st.session_state:
            st.session_state.active_steps_order = []
        if 'checkbox_states' in st.session_state:
            st.session_state.checkbox_states = {step: False for step in st.session_state.get('steps', [])}
        if 'radio_selections' in st.session_state:
            st.session_state.radio_selections = {step: "Idle" for step in st.session_state.get('steps', [])}
        
        # Reset all sub-step states
        if 'enhanced_domain_manager' in st.session_state:
            manager = st.session_state.enhanced_domain_manager
            for step, substeps in manager.step_substeps.items():
                step_key = step.replace(" ", "_").lower()
                if f'{step_key}_active_order' in st.session_state:
                    st.session_state[f'{step_key}_active_order'] = []
                if f'{step_key}_checkbox_states' in st.session_state:
                    st.session_state[f'{step_key}_checkbox_states'] = {sub_step: False for sub_step in substeps}
        
        st.success("Pipeline reset to initial state")
    
    # Show enhanced pipeline manager if requested
    if st.session_state.get('show_enhanced_pipeline_manager', False):
        enhanced_domain_pipeline_ui()
        if st.button("Close Enhanced Pipeline Manager"):
            st.session_state.show_enhanced_pipeline_manager = False
            st.rerun()



            