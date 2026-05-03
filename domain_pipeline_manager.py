import streamlit as st
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd

class DomainPipelineManager:
    """Manages domain-specific pipeline configurations and general pipeline saving"""
    
    def __init__(self):
        self.domains_file = "domain_pipelines.json"
        self.general_file = "saved_pipelines.json"
        self.domains = self.load_domains()
        self.general_pipelines = self.load_general_pipelines()
    
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
            'fe_active_order': st.session_state.get('fe_active_order', []),
            'fe_checkbox_states': st.session_state.get('fe_checkbox_states', {}),
            'fe_rename_mappings': st.session_state.get('fe_rename_mappings_Feature Engineering', {}),
            'anomaly_features': st.session_state.get('anomaly_features_prev', []),
            'anomaly_models': st.session_state.get('anomaly_models_prev', []),
            'inflection_method': st.session_state.get('inflection_method', 'underscore'),
            'human_ai_checkbox_states': st.session_state.get('human_ai_checkbox_states', {}),
            'prompt_template': st.session_state.get('infl_prompt_template', ''),
            'time_series_evaluation': st.session_state.get('timeseries_checkbox_states', {}),
            'standardize_column': st.session_state.get('standardize_column', None),
            'missing_data_config': st.session_state.get('missing_data_config', {}),
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
        
        # Apply main pipeline steps
        if 'active_steps' in config:
            st.session_state.active_steps_order = config['active_steps']
        
        if 'checkbox_states' in config:
            st.session_state.checkbox_states = config['checkbox_states']
        
        if 'radio_selections' in config:
            st.session_state.radio_selections = config['radio_selections']
        
        # Apply feature engineering settings
        if 'fe_active_order' in config:
            st.session_state.fe_active_order = config['fe_active_order']
        
        if 'fe_checkbox_states' in config:
            st.session_state.fe_checkbox_states = config['fe_checkbox_states']
        
        if 'fe_rename_mappings' in config:
            st.session_state['fe_rename_mappings_Feature Engineering'] = config['fe_rename_mappings']
        
        # Apply anomaly detection settings
        if 'anomaly_features' in config:
            st.session_state.anomaly_features_prev = config['anomaly_features']
        
        if 'anomaly_models' in config:
            st.session_state.anomaly_models_prev = config['anomaly_models']
        
        # Apply inflection settings
        if 'inflection_method' in config:
            st.session_state.inflection_method = config['inflection_method']

        if 'human_ai_checkbox_states' in config:
            st.session_state.human_ai_checkbox_states = config['human_ai_checkbox_states']

        if 'prompt_template' in config:
            st.session_state.infl_prompt_template = config['prompt_template']
        
        # Apply other settings
        if 'standardize_column' in config:
            st.session_state.standardize_column = config['standardize_column']
        
        if 'missing_data_config' in config:
            st.session_state.missing_data_config = config['missing_data_config']
        
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


def domain_pipeline_ui():
    """Streamlit UI for domain pipeline management"""
    
    st.markdown("## 🏗️ Domain Pipeline Management")
    st.markdown("Save and load pipeline configurations by domain or as general templates")
    
    # Initialize manager
    if 'domain_manager' not in st.session_state:
        st.session_state.domain_manager = DomainPipelineManager()
    
    manager = st.session_state.domain_manager
    
    # Tabs for different operations
    tab1, tab2, tab3, tab4 = st.tabs(["💾 Save Pipeline", "📂 Load Pipeline", "🗂️ Manage Domains", "⚙️ Manage General"])
    
    with tab1:
        st.markdown("### Save Current Pipeline Configuration")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("#### Domain-Specific Pipeline")
            domain_name = st.text_input("Domain Name (e.g., Medical, Finance, Retail)", 
                                      placeholder="Enter domain name")
            domain_desc = st.text_area("Description", placeholder="Describe this domain's processing needs")
            
            if st.button("💾 Save Domain Pipeline", type="primary"):
                if domain_name.strip():
                    manager.save_domain_pipeline(domain_name.strip(), domain_desc.strip())
                else:
                    st.error("Please enter a domain name")
        
        with col2:
            st.markdown("#### General Pipeline Template")
            general_name = st.text_input("Template Name", placeholder="Enter template name")
            general_desc = st.text_area("Description", placeholder="Describe this template")
            
            if st.button("💾 Save General Template"):
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
                selected_domain = st.selectbox("Select Domain", domains, format_func=manager.get_domain_info)
                
                if st.button("📂 Load Domain Pipeline", type="primary"):
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
                selected_pipeline = st.selectbox("Select Template", general_pipelines, format_func=manager.get_general_pipeline_info)
                
                if st.button("📂 Load General Template"):
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
    
    # Current pipeline status
    st.markdown("---")
    st.markdown("### 📊 Current Pipeline Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Active Steps:**")
        active_steps = st.session_state.get('active_steps_order', [])
        if active_steps:
            for i, step in enumerate(active_steps, 1):
                st.markdown(f"{i}. {step}")
        else:
            st.info("No steps active")
    
    with col2:
        st.markdown("**Feature Engineering Sub-steps:**")
        fe_steps = st.session_state.get('fe_active_order', [])
        if fe_steps:
            for i, step in enumerate(fe_steps, 1):
                st.markdown(f"{i}. {step}")
        else:
            st.info("No FE steps active")
    
    # Quick actions
    st.markdown("---")
    st.markdown("### 🚀 Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Reset Pipeline", type="secondary"):
            # Reset all pipeline state
            st.session_state.active_steps_order = []
            st.session_state.checkbox_states = {step: False for step in st.session_state.get('steps', [])}
            st.session_state.radio_selections = {step: "Idle" for step in st.session_state.get('steps', [])}
            st.session_state.fe_active_order = []
            st.session_state.fe_checkbox_states = {}
            st.session_state.timeseries_checkbox_states = {}
            st.session_state.human_ai_checkbox_states = {}
            st.success("Pipeline reset to initial state")
    
    with col2:
        if st.button("📋 View Current Config"):
            config = manager.get_current_pipeline_config()
            with st.expander("Current Pipeline Configuration"):
                st.json(config)
    
    with col3:
        if st.button("💾 Save as 'Default'", type="primary"):
            manager.save_general_pipeline("Default", "Default pipeline configuration")


# Add to the main app
def add_domain_pipeline_section():
    """Add domain pipeline management to the main app"""
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 🏗️ Pipeline Management")
    
    if st.sidebar.button("Pipeline Manager", type="primary"):
        st.session_state.show_pipeline_manager = True
    
    if st.sidebar.button("Reset Pipeline", type="secondary"):
        # Reset pipeline state
        if 'active_steps_order' in st.session_state:
            st.session_state.active_steps_order = []
        if 'checkbox_states' in st.session_state:
            st.session_state.checkbox_states = {step: False for step in st.session_state.get('steps', [])}
        if 'radio_selections' in st.session_state:
            st.session_state.radio_selections = {step: "Idle" for step in st.session_state.get('steps', [])}
        if 'fe_active_order' in st.session_state:
            st.session_state.fe_active_order = []
        if 'fe_checkbox_states' in st.session_state:
            st.session_state.fe_checkbox_states = {}
        if 'timeseries_checkbox_states' in st.session_state:
            st.session_state.timeseries_checkbox_states = {}
        if 'human_ai_checkbox_states' in st.session_state:
            st.session_state.human_ai_checkbox_states = {}
        st.success("Pipeline reset to initial state")
    
    # Show pipeline manager if requested
    if st.session_state.get('show_pipeline_manager', False):
        domain_pipeline_ui()
        if st.button("Close Pipeline Manager"):
            st.session_state.show_pipeline_manager = False
            st.rerun()
