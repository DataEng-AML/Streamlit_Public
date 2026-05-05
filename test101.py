import streamlit as st
# Set page config FIRST - must be the very first Streamlit command
from pipeline_manager import init_pipeline 
st.set_page_config(layout="wide")
# Initialize persistent pipeline
pipeline = init_pipeline()
from pipeline_diagram_agent import PipelineDiagramAgent
from correct_clean_api_agent import CleanAPIAgent as ccapi
from statistics_agent import FeaturesStatisticsAgent
from feature_engineering_agent import FeaturesEngineeringAgent
from preprocess_with_llm_agent import PreprocessWithLLMAgent
from streamlit_app_class_agent import CrewAIPyDEAML
from smart_missing_data_filler_agent import SmartMissingDataFiller
from wrapper_timeseries_evaluation_agent import get_timeseries_wrapper

import typing
from typing import Dict, Any, Optional, Union, List
import dateutil.parser

import plotly.graph_objects as go

from datetime import datetime
from typing import Any, Dict, Literal
from time import perf_counter
import inflection
import pandas as pd
import subprocess
import threading
import queue
import statsmodels.api as sm

###############################
from spellchecker import SpellChecker
import enchant
from collections import Counter

from rapidfuzz import process, fuzz

from streamlit_sortables import sort_items

import re
import nltk
from nltk.corpus import words

########################################################################################################
# dqllm\Scripts\activate

# Airlow Section
# from airflow.api.client.local_client import Client #as airflow_client
# from airflow.models import DagBag
# from airflow.configuration import conf
# from airflow.utils.dates import days_ago
# from airflow.utils.state import State
# import requests
# from requests.auth import HTTPBasicAuth

########################################################################################################

# pip install langchain
#from langchain.prompts import PromptTemplate
from langchain_core.prompts import PromptTemplate

# pip install langchain_experimental
#from langchain_experimental.agents import create_csv_agent
from langchain_experimental.agents.agent_toolkits import create_csv_agent
from langchain_experimental.agents import create_pandas_dataframe_agent
#from langchain.agents import AgentType, AgentExecutor, initialize_agent
#from langchain.agents import AgentExecutor, initialize_agent

#from langchain.agents import AgentExecutor
#from langchain.agents.loading import AgentExecutor
#from langchain.agents.agent_iterator import AgentExecutor 
#from langchain.agents.initializer import initialize_agent
from langchain.agents import initialize_agent
from langchain.agents import AgentExecutor, create_react_agent
from langchain import hub



from langchain_experimental.agents.agent_toolkits import create_python_agent
from langchain_experimental.tools import PythonREPLTool
from langchain.tools import Tool
import io
from io import StringIO

# pip install openai
#from langchain.llms import OpenAI

#pip install -U langchain-community
from langchain_community.llms import OpenAI

# pip install python-dotenv
from dotenv import load_dotenv
import os
import streamlit.components.v1 as components

# pip install chardet
import chardet


# other things to install
# pip install tabulate

# streamlit extras
from streamlit_extras.dataframe_explorer import dataframe_explorer

from IPython import display

from sklearn.impute import KNNImputer
from sklearn.linear_model import LinearRegression

from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer

import sys  # Import sys module
import calendar

import seaborn as sns
import matplotlib.pyplot as plt

# Custom classes to handle individual LADE needs
from phd_custom_lib.perform_eda import perform_eda
from phd_custom_lib.resolve_missing_data import resolve_missing_data
#from phd_custom_lib.dedup_data_operations import dedup_data_operations

import plotly.express as px
import missingno as msno # for missing data
import math

########################################################################################################
# The main project work and libraries to import 
from data_read_agent import DataReadAgent, DataFrameOutput
from missing_data_agent import MissingDataAgent

from human_data_standardization_agent import HumanDataStandardizationAgent # as hdsa
from human_data_standardization_agent import APIAgent

from column_standardization_agent import ColumnStandardizationAgent
#from .correct_clean_api_agent import CleanAPIAgent as ccapi

from wrapper_data_anomaly_agent import WrapperDataAnomalyAgent
from wrapper_missing_data_agent import WrapperMissingDataAgent

from data_normalisation_agent import DataNormalisationAgent
from wrapper_data_normalisation_agent import WrapperDataNormalisationAgent

from crewai_data_standardization_agent import CrewAIStandardizationAgent, DataStandardizerAI

#from pyDEAML import PyDEAML
#pydeaml = PyDEAML.DataReadAgent
import tempfile



######################################

# The pipeline codes here

import graphviz
import webbrowser

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import time
from graphviz import Digraph

from pyvis.network import Network
#from streamlit.components.v1 import html

import uuid
import plotly.graph_objs as go
#import plotly.graph_objects as go

from plotly.subplots import make_subplots

##########################################################
# introduce taipy
import taipy as tp
from taipy import Config
###########################################################

# HTML template for sticky containers
# Constants for margins
MARGINS = {
    "top": "2.875rem",
    "bottom": "0",
}

# HTML template for sticky containers
STICKY_CONTAINER_HTML = """
<style>
div[data-testid="stVerticalBlock"] div:has(div.fixed-header-{i}) {{
    position: sticky;
    {position}: {margin};
    background-color:white !important;
    z-index: 9999;
}}
</style>
<div class='fixed-header-{i}'/>
""".strip()

# Not to apply the same style to multiple containers
count = 0


# Function to create sticky containers
def sticky_container(
    *,
    height: int | None = None,
    border: bool | None = None,
    mode: Literal["top", "bottom"] = "top",
    margin: str | None = None,
):
    if margin is None:
        margin = MARGINS[mode]


    global count
    html_code = STICKY_CONTAINER_HTML.format(position=mode, margin=margin, i=count)
    count += 1

    container = st.container()
    container.markdown(html_code, unsafe_allow_html=True)
    return container



def enable_eda_chkbx():
    if st.session_state.eda_checkbox:
        st.session_state.pipeline_checkbox = False

def enable_pipeline_chkbx():
    if st.session_state.pipeline_checkbox:
        st.session_state.eda_checkbox = False


def smart_missing_data_ui():
    """Streamlit UI for smart missing data filling with multiple columns."""
    
    st.markdown("## Smart Missing Data Filler (Multi-Column)")
    
    if 'df' not in st.session_state or st.session_state.df is None:
        st.warning("Please load data first!")
        return
    
    df = st.session_state.df
    
    # Step 1: Column input
    st.subheader("1. Select Columns")
    
    # Show all columns
    all_columns = list(df.columns)
    missing_cols = [col for col in all_columns if df[col].isnull().any()]
    
    st.write("**Columns with missing data:**")
    if missing_cols:
        cols_per_row = 4
        for i in range(0, len(missing_cols), cols_per_row):
            cols = st.columns(cols_per_row)
            for j in range(cols_per_row):
                if i + j < len(missing_cols):
                    col = missing_cols[i + j]
                    missing_count = df[col].isnull().sum()
                    with cols[j]:
                        st.metric(col, missing_count)
    else:
        st.success("No columns have missing data!")
        return
    
    # Comma-separated input
    target_columns_input = st.text_input(
        "Enter column names (comma-separated):",
        placeholder="e.g., Column1, Column2, Column3",
        help="Enter one or more column names separated by commas"
    )
    
    # OR use multiselect as alternative
    use_multiselect = st.checkbox("Use dropdown selection instead", value=False)
    
    if use_multiselect:
        selected_columns = st.multiselect(
            "Select columns:",
            options=all_columns,
            default=[col for col in missing_cols[:3] if col in all_columns]
        )
        target_columns_input = ", ".join(selected_columns)
    
    if not target_columns_input:
        st.info("Please enter column names to proceed")
        return
    
    # Parse columns
    target_columns = [col.strip() for col in target_columns_input.split(',') if col.strip()]
    
    # Validate
    invalid_cols = [col for col in target_columns if col not in df.columns]
    if invalid_cols:
        st.error(f"Invalid columns: {', '.join(invalid_cols)}")
        st.info(f"Available columns: {', '.join(all_columns[:10])}{'...' if len(all_columns) > 10 else ''}")
        return
    
    
    # Step 2: Configuration
    st.subheader("AI Configuration")
    
    col1, col2= st.columns(2)
    with col1:
        model = st.selectbox(
            "Ollama Model",
            options=["llama2", "mistral", "gemma:2b", "codellama", "neural-chat"],
            index=0
        )
    
    with col2:
        window_size = st.slider(
            "Context Window Size",
            min_value=1,
            max_value=10,
            value=3,
            help="Number of adjacent rows to analyze"
        )
    
    # Step 3: Preview
    if st.button("Preview Analysis", key="preview_multi"):
        with st.spinner("Analyzing columns and relationships..."):
            try:
                filler = SmartMissingDataFiller(model=model)
                analysis = filler.analyze_multiple_columns(df, target_columns)
                
                st.write("**Column Analysis:**")
                st.json(analysis, expanded=False)
                
            except Exception as e:
                st.error(f"Preview failed: {e}")
    
    # Step 4: Fill
    st.subheader("Fill Missing Values")
    
    if st.button("Fill Selected Columns with AI", type="primary"):
        with st.spinner("AI is analyzing patterns across multiple columns..."):
            try:
                filler = SmartMissingDataFiller(model=model)
                result_df = filler.fill_missing_with_context(df, target_columns_input)
                
                # Update session state
                st.session_state.df = result_df
                
                # Show results
                st.success("AI has filled missing values across selected columns!")
                
                # Download
                csv = result_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "Download AI-Filled Data",
                    csv,
                    "ai_filled_multi_columns.csv",
                    "text/csv"
                )
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                st.info("""
                **Troubleshooting:**
                1. Start Ollama: `ollama serve`
                2. Check: `curl http://localhost:11434/api/tags`
                3. Try a smaller set of columns
                """)
                
class PipelineCrewAIPyDEAML:
    def __init__(self):
        self.log = []
        self.data = None  


    def detect_missing_data(self, data):
        """Detects missing data and generates a report."""
        # Always use st.session_state.df as the source
        if 'df' not in st.session_state or st.session_state.df is None:
            st.error("No data available")
            return pd.DataFrame(columns=['Column', 'Number of Missing Data', '% of Missing Data'])
        
        # Ensure data is a DataFrame
        if isinstance(data, dict):
            for key in ['data', 'df', 'dataframe']:
                if key in data and isinstance(data[key], pd.DataFrame):
                    data = data[key]
                    break
            else:
                try:
                    data = pd.DataFrame(data)
                except Exception as e:
                    st.error(f"Cannot convert to DataFrame: {e}")
                    return pd.DataFrame(columns=['Column', 'Number of Missing Data', '% of Missing Data'])

        # Initialize agent if needed
        missing_data_agent = MissingDataAgent()


        # Call the agent
        missing_data_agent.detect_missing_data(data)
        
        # Display the report
        st.markdown("##### Missing Data Report")
        if hasattr(missing_data_agent, 'missing_data_report'):
            st.dataframe(missing_data_agent.missing_data_report)

        # Update the main session state variable
        st.session_state.df = data
        return data



    def display_missing_data(self, data, feature_names):
        """Displays missing data information."""
        if 'df' not in st.session_state or st.session_state.df is None:
            st.error("No data available")
            return pd.DataFrame()
        
        data = st.session_state.df

        # MissingDataAgent instance from session state
        missing_data_agent = MissingDataAgent()

     
        # Call the function
        dis_mis_data = missing_data_agent.display_missing_data(data, feature_names)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Display Missing Data")
        #st.dataframe(missing_data_agent.display_missing_data)

        return dis_mis_data

    def replace_missing_data(self, data):
        # Initialize session state variables if missing
       
        if 'data' in st.session_state:
            data = st.session_state.data
        else:
            st.session_state.data = data.copy(deep=True)
            
        if 'df' not in st.session_state:
            st.session_state.df = data.copy(deep=True)
        if 'fix_history' not in st.session_state:
            st.session_state.fix_history = []

        # MissingDataAgent instance from session state
        missing_data_agent = MissingDataAgent()

        cleaned_missing_data = st.session_state.data.copy(deep=True)

        process = st.text_input("Enter the value to populate all missing data, e.g., NoData:", key="missing_data_input")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("Replace Missing Values", key="replace_missing_values_button"):
                if process:
                    # Save current state for undo
                    st.session_state.fix_history.append(st.session_state.data.copy(deep=True))

                    # Replace missing data in all columns
                    for column in cleaned_missing_data.columns:
                        cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)

                    st.success(f"Missing values replaced with '{process}'")
                    st.session_state.data = cleaned_missing_data
                    #st.dataframe(st.session_state.data)
                else:
                    st.warning("Please enter a value to replace missing data.")

        with col2:
            if st.button("Undo Last Fix", key="undo_last_fix_button"):
                if st.session_state.fix_history:
                    st.session_state.data = st.session_state.fix_history.pop()
                    st.success("Undid last fix")
                    #st.dataframe(st.session_state.data)
                else:
                    st.info("No fixes to undo.")

        with col3:
            if st.button("Undo All Fixes", key="undo_all_fixes_button"):
                st.session_state.data = st.session_state.df.copy(deep=True)
                st.session_state.fix_history.clear()
                st.success("Reverted all fixes to original data")
                #st.dataframe(st.session_state.data)

        with col4:
            if st.button("Save and Apply", key="save_and_apply_button"):
                st.session_state.df = st.session_state.data.copy(deep=True)
                st.session_state.fix_history.clear()
                st.success("Changes saved and permanently applied")
                #st.dataframe(st.session_state.data)

        return st.session_state.data


    def fix_missing_data(self, data):
        wrapper_missingdataagent = WrapperMissingDataAgent()

        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()
            
        # Put this at the top of your main UI section, before column selection
        with st.expander("🔧 AI Imputation Configuration", expanded=False):
            user_description = st.text_area(
                "Enter your custom imputation task description:",
                height=150,
                value="""Given the dataset summary and column with missing data, 
        select the best imputation method and explain your reasoning.""",
                help="Customize how the AI should approach missing data imputation"
            )
            
            # Initialize agent with the description
            if st.button("Update AI Configuration", type="secondary"):
                wrapper_missingdataagent.initialize_agent(user_description)
                st.success("AI configuration updated!")


        # Convert to DataFrame if needed
        if isinstance(data, dict):
            for key in ['data', 'df', 'dataframe']:
                if key in data and isinstance(data[key], pd.DataFrame):
                    data = data[key]
                    break
            else:
                try:
                    data = pd.DataFrame(data)
                except:
                    st.error("Cannot convert input to DataFrame")
                    return None
        
        # Use this DataFrame for everything
        current_df = data.copy(deep=True)
        
        # Initialize session states
        if 'data' not in st.session_state:
            st.session_state.data = current_df
        else:
            # Update if it's a dict
            if isinstance(st.session_state.data, dict):
                st.session_state.data = current_df
        
        if 'df' not in st.session_state:
            st.session_state.df = current_df.copy(deep=True)

#        if 'current_df' not in st.session_state:
#            st.session_state.current_df = current_df.copy(deep=True)
        
        if 'fix_history' not in st.session_state:
            st.session_state.fix_history = []


        # Initialize all session state variables at the start of your app
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = None  # or appropriate initial value

        #if 'df' not in st.session_state:
            #st.session_state.df = None

        # Initialize other agents/variables you might use
        if 'feature_engineering_agent' not in st.session_state:
            st.session_state.feature_engineering_agent = None

        if 'standardize_column_agent' not in st.session_state:
            st.session_state.standardize_column_agent = None



        st.markdown("###### Fix Missing Data With Stats or Fill")
        st.write("These columns have missing data: select a column and a process to fix missing data.")

        # Use current_df instead of st.session_state.data
        columns = [col for col in current_df.columns if current_df[col].isnull().any()]
        
        if not columns:
            st.info("No columns with missing data.")
            return current_df
        
        column = st.selectbox("Select a column to fix missing data", options=columns)

        # remove ai implementation from this one
        processes = [
            'replace', 'mean', 'median', 'mode', 'ffill', 'bfill', 'interpolate',
            'custom_date', 'custom_number', 'custom_text'
        ]
        process = st.selectbox("Choose a process to fix missing data", options=processes)

        custom_value = None
        if process == 'custom_text':
            custom_value = st.text_input(f"Enter text replacement value for column '{column}'")
        elif process == 'custom_number':
            custom_value = st.number_input(f"Enter number replacement value for column '{column}'", format="%g")
        elif process == 'custom_date':
            custom_value = st.date_input(f"Enter date replacement value for column '{column}'")


        col1, col2, col3, col4 = st.columns(4)


        fix_applied = False

        with col1:
            if st.button("Apply Missing Data Fix"):
                fix_applied = True
                # Save current state for undo
                st.session_state.fix_history.append(st.session_state.df.copy(deep=True))


                
                # Apply fix
                fixed_df, explanation = wrapper_missingdataagent.fix_missing_data_with_stats_or_fill(
                    st.session_state.df, column, process, custom_value)
                
                # Update ALL session state variables
                st.session_state.current_df = fixed_df
                st.session_state.df = fixed_df
                st.session_state.data = fixed_df
                
                st.success(f"Applied '{process}' to '{column}'")
                st.info(f"Imputation explanation:\n\n{explanation}")

        # Show data - will show updated if fix was applied
        st.write("##### Data Preview:")
        #st.dataframe(st.session_state.fixed_df if fix_applied else st.session_state.current_df)
        st.dataframe(st.session_state.df.head())


        if 'fixed_df' not in st.session_state:
            st.session_state.fixed_df = fixed_df = st.session_state.df.copy(deep=True)

        # Download button - will use updated data if fix was applied
        amdf_csv = st.session_state.df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Data", 
            amdf_csv, 
            "fixed_missing_data.csv" if fix_applied else "current_data.csv", 
            "text/csv"
        )

        with col2:
            if st.button("Undo Last Fix"):
                if st.session_state.fix_history:
                    last_df = st.session_state.fix_history.pop()
                    current_df = last_df
                    st.session_state.df = last_df  # Update main variable
                    st.success("Undid last fix")
                else:
                    st.info("No fixes to undo.")


        with col3:
            if st.button("Undo All Fixes"):
                current_df = st.session_state.original_fix_data.copy(deep=True)
                st.session_state.df = st.session_state.original_fix_data.copy(deep=True)
                st.session_state.fix_history.clear()
                st.success("Reverted all fixes to original data.")


        with col4:
            # Save and apply permanently to session
            if st.button("Save and Apply"):
                st.session_state.original_fix_data = current_df.copy(deep=True)
                st.session_state.fix_history.clear()
                st.success("Changes saved and permanently applied.")


    st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

# Column standardization

    def standardize_column(self, data, column, reference_column, prompt_template):
        if 'data' in st.session_state:
            data = st.session_state.data
        if 'column' in st.session_state:
            column = st.session_state.column
        if 'reference_column' in st.session_state:
            reference_column = st.session_state.reference_column
        if 'prompt_template' in st.session_state:
            prompt_template = st.session_state.prompt_template

            
        # ColumnStandardizationAgent instance from session state
        col_standardizer = ColumnStandardizationAgent()

        # Call the function
        ai_standardize_column = st.session_state.col_standardizer.standardize_column(data, column, reference_column, prompt_template)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Standardize Column")
        #st.dataframe(st.session_state.col_standardizer.standardize_column)
        
        return ai_standardize_column



    
    def standardize_missing_data(self, data, column, reference_column, prompt_template):
        if 'data' in st.session_state:
            data = st.session_state.data
        if 'column' in st.session_state:
            column = st.session_state.column
        if 'reference_column' in st.session_state:
            data = st.session_state.reference_column
        if 'prompt_template' in st.session_state:
            data = st.session_state.prompt_template

        # Call the function
        ai_standardize_missing_data = st.session_state.col_standardizer.standardize_missing_data(data, column, reference_column, prompt_template)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Standardize Missing Column Data")
        #st.dataframe(st.session_state.col_standardizer.standardize_column)
        
        return ai_standardize_missing_data



    def inflection_ai_standard_feature_name(self, data):
        if 'data' in st.session_state:
            data = st.session_state.data
        if 'prompt_template' in st.session_state:
            prompt_template = st.session_state.prompt_template

        # ColumnStandardizationAgent instance from session state
        if 'human_data_standardization_agent' not in st.session_state:
            st.session_state.human_data_standardization_agent = HumanDataStandardizationAgent()

        inflection_method = st.selectbox(
            "Select the inflection method",
            ['underscore', 'camelize', 'dasherize', 'humanize', 
             'titleize', 'pluralize', 'singularize', 'parameterize'],
            index=0,
            key='inflection_method'
        )

        prompt_template = st.text_input(
            "Enter the AI prompt template (use '{term}' as a placeholder for feature names)",
            value="Provide a single or compound word standardized value of the {term}",
            key='infl_prompt_template'
        )

        if st.button("Standardize Feature Names"):
            result_df = st.session_state.human_data_standardization_agent.inflection_ai_standard_feature_name(
                data
            )
            
            if result_df is not None:
                st.dataframe(result_df)


    def _st_human_inflxn_ai_correction(self, df: pd.DataFrame) -> pd.DataFrame:

        #st.markdown("----")
        st.write("---")
        st.write("##### 🧑‍🔧 Batch Edit `AI Standardized Feature` by Index")

        st.markdown("Use the form below to manually update specific rows by index.")

        # Show table with indices for reference
        st.dataframe(df[['original_feature_name', 'ai_standardized_feature']], use_container_width=True)

        # Dynamic input for batch edits
        batch_count = st.number_input("🔢 How many edits do you want to make?", min_value=1, max_value=len(df), value=1)

        edits = []
        for i in range(batch_count):
            col1, col2 = st.columns([1, 3])
            with col1:
                idx = st.number_input(f"Index #{i+1}", min_value=0, max_value=len(df)-1, key=f"idx_{i}")
            with col2:
                new_val = st.text_input(f"New value for index {idx}", key=f"new_val_{i}")
            edits.append((idx, new_val))

        if st.button("✅ Apply Batch Updates"):
            for idx, new_val in edits:
                if new_val:
                    df.at[idx, 'ai_standardized_feature'] = new_val
            st.success("✔️ Batch updates applied successfully!")
            st.dataframe(df[['original_feature_name', 'ai_standardized_feature']], use_container_width=True)

        return df   


    def streamlit_human_ai_standardized_data(self, df):

        #st.header("Human-AI Interactive Standardization")
        st.write("---")
        st.write("###### Human-AI Standardization")
        
        # Initialize stateful df holders

        if "df" not in st.session_state:
            st.session_state.df = df.copy()
        if "cleaned_data" not in st.session_state:
            st.session_state.cleaned_data = df.copy()
        if "human_ai_data" not in st.session_state:
            st.session_state.human_ai_data = df.copy()
        if "human_corrections" not in st.session_state:
            st.session_state.human_corrections = {}
        if "accuracy_results" not in st.session_state:
            st.session_state.accuracy_results = {}

        self.df = st.session_state.df
        self.cleaned_data = st.session_state.cleaned_data
        self.human_ai_data = st.session_state.human_ai_data
        self.human_corrections = st.session_state.human_corrections
        self.accuracy_results = st.session_state.accuracy_results



        if "selected_column" not in st.session_state:
            st.session_state.selected_column = None
        
        # --- Step 1: Column Selection ---
        selected_column = st.selectbox("Select a column to standardize", self.df.columns)

        #st.session_state.df = self.df.copy()
        st.session_state.selected_column = selected_column
        

        # Show distinct values
        unique_vals = st.session_state.df[selected_column].dropna().unique()
        
        
        st.write(f"###### Unique values in *{selected_column}*:")

        # If there are more than 30 unique values, make it scrollable
        if len(unique_vals) > 30:
            # Organize into 5 columns
            column_chunks = [[] for _ in range(3)]
            for i, val in enumerate(unique_vals):
                column_chunks[i % 3].append(val)

            # Generate HTML for the columns
            html_columns = ""
            for col in column_chunks:
                html_columns += "<div style='flex: 1; padding-right: 10px;'>"
                for item in col:
                    html_columns += f"<div>{item}</div>"
                html_columns += "</div>"

            # HTML code for the scrollable container with default Streamlit styling
            st.markdown(
                f"""
                <div style="
                    display: flex;
                    flex-direction: row;
                    max-height: 250px;
                    overflow-y: auto;
                    background-color: inherit;
                    color: inherit;
                    font-family: inherit;
                    border: 1px solid rgba(49, 51, 63, 0.2);
                    border-radius: 6px;
                    padding: 10px;
                ">
                    {html_columns}
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            # Custom CSS for tighter spacing
            st.markdown("""
                <style>
                .compact-list {
                    margin-bottom: -0.5rem;
                    line-height: 1.70;
                }
                </style>
            """, unsafe_allow_html=True)

            # Display across 5 columns
            cols = st.columns(3)
            for i, val in enumerate(unique_vals):
                with cols[i % 3]:
                    st.markdown(f"<div class='compact-list'>{val}</div>", unsafe_allow_html=True)
                
        st.write("---")               
        
        reference_column = st.selectbox(
            "Select a reference column for prompts (optional)",
            [""] + self.df.columns.to_list(), 
            index=0
        )
        if not reference_column:
            reference_column = selected_column

        prompt_template = st.text_input("Prompt template (use `{term}` as placeholder)", "Standardize the term: {term}")


        st.write("---")

        # def clean_text(text):
        #     if not isinstance(text, str):
        #         return ""
        #     text = text.strip().lower()
        #     text = re.sub(r'[^\w\s-]', '', text)
        #     text = re.sub(r'\s+', ' ', text)
        #     return text

        def clean_text(text):
            if not isinstance(text, str):
                return ""
            text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)
            text = re.sub(r'[_\-]+', ' ', text)
            text = text.strip().lower()
            text = re.sub(r'[^\w\s]', '', text)
            text = re.sub(r'\s+', ' ', text)

            return text


        # Download word corpus once
        nltk.download('words')
        
        # Load the set of English words
        english_words = set(words.words())
        
        def is_valid_word(word: str) -> bool:
            return word.lower() in english_words

        def group_similar_terms(values, threshold=90, max_group_size=5):
            grouped = {}
            seen = set()

            for val in values:
                if val in seen:
                    continue
                matches = process.extract(val, values, scorer=fuzz.ratio, limit=max_group_size)
                similar_group = [match[0] for match in matches if match[1] >= threshold and match[0] not in seen]
                for match in similar_group:
                    seen.add(match)
                grouped[val] = similar_group

            return grouped
            
        def standardize_with_dict_filtering(values, threshold=90):
            """
            Standardize values using spell checking and frequency analysis.
            Corrects spelling errors like 'Vaccum' -> 'Vacuum'.
            """
            freq = Counter(values)
            groups = group_similar_terms(values, threshold=threshold)
            standardized_map = {}
            
            # Initialize spell checker once
            spell = SpellChecker()
            
            if "human_ai_data" not in st.session_state:
                st.session_state["human_ai_data"] = self.human_ai_data.copy()

            # When you update the DataFrame:
            st.session_state["human_ai_data"] = self.human_ai_data.copy()

            for rep, variants in groups.items():
                # First, apply spell correction to all variants
                corrected_variants = []
                for v in variants:
                    # Clean the value
                    cleaned = clean_text(v)
                    # Split into words and correct each word
                    words = cleaned.split()
                    corrected_words = []
                    for word in words:
                        # Check if it's a valid word
                        if is_valid_word(word):
                            corrected_words.append(word)
                        else:
                            # Use spell checker to correct
                            corrected = spell.correction(word)
                            if corrected:
                                corrected_words.append(corrected)
                            else:
                                corrected_words.append(word)
                    corrected_variants.append(' '.join(corrected_words))
                
                # Create mapping from original to corrected
                variant_to_corrected = dict(zip(variants, corrected_variants))
                
                # Find the most common corrected form
                corrected_freq = Counter(corrected_variants)
                most_common_corrected = corrected_freq.most_common(1)[0][0]
                
                # Map all variants to the most common corrected form
                for v in variants:
                    standardized_map[v] = most_common_corrected

            return standardized_map
            
            
        # --- Initialization ---
        if "human_ai_data" not in st.session_state:
            # Always initialize as a DataFrame!
            st.session_state.human_ai_data = pd.DataFrame()

        if "comparison_df" not in st.session_state:
            st.session_state.comparison_df = pd.DataFrame()

            
        # Initialize standardization state
        if 'standardization_completed' not in st.session_state:
            st.session_state.standardization_completed = False
        if 'standardization_column' not in st.session_state:
            st.session_state.standardization_column = None

        # --- Standardization Button ---
        col_btn1, col_btn2 = st.columns([1, 4])
        with col_btn1:
            run_std_btn = st.button("Run Standardization Prompt", key="run_std_prompt_btn")
        
        if run_std_btn:
            lowercase_series = st.session_state.df[selected_column].astype(str).fillna("").map(clean_text)
            stnd_unique_values = lowercase_series.dropna().unique()

            ai_standardized_map = standardize_with_dict_filtering(stnd_unique_values, threshold=90)
            standardized_column = lowercase_series.map(ai_standardized_map).fillna(lowercase_series)

            # Save to session state
            st.session_state.standardized_column = standardized_column.copy()
            st.session_state.human_ai_data = pd.DataFrame({selected_column: standardized_column.copy()})
            
            # Store the original human-corrected values as the fixed reference
            st.session_state.human_corrected_reference = standardized_column.copy()

            if 'standard_names_df' not in st.session_state:
                st.session_state.standard_names_df = pd.DataFrame({
                    'original_feature_name': st.session_state.df[selected_column],
                    'ai_standardized_feature': standardized_column.copy()
                })
            
            # st.session_state.standard_names_df = pd.DataFrame({
            #     'original_feature_name': st.session_state.df[selected_column],
            #     'ai_standardized_feature': standardized_column.copy()
            # })
            st.session_state.comparison_df = pd.DataFrame({
                'Original': st.session_state.df[selected_column],
                'AI_Standardized': standardized_column,
                'Human_Corrected': st.session_state.human_corrected_reference
            }).drop_duplicates()
            
            # Store the original values separately to ensure they never change
            st.session_state.original_comparison_values = {
                'Original': st.session_state.df[selected_column].copy(),
                'AI_Standardized': standardized_column.copy()
            }
            
            # IMPORTANT: Update cleaned_data with AI-standardized values
            # This ensures the AI values are properly stored and displayed
            st.session_state.cleaned_data[selected_column] = standardized_column.copy()
            
            st.session_state.standardization_completed = True
            st.session_state.standardization_column = selected_column

            st.success(f"Standardization completed for **{selected_column}**.")
            st.rerun()

        # --- Show editing section if standardization has been completed ---
        if st.session_state.get('standardization_completed', False) and st.session_state.get('standardization_column') == selected_column:
            
            st.write("##### Comparison of Original, AI Standardized & Human Corrected Data")
            st.dataframe(st.session_state.comparison_df)

            # Optional: plot accuracy
            self.plot_accuracy_streamlit(
                df=self.df,
                cleaned_data=self.cleaned_data,
                human_ai_data=st.session_state.human_ai_data,
                column_name=selected_column,
                accuracy_results=self.accuracy_results,
                is_human_corrected=True
            )

            # --- Display AI Standardized Values (NOT Editable) ---
            st.write("---")
            st.write("###### 🤖 AI Standardized Values (Read-Only)")
            st.markdown("These AI-generated values are fixed and cannot be edited.")

            with st.expander("Show AI Standardized Values"):              
                if 'standard_names_df' in st.session_state and not st.session_state.standard_names_df.empty:
                    # Show side-by-side comparison with original data
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("##### Original Data")
                        st.dataframe(st.session_state.df[[selected_column]])#.head(20))
                    
                    with col2:
                        st.write("##### AI Standardized Values (Fixed)")
                        st.dataframe(st.session_state.cleaned_data[[selected_column]])#.head(20))
            
            # --- Editable Human Corrected Values (Unique Values Only) ---
            st.write("---")
            st.write("###### ✏️ Edit Human Standardized Values (Unique Values)")
            st.markdown("Edit unique human-corrected values below. Changes will replace **all occurrences** of that value in the column.")
            
            with st.expander("Edit Human Corrected Values"):              
                # Create an editable table of unique values for Human corrections
                if 'standard_names_df' in st.session_state and not st.session_state.standard_names_df.empty:
                    # Get unique human_corrected_feature values with their original mappings
                    # Check if the required columns exist
                    if 'ai_standardized_feature' not in st.session_state.standard_names_df.columns:
                        st.error("Missing 'ai_standardized_feature' column. Please run standardization first.")
                        st.write("Available columns:", list(st.session_state.standard_names_df.columns))
                        st.stop()
                    
                    if 'original_feature_name' not in st.session_state.standard_names_df.columns:
                        st.error("Missing 'original_feature_name' column. Please run standardization first.")
                        st.write("Available columns:", list(st.session_state.standard_names_df.columns))
                        st.stop()
                    
                    # Get unique AI standardized values (these are the current human values initially)
                    unique_ai_values = st.session_state.standard_names_df[['original_feature_name', 'ai_standardized_feature']].drop_duplicates(subset=['ai_standardized_feature'])
                    
                    # Store edited human values in session state to persist across reruns
                    if 'edited_human_values' not in st.session_state:
                        # Initialize with AI values (since initially human = AI)
                        st.session_state.edited_human_values = unique_ai_values[['ai_standardized_feature']].drop_duplicates().reset_index(drop=True).copy()
                        st.session_state.edited_human_values.columns = ['human_corrected_feature']
                    
                    # Create editable dataframe for unique human values only
                    edited_human_df = st.data_editor(
                        st.session_state.edited_human_values,
                        use_container_width=True,
                        key="editable_unique_human_values",
                        num_rows="dynamic",
                        column_config={
                            "human_corrected_feature": st.column_config.TextColumn(
                                "Human Corrected Value",
                                help="Edit this value to replace ALL occurrences in the column"
                            )
                        }
                    )
                    
                    # Update session state with edited human values
                    st.session_state.edited_human_values = edited_human_df
                    
                    # Show how many unique values there are
                    st.info(f"✏️ Editing {len(edited_human_df)} unique human-corrected values. Edit multiple values, then click Save.")
                    
                    # Update the human_ai_data with edited human values
                    if st.button("💾 Save Human Corrections", key="save_edited_human_values"):
                        # Get the new human-corrected values
                        new_human_values = edited_human_df['human_corrected_feature'].values
                        old_human_values = unique_ai_values['ai_standardized_feature'].values
                        
                        # Create mapping dictionary (AI value -> Human value)
                        human_mapping = dict(zip(old_human_values, new_human_values))
                        
                        # Apply the mapping to create human-corrected column
                        human_corrected_column = st.session_state.cleaned_data[selected_column].map(human_mapping).fillna(st.session_state.cleaned_data[selected_column])
                        
                        # Update human_ai_data with the human-corrected values
                        st.session_state.human_ai_data = pd.DataFrame({selected_column: human_corrected_column.copy()})
                        
                        # Recreate the comparison DataFrame with updated values
                        # IMPORTANT: Keep Original and AI values unchanged, only update Human
                        # Store the original values separately to ensure they never change
                        if 'original_comparison_values' not in st.session_state:
                            st.session_state.original_comparison_values = {
                                'Original': st.session_state.df[selected_column].copy(),
                                'AI_Standardized': st.session_state.cleaned_data[selected_column].copy()
                            }
                        
                        st.session_state.comparison_df = pd.DataFrame({
                            'Original': st.session_state.original_comparison_values['Original'],
                            'AI_Standardized': st.session_state.original_comparison_values['AI_Standardized'],
                            'Human_Corrected': human_corrected_column
                        }).drop_duplicates()
                        
                        st.success("✅ Updates made, human corrections saved, accuracy chart updated.")
                        
                        # Show how many values were changed
                        changes_made = sum(1 for old, new in zip(old_human_values, new_human_values) if old != new)
                        if changes_made > 0:
                            st.info(f"🔄 {changes_made} unique human value(s) were modified from AI values.")

                        
                        # Show updated comparison
                        st.write("##### Updated Comparison (Original vs AI vs Human)")
                        st.dataframe(st.session_state.comparison_df)
                        
                        # Re-plot accuracy with updated values
                        st.write("##### Updated Accuracy")
                        self.plot_accuracy_streamlit(
                            df=self.df,
                            cleaned_data=self.cleaned_data,
                            human_ai_data=st.session_state.human_ai_data,
                            column_name=selected_column,
                            accuracy_results=self.accuracy_results,
                            is_human_corrected=True
                        )

                # --- Apply Standardized Values to DataFrame ---
                st.write("---")
                st.write("###### Apply Standardized Values")
                
            if st.button("✅ Apply Standardized Values to DataFrame", key="apply_standardized_values"):
                # Update the main session_state.df with HUMAN-CORRECTED values (not AI values)
                # This ensures that any human corrections are applied to the final DataFrame
                if 'human_ai_data' in st.session_state and not st.session_state.human_ai_data.empty and selected_column in st.session_state.human_ai_data.columns:
                    st.session_state.df[selected_column] = st.session_state.human_ai_data[selected_column].copy()
                    st.success(f"Column **'{selected_column}'** has been updated with Human-corrected values!")
                else:
                    # Fallback to AI values if no human corrections exist
                    st.session_state.df[selected_column] = st.session_state.standardized_column.copy()
                    st.success(f"Column **'{selected_column}'** has been updated with AI standardized values (no human corrections applied)!")
                
                # Also update self.df to keep it in sync
                self.df = st.session_state.df.copy()
                
                # Display the updated DataFrame
                st.write("##### Updated DataFrame")
                st.dataframe(st.session_state.df)
                    
        # Provide download button
        csv = st.session_state.df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Updated DataFrame",
            data=csv,
            file_name=f"standardized_{selected_column}.csv",
            mime="text/csv"
        )

        # Reset button
        if st.button("🔄 Reset - Run New Standardization", key="reset_standardization"):
            st.session_state.standardization_completed = False
            st.session_state.standardization_column = None
            if 'edited_unique_values' in st.session_state:
                del st.session_state.edited_unique_values
            st.rerun()


        st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space

        # # --- Step 3: Final Output and Download ---
        # if st.button("Finalize and Export"):
        #     if "human_ai_data" in st.session_state and not st.session_state.human_ai_data.empty:
        #         # Update the main DataFrame with human-corrected values
        #         for col in st.session_state.human_ai_data.columns:
        #             if col in st.session_state.df.columns:
        #                 st.session_state.df[col] = st.session_state.human_ai_data[col].copy()
                
        #         st.success("✅ Main DataFrame updated with human-corrected values!")
                
        #         final_df = st.session_state.human_ai_data.copy()
        #         st.success("Final human-AI standardized dataset is ready.")

        #         st.write("### Preview:")
        #         st.dataframe(final_df.head())

        #         st.write("### Final Output:")
        #         st.dataframe(st.session_state.human_ai_data)
                
        #         st.write("### Updated Main DataFrame:")
        #         st.dataframe(st.session_state.df.head())

        #         # Download
        #         csv = final_df.to_csv(index=False).encode("utf-8")
        #         st.download_button("⬇️ Download as CSV", csv, "human_ai_data.csv", "text/csv")
        #     else:
        #         st.warning("No finalized data found. Please run standardization and corrections first.")

            
            
        #     self.plot_accuracy_streamlit(
        #         df=self.df,
        #         cleaned_data=self.cleaned_data,
        #         human_ai_data=st.session_state.human_ai_data,
        #         column_name=selected_column,
        #         accuracy_results=self.accuracy_results,
        #         is_human_corrected=True
        #     )


    st.divider()



        # with st.expander("👨‍🦱 Manual Value Standardization", expanded=False):

        #     # --- Step 2: Manual Adjustment ---
        #     st.write("###### Manual Value Standardization")

        #     selected_column = st.selectbox("Select a column to standardize", self.df.columns, key="manual_correct_column_select")

        #     # Initialize manual standardization state
        #     if 'manual_standard_names_df' not in st.session_state:
        #         # Create initial DataFrame with unique values only (for easier editing)
        #         # Convert all values to strings for editing compatibility
        #         unique_values = st.session_state.df[selected_column].astype(str).drop_duplicates().reset_index(drop=True)
        #         st.session_state.manual_standard_names_df = pd.DataFrame({
        #             'original_feature_name': unique_values,
        #             'human_corrected_feature': unique_values.copy()  # Start identical to original
        #         })
                
        #         # Initialize manual accuracy tracking
        #         st.session_state.manual_accuracy_results = {}

        #     # Update the manual DataFrame if column changes
        #     if selected_column != st.session_state.get('manual_selected_column', None):
        #         unique_values = st.session_state.df[selected_column].astype(str).drop_duplicates().reset_index(drop=True)
        #         st.session_state.manual_standard_names_df = pd.DataFrame({
        #             'original_feature_name': unique_values,
        #             'human_corrected_feature': unique_values.copy()
        #         })
        #         st.session_state.manual_selected_column = selected_column

        #     # Show current manual standardization state
        #     if 'manual_standard_names_df' in st.session_state and not st.session_state.manual_standard_names_df.empty:
                
        #         st.write("##### Manual Standardization - Edit Unique Values")
        #         st.write("Edit the 'Human Corrected Feature' column below to standardize values. Changes affect **all occurrences** of that original value.")
                
        #         # Show count of unique values
        #         st.info(f"✏️ Editing {len(st.session_state.manual_standard_names_df)} unique value(s). Edit multiple values, then click Apply.")

        #         # Create editable table for manual corrections (unique values only)
        #         editable_manual_df = st.data_editor(
        #             st.session_state.manual_standard_names_df,
        #             use_container_width=True,
        #             key="editable_manual_values",
        #             num_rows="dynamic",
        #             column_config={
        #                 "original_feature_name": st.column_config.TextColumn(
        #                     "Original Feature Name",
        #                     help="Original feature name (read-only)",
        #                     disabled=True
        #                 ),
        #                 "human_corrected_feature": st.column_config.TextColumn(
        #                     "Human Corrected Value",
        #                     help="Edit this value to replace ALL occurrences in the column"
        #                 )
        #             }
        #         )

        #         # Update session state with edited values
        #         st.session_state.manual_standard_names_df = editable_manual_df

        #         # Calculate accuracy for manual corrections (Original vs Human only - no AI)
        #         def calculate_manual_accuracy(df, column_name):
        #             total_values = len(df)
        #             matching_values = (df['original_feature_name'] == df['human_corrected_feature']).sum()
        #             accuracy = (matching_values / total_values) * 100 if total_values > 0 else 100
        #             return accuracy

        #         manual_accuracy = calculate_manual_accuracy(st.session_state.manual_standard_names_df, selected_column)
                
        #         # Store accuracy result
        #         st.session_state.manual_accuracy_results[selected_column] = {
        #             'Original_Accuracy': manual_accuracy,
        #             'Human_Accuracy': 100.0  # Human is always 100% (reference)
        #         }

        #         # Display accuracy
        #         col1, col2 = st.columns([1, 2])
        #         with col1:
        #             st.write("##### Manual Standardization Accuracy")
        #             accuracy_table = pd.DataFrame({
        #                 "Accuracy": [
        #                     f"{manual_accuracy:.2f}%",
        #                     "100.00%"
        #                 ]
        #             }, index=["Original", "Human"])
        #             st.table(accuracy_table)

        #         with col2:
        #             # Plot accuracy (Original vs Human only)
        #             fig, ax = plt.subplots(figsize=(5, 3))
        #             ax.bar(
        #                 ['Original', 'Human'],
        #                 [manual_accuracy, 100.0],
        #                 color=['#FF9999', '#99FF99']
        #             )
        #             ax.set_ylim(0, 100)
        #             ax.set_ylabel('Accuracy (%)', fontsize=8)
        #             ax.set_title(f'Manual Standardization Accuracy for {selected_column}', fontsize=9)
        #             ax.tick_params(axis='both', labelsize=7)
        #             fig.tight_layout()
        #             st.pyplot(fig, bbox_inches="tight")

        #         # Show unique values summary
        #         st.write("##### Unique Values Summary")
        #         unique_summary = st.session_state.manual_standard_names_df[['original_feature_name', 'human_corrected_feature']].drop_duplicates()
        #         st.dataframe(unique_summary)

        #         # Apply Manual Correction button
        #         if st.button("✅ Apply Manual Corrections to DataFrame", key="apply_manual_correction"):
        #             # Apply the human corrections to the main DataFrame
        #             # Create a mapping from original to corrected values
        #             correction_map = dict(zip(
        #                 st.session_state.manual_standard_names_df['original_feature_name'],
        #                 st.session_state.manual_standard_names_df['human_corrected_feature']
        #             ))
                    
        #             # Apply mapping to the selected column in the main DataFrame
        #             st.session_state.df[selected_column] = st.session_state.df[selected_column].map(correction_map).fillna(st.session_state.df[selected_column])
                    
        #             # Update human_ai_data for consistency
        #             st.session_state.human_ai_data[selected_column] = st.session_state.manual_standard_names_df['human_corrected_feature'].copy()
                    
        #             st.success(f"✅ Manual corrections applied to column '{selected_column}'!")
                    
        #             # Show updated DataFrame
        #             st.write("##### Updated DataFrame")
        #             st.dataframe(st.session_state.df)
                    
        #             # Download button
        #             csv = st.session_state.df.to_csv(index=False).encode('utf-8')
        #             st.download_button(
        #                 label="📥 Download DataFrame with Manual Corrections",
        #                 data=csv,
        #                 file_name=f"manual_corrected_{selected_column}.csv",
        #                 mime="text/csv"
        #             )

            

        #     # --- Step 3: Final Output and Download ---
        #     if st.button("Finalize and Export"):
        #         if "human_ai_data" in st.session_state and not st.session_state.human_ai_data.empty:
        #             # Update the main DataFrame with human-corrected values
        #             for col in st.session_state.human_ai_data.columns:
        #                 if col in st.session_state.df.columns:
        #                     st.session_state.df[col] = st.session_state.human_ai_data[col].copy()
                    
        #             st.success("✅ Main DataFrame updated with human-corrected values!")
                    
        #             final_df = st.session_state.human_ai_data.copy()
        #             st.success("Final human-AI standardized dataset is ready.")

        #             st.write("### Preview:")
        #             st.dataframe(final_df.head())

        #             st.write("### Final Output:")
        #             st.dataframe(st.session_state.human_ai_data)
                    
        #             st.write("### Updated Main DataFrame:")
        #             st.dataframe(st.session_state.df.head())

        #             # Download
        #             csv = final_df.to_csv(index=False).encode("utf-8")
        #             st.download_button("⬇️ Download as CSV", csv, "human_ai_data.csv", "text/csv")
        #         else:
        #             st.warning("No finalized data found. Please run standardization and corrections first.")

                
                
        #         self.plot_accuracy_streamlit(
        #             df=self.df,
        #             cleaned_data=self.cleaned_data,
        #             human_ai_data=st.session_state.human_ai_data,
        #             column_name=selected_column,
        #             accuracy_results=self.accuracy_results,
        #             is_human_corrected=True
        #         )


        # st.divider()



    def plot_accuracy_streamlit(self, df, cleaned_data, human_ai_data, column_name, accuracy_results, is_human_corrected=False):
        """
        Streamlit-compatible function to visualize accuracy of original, AI-standardized, and human-corrected data.
        
        Accuracy is calculated as how close each version is to the human-corrected (reference) data.
        - Original Accuracy: How close original data is to human-corrected data (CONSTANT - never changes)
        - AI Accuracy: How close AI-standardized data is to human-corrected data (changes when AI values are edited)
        - Human Accuracy: Always 100% (reference)
        
        The human_ai_data is the reference. When is_human_corrected=True:
        - human_ai_data contains the current human-corrected values (the reference)
        - cleaned_data contains the original AI standardized values (before any editing)
        - df contains the original raw data (never changes)
        
        Note: Original accuracy is CONSTANT because it compares original data with human-corrected data.
        When only AI values are edited, the Original accuracy should NOT change because neither
        the original data nor the human-corrected data has changed.
        """

        total_values = len(df[column_name])
        
        # Human accuracy is always 100% (reference)
        human_corrected_accuracy = 100
        
        if is_human_corrected:
            # Original accuracy: How close original data is to human-corrected values
            # This is CONSTANT - it compares original data with human-corrected data
            # Since neither original nor human values change when AI values are edited, this stays the same
            orig_accuracy = (df[column_name] == human_ai_data[column_name]).sum() / total_values * 100
            
            # AI accuracy: How close the original AI standardized values are to the human-corrected values
            # This CHANGES when the AI values are edited to be closer/further from human values
            # cleaned_data contains the original AI values, human_ai_data contains the human-corrected values
            ai_corrected_accuracy = (cleaned_data[column_name] == human_ai_data[column_name]).sum() / total_values * 100
        else:
            # Default calculation for non-human-corrected mode
            orig_accuracy = (df[column_name] == human_ai_data[column_name]).sum() / total_values * 100
            ai_corrected_accuracy = (cleaned_data[column_name] == human_ai_data[column_name]).sum() / total_values * 100

        # Store in dictionary for later use
        accuracy_results[column_name] = {
            'Original_Accuracy': orig_accuracy,
            'AI_Accuracy': ai_corrected_accuracy,
            'Human_Accuracy': human_corrected_accuracy
        }
        
        
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            fig, ax = plt.subplots(figsize=(5, 3))  # Small figure

            accuracy_values = [orig_accuracy, ai_corrected_accuracy, human_corrected_accuracy]

            bars = ax.bar(
                ['Orig', 'AI', 'Human'],
                accuracy_values,
                #[orig_accuracy, ai_corrected_accuracy, human_corrected_accuracy],
                color=['#FF9999', '#99CCFF', '#99FF99']
            )
       
            ax.set_ylim(0, 100)
            ax.set_ylabel('Accuracy (%)', fontsize=8)
            ax.set_title(f'Accuracy for {column_name}', fontsize=9)
            ax.tick_params(axis='both', labelsize=7)

            # ✅ Add value labels
            #ax.bar_label(bars, fmt='%.1f%%', fontsize=7)
            ax.bar_label(bars, fmt='%.2f%%', label_type='center', fontsize=7)

            fig.tight_layout()
            st.pyplot(fig, bbox_inches="tight")

       
        
class PyDEAMLPipeline:
    def __init__(self):
        self.log = []
        self.data = None
        # Initialize the network object for Pyvis
        self.net = Network(height="20px", width="125%", directed=True)
        self.net.set_options('''
        {
          "nodes": {
            "shape": "box",
            "font": {
              "size": 14
            }
          },
          "edges": {
            "arrows": {
              "to": {
                "enabled": true
              }
            },
            "smooth": false
          },
          "interaction": {
            "dragNodes": true,
            "dragView": true,
            "zoomView": true
          },
            "physics": {
                "enabled": true,
                "solver": "barnesHut",
            "barnesHut": {
                "gravitationalConstant": -10000,
                "centralGravity": 0,
                "springLength": 250,
                "springConstant": 0.001,
                "damping": 0.09,
                "avoidOverlap": 0
            },
            "stabilization": {
                "enabled": true,
                "iterations": 1000,
                "updateInterval": 25,
                "onlyDynamicEdges": false,
                "fit": true
            },
            "maxVelocity": 50,
            "minVelocity": 0.1,
            "timestep": 0.5
        }
        }
        ''')



    def _add_node(self, step, statuses, is_substep=False):
        status = statuses.get(step, 'idle')
        
        if status == 'active':
            color = 'lightblue'
            border_color = '#1e88e5' if is_substep else None
        elif status == 'completed':
            color = 'lightgreen'
            border_color = '#00c853' if is_substep else None  # Solid green for completed substeps
        elif status == 'failed':
            color = 'lightcoral'
            border_color = '#d32f2f' if is_substep else None
        else:
            color = 'lightgray'
            border_color = None


        # Adjust size and shape for substeps
        if is_substep:
            # Add node with border for completed substeps
            if border_color:
                self.net.add_node(
                    step, 
                    label=step, 
                    color=color, 
                    shape='ellipse',  # Different shape for substeps
                    title=step,
                    borderWidth=3,
                    color_border=border_color
                )
            else:
                self.net.add_node(
                    step, 
                    label=step, 
                    color=color, 
                    shape='ellipse',  # Different shape for substeps
                    title=step,
                    borderWidth=1
                )
        else:
            self.net.add_node(step, label=step, color=color, shape='box', title=step)




    def display_horizontal_pipeline(self, steps, statuses):
        for step in steps:
            self._add_node(step, statuses, is_substep=False)

        for i in range(len(steps) - 1):
            self.net.add_edge(steps[i], steps[i + 1])


    def add_substeps(self, parent_step, substeps, statuses):
        """Add substeps to a main step"""
        for substep in substeps:
            # Add substep node
            self._add_node(substep, statuses, is_substep=True)
            # Connect parent to first substep
            self.net.add_edge(parent_step, substep)
        
        # Connect substeps sequentially
        for i in range(len(substeps) - 1):
            self.net.add_edge(substeps[i], substeps[i + 1])

    def generate_html(self):
        html = self.net.generate_html()
        return html
  
        
    def clean_data(self, data):
        st.write("Loaded raw/preprocessed data into the pipeline.")
        #data = data.drop_duplicates()
        return data

    def perform_feature_engineering(self, data):
        st.write("Feature Engineering")
        #numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        numeric_columns = data.apply(pd.to_numeric, errors='coerce').select_dtypes(include=['number']).columns
        return data


    def split_data(self, data):
        st.write("Splitting Data: Preparing train/test split.")
        #numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        numeric_columns = data.apply(pd.to_numeric, errors='coerce').select_dtypes(include=['number']).columns
        if len(numeric_columns) < 2:
            st.error("Not enough numeric columns to perform split.")
            return None, None, None, None

        target_column = 'Salary' if 'Salary' in data.columns else numeric_columns[0]
        X = data[numeric_columns].drop(columns=[target_column], errors='ignore')
        y = data[target_column]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        return X_train, X_test, y_train, y_test

    def train_model(self, X_train, y_train):
        st.write("Training Model: Performing linear regression.")
        model = LinearRegression()
        model.fit(X_train, y_train)
        return model

    def evaluate_model(self, model, X_test, y_test):
        st.write("Evaluating Model: Calculating mean squared error.")
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        return {'MSE': mse}


#######################################



# class OutputCapture(list):
#     def __init__(self):
#         self._stdout = sys.stdout  # Keep the original stdout
#         sys.stdout = self  # Redirect stdout to this object

#     def write(self, data):
#         data = st.session_state.df
#         self.append(data)  # Append printed data into the list


#     def flush(self):
#         pass  # Required for compatibility, no-op here

#     def get_output(self):
#         return ''.join(self)  # Join list items into a single string

class OutputCapture(list):
    def __init__(self):
        self._stdout = sys.stdout  # Keep the original stdout
        sys.stdout = self  # Redirect stdout to this object

    def write(self, data):
        # Only append string data, ignore empty strings
        if isinstance(data, str) and data.strip():
            self.append(data)
        elif data:  # For non-string data, convert to string
            self.append(str(data))

    def flush(self):
        pass  # Required for compatibility, no-op here

    def get_output(self):
        # Join all string items
        return ''.join(self)
    
# Class to handle data operations
################################# PyDEAML Agents #################################
class PyLade:
    def __init__(self):
        self.log = []
        self.df = None


    # Function to create a pandas DataFrame agent
    def generate_create_csv(self,uploaded_file):
        # Ensure that 'uploaded_file' is the file-like object
        if hasattr(uploaded_file, 'getvalue'):
            file_content = uploaded_file.getvalue()
            with open('wip.csv', 'wb') as f:
                f.write(file_content)
        else:
            st.error("The file is not in the expected format.")
            
        create_csv = create_csv_agent(OpenAI(temperature=0), "wip.csv", verbose=True, allow_dangerous_code=True)
        return create_csv          
 

    def load_data(self, uploaded_file):
        # Check if a file is uploaded
        if uploaded_file is None:
            st.error("No file uploaded. Please upload a file to continue.")
            return None

        try:
            # Process and load data
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_file_path = temp_file.name
            
            # Data agent to handle the file reading
            data_agent = DataReadAgent()
            read_input_file = data_agent.read_dir_file(temp_file_path)
            
            if isinstance(read_input_file, DataFrameOutput):
                if 'df' in st.session_state:
                    df = read_input_file.data
            
            os.remove(temp_file_path)  # Clean up temporary file
            #st.dataframe(df.head())

            st.session_state.df = df


            return df

        except Exception as e:
            st.error(f"An error occurred while processing the file: {str(e)}")
            return None


    def df_csv_io(self,df):
        csv_in_memory = io.BytesIO()  # Use BytesIO for binary data (use StringIO for text-based CSVs)
        df.to_csv(csv_in_memory, index=False)  # df to the buffer
        csv_in_memory.getvalue()
        return csv_in_memory   

    def fix_duplicate_data(self, df):
        st.write("Fix Duplicate Data")
        self.log.append("Applied 'Fix Duplicate Data'")
        dedup_data = df.drop_duplicates()
        st.write(dedup_data.head(20))
        return dedup_data 

    def accuracy_dedup_data(self, df):
        st.write("Accuracy Deduplicated Data")
        nrows_original = len(df)
        nrows_dedup_data = len(self.fix_duplicate_data(df))
        nrows_difference = nrows_original - nrows_dedup_data

        if nrows_difference == 0:
            accuracy_dedup_data = 1
        else:
            accuracy_dedup_data = nrows_difference / nrows_original

        st.write(accuracy_dedup_data.head(20))
        return accuracy_dedup_data

    def accuracy_missing_data(self, df):
        st.write("Missing Data Accuracy:")
        value_count = df.count().sum()
        element_count = df.size
        ratio_missing_data = (element_count - value_count) / element_count
        accuracy_missing_data = 1 - ratio_missing_data
        st.write(accuracy_missing_data)
        return accuracy_missing_data


    def predict_missing_data(self, df, knn):
        # Copy dataframe to avoid modifying the original
        predict_missing_data = df.copy()

        # Separate numerical and categorical columns
        num_cols = predict_missing_data.select_dtypes(include=[np.number]).columns
        cat_cols = predict_missing_data.select_dtypes(include=[object]).columns

        # Store original categorical columns
        cat_maps = {}
        for col in cat_cols:
            predict_missing_data[col] = predict_missing_data[col].astype('category')
            cat_maps[col] = predict_missing_data[col].cat.categories
            predict_missing_data[col] = predict_missing_data[col].cat.codes.replace(-1, np.nan)
        
        # Initialize KNNImputer
        imputer = KNNImputer(n_neighbors=knn)
        
        # Impute missing values
        predict_missing_data_imputed = pd.DataFrame(imputer.fit_transform(predict_missing_data), columns=predict_missing_data.columns)

        # Convert back to original types
        for col in cat_cols:
            predict_missing_data_imputed[col] = predict_missing_data_imputed[col].round(0).astype(int)
            predict_missing_data_imputed[col] = pd.Categorical.from_codes(predict_missing_data_imputed[col], cat_maps[col])

        st.write(predict_missing_data_imputed.head(20))
        return predict_missing_data_imputed
    


    def replace_missing_data(self, df, process='replace'):
        st.write("Replace Missing Data:")
        self.log.append(f"Applied 'Replace Missing Data' using method '{process}'")

        if 'fixed_missing_data' not in st.session_state:
            st.session_state.fixed_missing_data = df.copy()

        fixed_missing_data = df.copy()
        for column in st.session_state.fixed_missing_data.columns:
            if fixed_missing_data[column].isnull().all():
                st.session_state.fixed_missing_data.drop(column, axis=1, inplace=True)
            else:
                if process == 'replace' and pd.api.types.is_numeric_dtype(st.session_state.fixed_missing_data[column]):
                    st.session_state.fixed_missing_data[column].fillna(9999, inplace=True)
                elif process == 'replace' and pd.api.types.is_object_dtype(st.session_state.fixed_missing_data[column]):
                    st.session_state.fixed_missing_data[column].fillna('NA', inplace=True)
                elif process in ['mean', 'median', 'mode'] and pd.api.types.is_numeric_dtype(st.session_state.fixed_missing_data[column]):
                    st.session_state.fixed_missing_data[column].fillna(
                        st.session_state.fixed_missing_data[column].mean() if process == 'mean' else (
                            st.session_state.fixed_missing_data[column].median() if process == 'median' else st.session_state.fixed_missing_data[column].mode().iloc[0]), inplace=True)
                elif process in ['ffill', 'bfill']:
                    st.session_state.fixed_missing_data[column].fillna(method=process, inplace=True)
                elif process == 'interpolate' and pd.api.types.is_numeric_dtype(st.session_state.fixed_missing_data[column]):
                    st.session_state.fixed_missing_data[column].interpolate(inplace=True)
                #elif process == 'replace with predicted values':
                #    fixed_missing_data = self.predict_missing_data(fixed_missing_data, 1)

        st.write(st.session_state.fixed_missing_data.head(20))
        return st.session_state.fixed_missing_data

    def fix_unstandardized_data(self, df):
        st.write("Fix Unstandardized Data:")
        self.log.append("Applied 'Fix Unstandardized Data'")

        def space_case(term):
            if isinstance(term, str):
                term = term.strip().lower().replace(" ", "_").replace("/", "_")
                return term
            else:
                return term

        standardized_data = df.copy()
        for column in standardized_data.columns:
            standardized_data[column] = standardized_data[column].apply(space_case)

        st.write(standardized_data)
        return standardized_data

    def accuracy_standardized_data(self, df):
        st.write("Accuracy Unstandardized Data")
        standardized_data = self.fix_unstandardized_data(df)
        orig_standard_data = (df != standardized_data).astype(int)
        total_number_of_values_X = df.size

        ratio_δi_X = orig_standard_data.sum().sum() / total_number_of_values_X
        accuracy_df = 1 - ratio_δi_X

        st.write(accuracy_df.head(20))
        return accuracy_df

    def total_accuracy(self, df):
        st.write("Total Accuracy")
        sumAccuracy = (
            self.accuracy_missing_data(df) +
            self.accuracy_standardized_data(df) +
            self.accuracy_dedup_data(df)
        )
        num_transformation = 3  # increase this number if you add more transformations
        aveAccuracy = sumAccuracy / num_transformation
        st.write(aveAccuracy)
        return aveAccuracy

    def perform_data_standardization(self, df):
        """Perform data standardization using AI"""
        st.write("Performing Data Standardization with AI")
        
        # Initialize the AI Standardizer
        data_standardizer = DataStandardizerAI(use_ai=True)
        
        # Get all columns for standardization
        all_columns = df.columns.tolist()
        
        # Standardize all columns
        standardized_df = data_standardizer.auto_standardize_dataframe(df, all_columns)
        
        st.success("✅ Data standardization complete!")

        return standardized_df


#################################################################################

#################################### main function  #############################

#################################################################################


def main():
    load_dotenv()

    # Instantiate classes after set_page_config
    pydeamlpipeline = PyDEAMLPipeline()
    crewailpydeamlpipeline = PipelineCrewAIPyDEAML()

    # For LangChain OpenAI
    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_API_KEY") == "":
        st.error("OPENAI_API_KEY is not set")
        return

    st.markdown(
        """
        <style>
            /* Full-width containers */
            .block-container {
                padding-left: 5rem;
                padding-right: 5rem;
            }
            /* Optional: Adjust the width of sidebar */
            .css-1d391kg { 
                width: 25%; /* Adjust the sidebar width */
            }
        </style>
        """, unsafe_allow_html=True
    )


    def auto_flatten_json(df):
        """Identifies and explodes all columns containing list-like data."""
        # Find columns where any cell contains a list
        list_cols = [
            col for col in df.columns 
            if df[col].apply(lambda x: isinstance(x, list)).any()
        ]
        
        if list_cols:
            # Explode all detected list columns at once
            df = df.explode(list_cols).reset_index(drop=True)
            st.info(f"Automatically exploded columns: {', '.join(list_cols)}")
        
        # Attempt to convert any numeric-looking columns to proper types
        return df.apply(pd.to_numeric, errors='ignore')


    #st.set_page_config(page_title="Data Quality with Expressions")
    #st.header("EDA and Data Pipeline")
    st.markdown("## **EDA and Data Pipeline**")
    
    if 'uploaded_file' not in st.session_state:
        st.session_state.uploaded_file = None


    uploaded_file = st.sidebar.file_uploader("Upload a file", type=["csv", "xlsx", "json"], key="file_uploader")

    
    crew_ai = PyLade()
    pydeaml = CrewAIPyDEAML() # to replicate the implementation in Jupyter Notebook
    fea_eng_agt = FeaturesEngineeringAgent()

    if uploaded_file is not None and uploaded_file != st.session_state.uploaded_file:
        # New file uploaded, reset DataFrame
        df = crew_ai.load_data(uploaded_file)
        st.session_state.df = df
        st.session_state.original_df = df.copy(deep=True)
        st.session_state.uploaded_file = uploaded_file
        st.session_state.df_reset = True
        st.session_state.data_eda = "EDA Notes"

        st.success(f"Data loaded from the file :blue[**{uploaded_file.name}**] and DataFrame generated successfully!")

        st.dataframe(df.head())

        st.info("Check the :green[**Enable EDA Options**] on the sidebar to proceed with data exploration.")   

    
        # 🔥 ADD THIS LINE - Save to persistent pipeline
        # 🔥 CLEAR existing pipeline and save NEW data
        pipeline.clear_all()  # Remove all previous pipeline data
        pipeline.save_data(df, 'raw_data', agent_used='data_read_agent')

        st.success(f"Data loaded from the file :blue[**{uploaded_file.name}**] and DataFrame generated successfully!")

        st.dataframe(df.head())

        st.info("Check the :green[**Enable EDA Options**] on the sidebar to proceed with data exploration.")
        
        st.rerun()  # Force refresh to show new data

    # Main area tabs
    tab1, tab2, tab3 = st.tabs(["Current Data", "Add Pipeline Step", "Pipeline History"])

    with tab1:
        #st.header("Current Data State")
        st.markdown("### **Current Data State**")

        if 'df' not in st.session_state:
            st.session_state.df = None
        
        if pipeline.load_data() is not None and st.session_state.df is not None:  # Check if pipeline.load_data() is not None and st.session_state.df = None

            current_step = pipeline.metadata.get('current_data')
            if current_step and current_step != 'raw_data':
                st.info(f"📌 Current data from: **{current_step}**")


            if uploaded_file is not None:
                #st.dataframe(pipeline.load_data().head(), use_container_width=True)
                st.dataframe(pipeline.load_data().head(), use_container_width=True)
            else:
                st.error("No file uploaded. Please upload a file to continue.")

            
            with st.expander("📋 Column Information"):
                col_info = pd.DataFrame({
                    'Column Name': pipeline.load_data().columns,
                    'Data Type': pipeline.load_data().dtypes.values,
                    'Null Count': pipeline.load_data().isnull().sum().values,
                    'Unique Values': pipeline.load_data().nunique().values
                })
                st.dataframe(col_info)
        else:
            st.info("📁 No data loaded. Please upload a CSV or Excel file to begin.")

    with tab2:
        #st.header("Add Pipeline Step")
        st.markdown("### **Add Pipeline Step**")
        
        # 🔥 ONLY load from pipeline

        st.write("This reference.")
        if pipeline.load_data() is not None:
           #st.dataframe(pipeline.load_data().head(), use_container_width=True)
           st.dataframe(pipeline.load_data().head(), use_container_width=True)
            
           st.success(f"Working with data: {pipeline.load_data().shape[0]} rows, {pipeline.load_data().shape[1]} columns")

        else:
            st.info("⚠️ No data found. Please upload a file in the **Current Data** tab first.")           

    if 'original_df' not in st.session_state:
        st.session_state.original_df = None  # Backup of original data

    if 'missing_data_agent' not in st.session_state:
        st.session_state.missing_data_agent = None

    if 'fix_history' not in st.session_state:
        st.session_state.fix_history = []


    # Initialize df variable to avoid UnboundLocalError
    df = None

    # Only load data if it's not already in session state AND a file is uploaded
    if ('df' not in st.session_state or st.session_state.df is None) and uploaded_file is not None:
        df = crew_ai.load_data(uploaded_file)

    # Update session state with loaded data
    if df is not None and 'reset_to_this_df' not in st.session_state:
        st.session_state.reset_to_this_df = df.copy(deep=True)
        if 'df' not in st.session_state or st.session_state.df is None:
            st.session_state.df = df
    else:
        if 'reset_to_this_df' not in st.session_state:
            st.session_state.reset_to_this_df = None


     # Initialise the session_state reset
    if 'df_reset' not in st.session_state:
        st.session_state.df_reset = False


    if ('df' not in st.session_state or st.session_state.df is None) and uploaded_file is not None: 
        df = crew_ai.load_data(uploaded_file) # Update session state with loaded data 
        



    with st.sidebar:
        # This is actually the simplest way - columns work fine in sidebar
        left, right = st.columns(2)
        
        with left:
            if st.button("Reset DataFrame", key="sidebar_left"):

                st.session_state.df = st.session_state.reset_to_this_df
                st.session_state.df_reset = False
                st.session_state.data_eda = "EDA Notes"
                #st.write("If already uploaded, initial DataFrame is restored")
                st.success("✅ If already uploaded, initial DataFrame is restored!")
        
        with right:
            if st.button("Clear Caches", key="sidebar_right"):
                # Clear all cached functions
                st.cache_data.clear()
                st.cache_resource.clear()
                # Show confirmation
                st.success("✅ Caches Cleared!")

        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

    # Setting for the EDA sidebar
    if 'data_eda' not in st.session_state:
        st.session_state.data_eda = "EDA Notes"
    if 'reset' not in st.session_state:
        st.session_state.reset = False


    if 'data_eda_pipeline' not in st.session_state:
        st.session_state.data_eda_pipeline = "Pipeline Notes"



    # Function to reset selection
    def reset_eda_selection():
        st.session_state.data_eda = "EDA Notes"
        st.session_state.reset_eda_radio = "No"

    
    enable_eda = st.sidebar.checkbox("Enable EDA Options", key="eda_checkbox", help = "Enable this checkbox to activate EDA", on_change=enable_eda_chkbx)



    # Only show the radio button if the checkbox is checked
    if enable_eda:
        eda_options = ["EDA Notes", "Statistics Report", "Explore with LLM", "Preprocess with LLM", "Preprocess Data"]
        default_index = (
            0 if st.session_state.get('data_eda', "") == "" else ["EDA Notes", "Statistics Report", "Explore with LLM", "Preprocess with LLM", "Preprocess Data"].index(st.session_state.data_eda)
        )
        data_eda = st.sidebar.radio(
            "EDA - explore the data",
            options=eda_options,
            format_func=lambda x: "EDA Notes" if x == "" else x,
            index=default_index,
            key='de_data_eda'
        )
        st.session_state.data_eda = data_eda
    
        # Reset the df_reset flag after the radio button is rendered
        if st.session_state.df_reset:
            st.session_state.df_reset = False
        
        
        space_indent = "&nbsp;" * 50
        #st.markdown(f"{indent}Your indented text here", unsafe_allow_html=True)


        if data_eda == "EDA Notes":
            oak = os.getenv("OPENAI_API_KEY")
            #st.write(oak)

            eda_1 = st.markdown(

            """
            ---
            **Notes for Enable EDA Options**  
            EDA - explore the data

            - **Preprocess Data:** *To provide some information and basic processing.*
            - **Statistics Report:** *Provide several statistics about the DataFrame.*
            - **Explore with LLM:** *To explore the DataFrame with LLM.*
            - **Preprocess with LLM:** *To preprocess the DataFrame in collaboration with LLM suggestions.*
            - **Preprocess Data:** *To use predefined solutions to process the DataFrame*
            """
            , unsafe_allow_html=True

            )




        st.markdown("<hr>", unsafe_allow_html=True)

        if data_eda == "Statistics Report":
            st.sidebar.write("Statistics Report is selected")

            if st.session_state.df is not None:
                st.dataframe(st.session_state.df.head())  

            else:
                ""

            st.markdown("<hr>", unsafe_allow_html=True)
            #st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space
            
            stats_agent = FeaturesStatisticsAgent()

            # Usage example assuming your DataFrame is in st.session_state.df
            if 'df' in st.session_state and st.session_state.df is not None:
                overall_dataframe_summary, group_column = stats_agent.single_dataframe_summary(st.session_state.df)

            else:
                st.info("Please upload or load a DataFrame first.")



            # Usage example assuming your DataFrame is in st.session_state.df
            if 'df' in st.session_state and st.session_state.df is not None:
                stats_agent.display_summary_in_columns(st.session_state.df)
            else:
                st.info("Please upload or load a DataFrame first.")



        if data_eda == "Preprocess Data":
            st.sidebar.write("Preprocess Data is selected")
            
            # Check if the DataFrame exists in session state
            if st.session_state.df is not None:
                df = st.session_state.df  # Get the current DataFrame
                
                df = fea_eng_agt.preprocess_data(df)  # Apply preprocessing logic
                df = st.session_state.df  # Get the current DataFrame

            else:
                ""#st.session_state.df = df
                #st.warning("No data uploaded yet. Please upload a file first.")
                
            #st.dataframe(st.session_state.df.head())  
            #st.session_state.df = df
            
        
        if data_eda == "Explore with LLM":
            # Check if the DataFrame exists in session state
            st.write("Exploring the Data with LLM:")

            if st.session_state.df is not None:
                st.dataframe(st.session_state.df.head())  

            else:
                ""#st.warning("No data uploaded yet. Please upload a file first.")

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space

            st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)  
            
            if st.session_state.df is not None:
                df = st.session_state.df  # Get the preprocessed DataFrame from session_state
            
            #st.dataframe(st.session_state.df .head()) 
            st.sidebar.write("EDA with LLM is selected")

            
            preprocessed_csv = crew_ai.df_csv_io(df)
            
            wip_preprocessed_csv = crew_ai.generate_create_csv(preprocessed_csv)
            
            user_question = None

            output_capture = OutputCapture()

            if wip_preprocessed_csv is not None:
                st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space

                st.write("Ask questions about the data or request a chart.")
                user_question = st.text_input("Text summary of data exploration with LLM: ")

                if user_question is not None and user_question != "":
                    with st.spinner(text="In progress..."):
                        action_input = user_question
                        response = wip_preprocessed_csv.run(user_question)
                        
                        #st.subheader("Action Input:")
                        st.write("**:green[User Request:]**")
                        st.text(action_input)

                        st.write("**:green[Internal Output:]**")
                        captured_output = output_capture.get_output()
                        cleaned_output = re.sub(r'\x1b\[[0-9;]*m', '', captured_output)
                        output_lines = cleaned_output.split('\n')

                        if len(output_lines) >= 6:
                            st.text('\n'.join(output_lines[5:]))
                        else:
                            st.text(cleaned_output)

                        #st.subheader("Final Answer:")
                        st.write("**:green[Final Response:]**")
                        for part in response.split("\n\n"):
                            st.write(part)



                        # Check if the user requested a chart                                  
                        #if "chart" in user_question.lower() or "graph" in user_question.lower() or "plot" in user_question.lower():
                            #st.subheader("Generated Chart:")

                        # Check if the user requested a chart   
                        generate_viz = {
                            "chart", "graph", "plot", "visual", "viz", "visualisation", "diagram", "figure", "fig",
                            "bar", "line", "scatter", "pie", "histogram", "area", "heatmap", 
                            "boxplot", "whisker", "bubble", "treemap", "sunburst", "radar", 
                            "sankey", "funnel", "gantt", "trend", "distribution", "correlation",
                            "map", "choropleth", "violin", "timeline", "timeseries", "hist"
                        }

                        if any(word in user_question.lower() for word in generate_viz):
                            st.subheader("Generated Chart:")
                                                                
                            width = st.slider("Chart width", min_value=1, max_value=20, value=10)
                            height = st.slider("Chart height", min_value=1, max_value=20, value=6)
                            
                            try:
                                # Convert datetime columns
                                datetime_columns = df.select_dtypes(include=['object']).columns
                                for col in datetime_columns:
                                    try:
                                        df[col] = pd.to_datetime(df[col])
                                    except:
                                        pass
                                
                                chart_type = st.selectbox("Select chart type", [
                                    "Bar", "Line", "Scatter", "Area", "Histogram", "Box", "Violin", 
                                    "Heatmap", "Pie", "Sunburst", "Treemap", "Funnel", "Waterfall", "Parallel Coordinates", "Parallel Categories"
                                ])
                                
                                # Allow multiple column selection
                                selected_columns = st.multiselect("Select columns to plot", df.columns, default=df.columns[0])
                                
                                # Time aggregation for datetime columns
                                time_agg = {}
                                selected_year = None
                                datetime_columns = [col for col in selected_columns if pd.api.types.is_datetime64_any_dtype(df[col])]

                                if datetime_columns:
                                    min_year = min(df[col].dt.year.min() for col in datetime_columns)
                                    max_year = max(df[col].dt.year.max() for col in datetime_columns)
                                    selected_year = st.slider("Select year", min_value=min_year, max_value=max_year, value=min_year)

                                for col in selected_columns:
                                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                                        agg_option = st.selectbox(f"Time aggregation for {col}", 
                                                                ["None", "Year", "Quarter", "Month", "Week", "Day", "Hour", "Month of Year", "Day of Week"])
                                        if agg_option != "None":
                                            time_agg[col] = agg_option.lower()

                                # Filter data for selected year
                                if selected_year is not None:
                                    df = df[df[datetime_columns[0]].dt.year == selected_year]

                                # Apply time aggregation
                                for col, agg in time_agg.items():
                                    if agg == "month of year":
                                        df[f"{col}_{agg}"] = pd.Categorical(df[col].dt.strftime('%B'), 
                                                                            categories=list(calendar.month_name)[1:], 
                                                                            ordered=True)
                                    elif agg == "day of week":
                                        df[f"{col}_{agg}"] = pd.Categorical(df[col].dt.strftime('%A'), 
                                                                            categories=list(calendar.day_name), 
                                                                            ordered=True)
                                    elif agg == "quarter":
                                        df[f"{col}_{agg}"] = pd.Categorical(df[col].dt.to_period('Q').astype(str), 
                                                                            categories=['Q1', 'Q2', 'Q3', 'Q4'], 
                                                                            ordered=True)
                                    elif agg == "week":
                                        df[f"{col}_{agg}"] = pd.Categorical(df[col].dt.isocalendar().week, 
                                                                            categories=range(1, 54), 
                                                                            ordered=True)
                                    else:
                                        df[f"{col}_{agg}"] = getattr(df[col].dt, agg)
                                    selected_columns = [f"{col}_{agg}" if c == col else c for c in selected_columns]

                                # Additional feature selection based on chart type
                                color_column = st.selectbox("Select color column (optional)", ["None"] + list(df.columns))
                                size_column = st.selectbox("Select size column (optional)", ["None"] + list(df.columns))

                                # Common parameters
                                params = {
                                    "data_frame": df,
                                    "color": None if color_column == "None" else color_column,
                                    "title": f"{chart_type} Chart" + (f" for Year {selected_year}" if selected_year else "")
                                }

                                # Generate chart based on chart type
                                if chart_type in ["Bar", "Line", "Scatter", "Area"]:
                                    params["x"] = selected_columns[0]
                                    params["y"] = selected_columns[1:] if len(selected_columns) > 1 else selected_columns[0]
                                    if chart_type == "Bar":
                                        fig = px.bar(**params)
                                    elif chart_type == "Line":
                                        fig = px.line(**params)
                                    elif chart_type == "Scatter":
                                        params["size"] = None if size_column == "None" else size_column
                                        fig = px.scatter(**params)
                                    elif chart_type == "Area":
                                        fig = px.area(**params)
                                elif chart_type == "Histogram":
                                    #fig = px.histogram(df, x=selected_columns, color=color_column)
                                    fig = px.histogram(df, x=selected_columns[0], color=color_column if color_column != "None" else None)

                                elif chart_type == "Radar":
                                    fig = px.line_polar(df, r=selected_columns[0], theta=selected_columns[1] if len(selected_columns) > 1 else None, line_close=True)

                                elif chart_type in ["Box", "Violin"]:
                                    params["x"] = selected_columns[0]
                                    params["y"] = selected_columns[1] if len(selected_columns) > 1 else None
                                    fig = px.box(**params) if chart_type == "Box" else px.violin(**params)
                                elif chart_type == "Heatmap":
                                    fig = px.density_heatmap(df, x=selected_columns[0], y=selected_columns[1] if len(selected_columns) > 1 else None)

                                elif chart_type == "Gantt":
                                    # Assumes: Col 0 = Task, Col 1 = Start, Col 2 = Finish
                                    fig = px.timeline(df, x_start=selected_columns[1], x_end=selected_columns[2], y=selected_columns[0])
                                    fig.update_yaxes(autorange="reversed")

                                elif chart_type == "Choropleth":
                                    fig = px.choropleth(df, locations=selected_columns[0], color=selected_columns[1] if len(selected_columns) > 1 else None)

                                elif chart_type == "Parallel Coordinates":
                                    #fig = px.parallel_coordinates(df, dimensions=selected_columns, color=color_column)
                                    fig = px.parallel_coordinates(df, dimensions=selected_columns, color=color_column if color_column != "None" else None)

                                elif chart_type == "Parallel Categories":
                                    #fig = px.parallel_categories(df, dimensions=selected_columns, color=color_column)
                                    fig = px.parallel_categories(df, dimensions=selected_columns, color=color_column if color_column != "None" else None)

                                elif chart_type in ["Pie", "Sunburst", "Treemap"]:
                                    params["values"] = selected_columns[-1]
                                    params["names" if chart_type == "Pie" else "path"] = selected_columns[:-1]
                                    if chart_type == "Pie":
                                        fig = px.pie(**params)
                                    elif chart_type == "Sunburst":
                                        fig = px.sunburst(**params)
                                    else:
                                        fig = px.treemap(**params)

                                elif chart_type == "Funnel":
                                    fig = px.funnel(df, x=selected_columns[0], y=selected_columns[1] if len(selected_columns) > 1 else None)
                                elif chart_type == "Waterfall":
                                    fig = px.waterfall(df, x=selected_columns[0], y=selected_columns[1] if len(selected_columns) > 1 else None)

                                
                                fig.update_layout(width=width*50, height=height*50)
                                st.plotly_chart(fig, use_container_width=True)
                        
                            except Exception as e:
                                st.error(f"Could not generate {chart_type}. Please ensure you have selected the correct columns for this format.")

            st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True) # space

            st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True) 


        # if data_eda == "Preprocess with LLM":
                
        #     pwllm = PreprocessWithLLMAgent()
        #     output_capture = OutputCapture()

        #     st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

        #     st.markdown("#### Interactive LLM Data Processing")

        #     pwllm.run(output_capture)
            
        #     st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True) # space
        #     st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)




        if data_eda == "Preprocess with LLM":

            if "new_df" not in st.session_state:
                st.session_state.new_df = None
            if "old_df" not in st.session_state:
                st.session_state.old_df = None

            #if "df" not in st.session_state:
                #st.session_state.df = original_df

            if "transformed" not in st.session_state:
                st.session_state.transformed = False


            pwllm = PreprocessWithLLMAgent()
            
            output_capture = OutputCapture()
            st.session_state.new_df, original_df = pwllm.run(output_capture) 


            if st.session_state.new_df is None:
                st.write("")

            else:
                for col in st.session_state.new_df.columns:
                    if st.session_state.new_df[col].dtype == 'object':
                        st.session_state.new_df[col] = st.session_state.new_df[col].astype(str)

            if original_df is None:
                st.write("")

            else:
                for col in original_df.columns:
                    if original_df[col].dtype == 'object':
                        original_df[col] = original_df[col].astype(str)    


            if st.session_state.new_df is not None and not st.session_state.new_df.empty:
                # assign the new DataFrame to session state of new_df
                st.session_state.df = st.session_state.new_df
                st.success("✅ Preprocessing with LLM completed successfully!")
            else:
                st.warning("⚠️ No new DataFrame was generated or it is empty.")

            
        
            col1, col2 = st.columns(2)

            with col1:
                #Callback
                def activate_transformation():
                    st.session_state.df = st.session_state.new_df
                    st.session_state.transformed = True

                # 3. Widget with callback
                st.button("Confirm Transformation", on_click=activate_transformation)

                # 4. Display logic based on state
                if st.session_state.transformed:
                    st.success("✅ Transformed DataFrame Activated!")
                    st.dataframe(st.session_state.df)

                #if st.button("Confirm Transformation", use_container_width=True, key="confirm_trnfm_btn"):
                    st.success("✅ Transformed DataFrame Activated!")
                    st.write(f"Shape: Old df: {original_df.shape}, New df: {st.session_state.df.shape}")

                    st.markdown("##### Active Dataframe")
                    st.dataframe(st.session_state.df)


            with col2:
                if st.button("Revert to Old DataFrame", use_container_width=True, key="reset_trnfm_df_btn"):
                    st.session_state.df = original_df
                    st.write(original_df.shape)
                    st.markdown("##### Active Dataframe")
                    st.dataframe(st.session_state.original_df)
            



    else:
        st.sidebar.info("Check box EDA Options.")

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)


    #############
    # The DE Pipeline

    # Initialize session state variables
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'temp_df' not in st.session_state:
        st.session_state.temp_df = None 
    if 'original_df' not in st.session_state:
        st.session_state.original_df = None
    if 'statuses' not in st.session_state:
        st.session_state.statuses = {}
    if 'checkbox_key' not in st.session_state:
        st.session_state.checkbox_key = None
    if 'step_outputs' not in st.session_state:
        st.session_state.step_outputs = {}
    if 'visualizer' not in st.session_state:
        st.session_state.visualizer = None

    if 'current_step' not in st.session_state:
        st.session_state.current_step = 0  # Initialize it to 0 or any appropriate value

    if 'standardize_column' not in st.session_state:
        st.session_state.standardize_column = None



    enable_pipeline= st.sidebar.checkbox("Enable Pipeline Options", key="pipeline_checkbox", on_change=enable_pipeline_chkbx)

    if not enable_pipeline:
        if 'steps' in st.session_state:
            for step in st.session_state.steps:
                if step in st.session_state.checkbox_states:
                    st.session_state.checkbox_states[step] = False
                    

    # Only show the radio button if the checkbox is checked
    if enable_pipeline:
        enable_pipeline_1 = st.markdown(

    """
    ---
    **Notes for Enable EDA Options**  
    EDA - explore the data

    - **Preprocess Data:** *To provide some information and basic processing.*
    - **Statistics Report:** *Provide several statistics about the DataFrame.*
    - **Explore with LLM:** *To explore the DataFrame with LLM.*
    - **Preprocess with LLM:** *To preprocess the DataFrame in collaboration with LLM suggestions.*
    - **Preprocess Data:** *To use predefined solutions to process the DataFrame*
    """
    , unsafe_allow_html=True

    )
    
    if enable_pipeline:

            
        # Initialize steps and checkbox states
        if 'steps' not in st.session_state:
            st.session_state.steps = [
                "Input Data",
                "Data Standardization with AI",
                "Feature Engineering",
                "Missing Data",
                "Standardize Column Data",
                "Inflection AI Standard Feature Name",
                "Human AI Standard Feature Values",
                "Data Anomaly Evaluation",
                "Time Series Evaluation",
                "Data Normalisation"

            ]
            
        if 'radio_selections' not in st.session_state:
            st.session_state.radio_selections = {step: "Idle" for step in st.session_state.steps}
            
        if 'radio_selections_states' not in st.session_state:
                st.session_state.radio_selections_states = {step: False for step in st.session_state.steps}
        if 'checkbox_states' not in st.session_state:
            # Initialize checkbox states for all steps
            st.session_state.checkbox_states = {step: False for step in st.session_state.steps}
   

        #data = pd.read_csv(uploaded_file)
        if st.session_state.df is not None:
            data = st.session_state.df
        else:
            "Upload Data"

        # Initialize session state for step statuses
        for step in st.session_state.steps:
            if step not in st.session_state.statuses:
                st.session_state.statuses[step] = 'idle'



        # Sidebar controls for pipeline steps
        with st.sidebar:
            st.markdown("### Pipeline Steps")

            # Initialize if not present
            if "checkbox_states" not in st.session_state:
                st.session_state.checkbox_states = {step: False for step in st.session_state.steps}
            if "active_steps_order" not in st.session_state:
                st.session_state.active_steps_order = []


            # Callback function to synchronize checkbox states and statuses
            def update_checkbox_state(step):
                if step not in st.session_state.statuses:
                    st.session_state.statuses[step] = 'idle'
                
                if step not in st.session_state.checkbox_states:
                    st.session_state.checkbox_states[step] = False
                
                # Read the actual checkbox state from the widget's session state
                checkbox_key = f"checkbox_{step}"
                is_checked = st.session_state.get(checkbox_key, False)
                st.session_state.checkbox_states[step] = is_checked
                
                st.session_state.statuses[step] = 'current' if is_checked else 'idle'
                
                # When unchecking a step, also clear its sub-steps
                if not st.session_state.checkbox_states[step]:
                    # Clear Feature Engineering sub-steps
                    if step == "Feature Engineering" and 'fe_active_order' in st.session_state:
                        st.session_state.fe_active_order = []
                        st.session_state.fe_checkbox_states = {sub_step: False for sub_step in st.session_state.get('fe_sub_steps', [])}
                        # Also reset the working_df so it re-initializes when step is re-enabled
                        st.session_state.fe_reset_working_df = True
                    # Clear Data Standardization sub-steps
                    elif step == "Data Standardization with AI" and 'standardization_active_order' in st.session_state:
                        st.session_state.standardization_active_order = []
                        st.session_state.standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.get('standardization_sub_steps', [])}
                    # Clear Missing Data sub-steps
                    elif step == "Missing Data" and 'missing_data_active_order' in st.session_state:
                        st.session_state.missing_data_active_order = []
                        st.session_state.missing_data_checkbox_states = {sub_step: False for sub_step in st.session_state.get('missing_data_sub_steps', [])}
                    # Clear Column Standardization sub-steps
                    elif step == "Standardize Column Data" and 'column_standardization_active_order' in st.session_state:
                        st.session_state.column_standardization_active_order = []
                        st.session_state.column_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.get('column_standardization_sub_steps', [])}
                    # Clear Inflection Standardization sub-steps
                    elif step == "Inflection AI Standard Feature Name" and 'inflection_standardization_active_order' in st.session_state:
                        st.session_state.inflection_standardization_active_order = []
                        st.session_state.inflection_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.get('inflection_standardization_sub_steps', [])}
                    # Clear Human Standardization sub-steps

                    elif step == "Human AI Standard Feature Values" and 'human_ai_standardization_active_order' in st.session_state:
                        st.session_state.human_ai_standardization_active_order = []
                        if 'human_ai_standardization_checkbox_states' not in st.session_state:
                            st.session_state.human_ai_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.get('human_ai_standardization_sub_steps', [])}

                    # Clear Anomaly Evaluation sub-steps
                    elif step == "Data Anomaly Evaluation" and 'anomaly_active_order' in st.session_state:
                        st.session_state.anomaly_active_order = []
                        st.session_state.anomaly_checkbox_states = {sub_step: False for sub_step in st.session_state.get('anomaly_sub_steps', [])}

                    elif step == "Time Series Evaluation" and 'timeseries_active_order' in st.session_state:
                        st.session_state.timeseries_active_order = []
                        st.session_state.timeseries_checkbox_states = {sub_step: False for sub_step in st.session_state.get('timeseries_sub_steps', [])}  


                    # Clear Normalization sub-steps
                    elif step == "Data Normalisation" and 'normalization_active_order' in st.session_state:
                        st.session_state.normalization_active_order = []
                        st.session_state.normalization_checkbox_states = {sub_step: False for sub_step in st.session_state.get('normalization_sub_steps', [])}
                
                # Trigger a refresh of the pipeline diagram
                st.session_state.refresh_pipeline = True


            for step in st.session_state.steps:
                checked = st.checkbox(
                    step, 
                    value=st.session_state.checkbox_states.get(step, False), 
                    key=f"checkbox_{step}",
                    on_change=update_checkbox_state,
                    args=(step,)
                )

                # Manage order list: add if checked and not in list
                if checked and step not in st.session_state.active_steps_order:
                    st.session_state.active_steps_order.append(step)
                # Remove if unchecked and still in list
                if not checked and step in st.session_state.active_steps_order:
                    st.session_state.active_steps_order.remove(step)


        # Main title
        st.markdown("#### Pipeline for Data Analysis and Engineering")


        # Sidebar - Pipeline Status
        with st.sidebar:
            st.header("📊 Pipeline Status")
            
            # Show pipeline summary
            summary = pipeline.get_pipeline_summary()
            

            if summary.get('current_shape') is not None:
                st.success(f"{summary['total_steps']} steps completed")
                st.write(f"**Current data:** {summary['current_shape'][0]} rows, {summary['current_shape'][1]} columns")
            else:
                st.write("Summary data is not available.")


                # Show step history
                with st.expander("Step History"):
                    steps = pipeline.list_steps()
                    for i, (step_name, info) in enumerate(steps.items(), 1):
                        st.write(f"{i}. {step_name}")
                        st.caption(f"   {info['timestamp'][:10]} | {info['shape'][0]} rows")
                
                # Reset button
                if st.button("Reset Entire Pipeline", type="secondary"):
                    pipeline.clear_all()
                    st.rerun()
                else:
                    st.info("No steps completed yet")

                
        
        # Make the pipeline visualization sticky
        #pipeline_container = sticky_container(height=200, mode="top", margin="2.875rem")
        pipeline_container = sticky_container(height=200, mode="top", margin="2.875rem")
        
        with pipeline_container:
            #st.markdown("##### Pipeline Flow")
            #st.write("Pipeline Flow")
            
            # Sort active_steps by checkbox states
            active_steps = st.session_state.active_steps_order

            if active_steps:  
                visualizer = PipelineDiagramAgent()
                
                # Create a combined list of all nodes in execution order
                all_nodes = []
                
                # Add active steps and their sub-steps in execution order
                for step in active_steps:
                    # Add the main step
                    all_nodes.append(step)
                    #visualizer._add_node(step=step, statuses=st.session_state.statuses)
                    visualizer._add_node(step=step, statuses=st.session_state.statuses, is_substep=False)
                    
                    # Add sub-steps for this specific step in order
                    sub_steps_added = []
                    
                    # Feature Engineering sub-steps
                    if step == "Feature Engineering" and 'fe_active_order' in st.session_state and st.session_state.fe_active_order:
                        for sub_step in st.session_state.fe_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Feature Engineering")
                            sub_steps_added.append(sub_step)
                    
                    # Data Standardization with AI sub-steps
                    elif step == "Data Standardization with AI" and 'standardization_active_order' in st.session_state and st.session_state.standardization_active_order:
                        for sub_step in st.session_state.standardization_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Data Standardization with AI")
                            sub_steps_added.append(sub_step)
                    
                    # Missing Data sub-steps
                    elif step == "Missing Data" and 'missing_data_active_order' in st.session_state and st.session_state.missing_data_active_order:
                        for sub_step in st.session_state.missing_data_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Missing Data")
                            sub_steps_added.append(sub_step)
                    
                    # Standardize Column Data sub-steps
                    elif step == "Standardize Column Data" and 'column_standardization_active_order' in st.session_state and st.session_state.column_standardization_active_order:
                        for sub_step in st.session_state.column_standardization_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Standardize Column Data")
                            sub_steps_added.append(sub_step)
                    
                    # Inflection AI Standard Feature Name sub-steps
                    elif step == "Inflection AI Standard Feature Name" and 'inflection_standardization_active_order' in st.session_state and st.session_state.inflection_standardization_active_order:
                    #elif step == "Inflection AI Standard Feature Name" and 'inflection_standardization_active_order' in st.session_state and len(st.session_state.inflection_standardization_active_order) > 0:
                        for sub_step in st.session_state.inflection_standardization_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Inflection AI Standard Feature Name")
                            sub_steps_added.append(sub_step)
                    
                    # Data Anomaly Evaluation sub-steps
                    elif step == "Data Anomaly Evaluation" and 'anomaly_active_order' in st.session_state and st.session_state.anomaly_active_order:
                        for sub_step in st.session_state.anomaly_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Data Anomaly Evaluation")
                            sub_steps_added.append(sub_step)

                    # Time Series Evaluation sub-steps
                    elif step == "Time Series Evaluation" and 'timeseries_active_order' in st.session_state and st.session_state.timeseries_active_order:
                        for sub_step in st.session_state.timeseries_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Time Series Evaluation")
                            sub_steps_added.append(sub_step)

                    
                    # Human AI Standard Feature Values sub-steps
                    elif step == "Human AI Standard Feature Values" and 'human_ai_standardization_active_order' in st.session_state and st.session_state.human_ai_standardization_active_order:
                        for sub_step in st.session_state.human_ai_standardization_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Human AI Standard Feature Values")
                            sub_steps_added.append(sub_step)


                    # Data Normalisation sub-steps
                    elif step == "Data Normalisation" and 'normalization_active_order' in st.session_state and st.session_state.normalization_active_order:
                        for sub_step in st.session_state.normalization_active_order:
                            all_nodes.append(sub_step)
                            visualizer._add_node(step=sub_step, statuses=st.session_state.statuses, is_substep=True, parent_step="Data Normalisation")
                            sub_steps_added.append(sub_step)
                    
                    # Connect sub-steps linearly to each other, then to main step
                    if sub_steps_added:
                        # Connect sub-steps linearly to each other in reverse order
                        # sub_step_3 --> sub_step_2 --> sub_step_1
                        for i in range(len(sub_steps_added) - 1, 0, -1):
                            visualizer.add_edge(sub_steps_added[i], sub_steps_added[i - 1])
                        
                        # Connect first sub-step to main step
                        if sub_steps_added:
                            visualizer.add_edge(sub_steps_added[0], step)
                        
                        # Connect main step to next main step (if any)
                        next_step_index = active_steps.index(step) + 1
                        if next_step_index < len(active_steps):
                            next_main_step = active_steps[next_step_index]
                            visualizer.add_edge(step, next_main_step)
                    else:
                        # No sub-steps, connect directly to next main step
                        next_step_index = active_steps.index(step) + 1
                        if next_step_index < len(active_steps):
                            next_main_step = active_steps[next_step_index]
                            visualizer.add_edge(step, next_main_step)

                # Render the network graph in Streamlit app
                #visualizer.render()
                visualizer.render(show_download=True)

            else:
                st.write("No components added yet - check steps in sidebar")






        # Ensure session state variables are initialized
        if 'checkbox_states' not in st.session_state:
            st.session_state.checkbox_states = {step: False for step in st.session_state.steps}



        if 'statuses' not in st.session_state:
            st.session_state.statuses = {step: 'idle' for step in st.session_state.steps}

        for step in st.session_state.steps:
            if f"radio_{step}" not in st.session_state:
                st.session_state[f"radio_{step}"] = "Idle"

        # Define the callback function
        def update_radio_selections_states(step):
            action = st.session_state[f"radio_{step}"]

            # Don't update if step is permanently accepted
            if st.session_state.get(f"{step}_permanently_accepted", False):
                return

            # Ensure statuses has the step key before accessing
            if step not in st.session_state.statuses:
                st.session_state.statuses[step] = 'idle'

            if action == "Accept":
                st.session_state.statuses[step] = "completed"
                st.success(f"Changes for {step} have been accepted.", icon="✅")
            elif action == "Reject":
                st.session_state.statuses[step] = "idle"
                st.warning(f"Changes for {step} have been rejected. Reverting to previous state.", icon="⚠️")

                # Reset statuses of subsequent steps
                step_index = st.session_state.steps.index(step)
                for subsequent_step in st.session_state.steps[step_index:]:
                    # Ensure subsequent_step exists in statuses before accessing
                    if subsequent_step not in st.session_state.statuses:
                        st.session_state.statuses[subsequent_step] = 'idle'
                    st.session_state.statuses[subsequent_step] = "idle"
                    # Ensure subsequent_step exists in radio_selections before accessing
                    if subsequent_step not in st.session_state.radio_selections:
                        st.session_state.radio_selections[subsequent_step] = "Idle"
                    st.session_state.radio_selections[subsequent_step] = "Idle"
                    # Also reset permanent acceptance for subsequent steps
                    st.session_state[f"{subsequent_step}_permanently_accepted"] = False

            # Update the selection state
            st.session_state.radio_selections[step] = action


# Start the @st.fragment code block here
        @st.fragment
        def render_input_data_step():
            # Safety check - no data
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available for processing. Please upload a file first.")
                return
            
            # CRITICAL: Return early if already completed
            if st.session_state.get('input_data_completed', False):
                st.info("✅ Input Data step already completed")
                st.dataframe(st.session_state.df.head(10))  # Show only first 10 rows for performance
                return
            
            # Process data only if not completed
            st.write("### Processing Input Data Step")
            st.write("Cleaning and preparing the raw data...")
            
            cleaned_df = pydeamlpipeline.clean_data(st.session_state.df)
            st.session_state.df = cleaned_df
            st.session_state.input_data_completed = True
            
            st.success("✅ Input Data processing completed!")
            st.dataframe(st.session_state.df.head())  # Show only first 10 rows

            
            # Show distinct values
            with st.expander("Distinct Values per Feature"):
                unique_data = {
                    "Feature Name": st.session_state.df.columns,
                    "Distinct Values": [", ".join(map(str, st.session_state.df[col].unique())) for col in st.session_state.df.columns],
                    "Count": [st.session_state.df[col].nunique() for col in st.session_state.df.columns]
                }
                unique_df = pd.DataFrame(unique_data)
                st.table(unique_df)
            
            st.markdown("---")



        # Main step fragment
        @st.fragment
        def render_missing_data_step():
            st.write("**Current DataFrame before Missing Data:**")
            
            # Add safety check
            if st.session_state.df is not None:
                st.dataframe(st.session_state.df.head())
            else:
                st.warning("No data available.")
                return


            # Initialize missing data sub-steps if not present
            if 'missing_data_sub_steps' not in st.session_state:
                st.session_state.missing_data_sub_steps = [
                    "Detect Missing Data", "Display Missing Data", 
                    "Fix Missing Data with AI", "Fix Missing Data without AI",
                    "Replace Missing Data", "Smart Contextual Fill"
                ]
            
            if 'missing_data_checkbox_states' not in st.session_state:
                st.session_state.missing_data_checkbox_states = {sub_step: False for sub_step in st.session_state.missing_data_sub_steps}
            
            if 'missing_data_active_order' not in st.session_state:
                st.session_state.missing_data_active_order = []

            st.markdown("#### Missing Data Sub-Steps")
            st.markdown("Select the missing data operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.missing_data_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.missing_data_checkbox_states.get(sub_step, False),
                        key=f"missing_data_{sub_step}_{step}"
                    )
                    st.session_state.missing_data_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.missing_data_active_order:
                        st.session_state.missing_data_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.missing_data_active_order:
                        st.session_state.missing_data_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.missing_data_active_order:
                st.markdown("##### Applying Selected Missing Data Operations")
                
                # Create a copy of the dataframe to work with
                working_df = st.session_state.df.copy()

                #st.write("working_df")
                st.dataframe(working_df.head())
                
                for sub_step in st.session_state.missing_data_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Detect Missing Data":
                        render_detect_missing_data_substep(working_df)
                    elif sub_step == "Display Missing Data":
                        render_display_missing_data_substep(working_df)
                    elif sub_step == "Fix Missing Data with AI":
                        working_df = render_fix_missing_data_with_ai_substep(working_df)
                    elif sub_step == "Fix Missing Data without AI":
                        working_df = render_fix_missing_data_without_ai_substep(working_df)
                    elif sub_step == "Replace Missing Data":
                        working_df = render_replace_missing_data_substep(working_df)
                    elif sub_step == "Smart Contextual Fill":
                        working_df = render_smart_contextual_fill_substep(working_df)
                    
                    st.markdown("---")
                
                # Final update - ensure working_df is saved
                st.session_state.df = working_df
                st.success("✅ Missing Data operations completed!")
            else:
                st.info("Select at least one missing data sub-step to apply.")


        # Sub-step fragments
        @st.fragment
        def render_detect_missing_data_substep(working_df):
            if 'df' in st.session_state and st.session_state.df is not None:
                result = crewailpydeamlpipeline.detect_missing_data(working_df)
                if isinstance(result, pd.DataFrame):
                    working_df = result
                    st.success("Missing data detection completed")
                else:
                    st.warning("Missing data detection completed manually")
                    total_rows = len(working_df)
                    missing_count = working_df.isnull().sum()[working_df.isnull().sum() > 0]
                    missing_percentage = ((missing_count / total_rows) * 100).round(2)
                    
                    report_df = pd.DataFrame({
                        'Column': missing_count.index,
                        'Missing Count': missing_count.values,
                        'Missing %': missing_percentage.values
                    })
                    st.dataframe(report_df)
            return working_df


        @st.fragment
        def render_display_missing_data_substep(working_df):
            cols_with_missing = working_df.columns[working_df.isnull().any()].tolist()
            if not cols_with_missing:
                st.success("🎉 No missing data found in any columns!")
            else:
                feature_names = st.multiselect(
                    "Select columns to check for missing data:",
                    options=cols_with_missing,
                    key=f"missing_data_select_{sub_step}_{step}"
                )
                if feature_names:
                    try:
                        mis_data = crewailpydeamlpipeline.display_missing_data(working_df, feature_names)
                        mis_data = mis_data.rename_axis("Index")
                        st.session_state["mis_data"] = mis_data
                        if not mis_data.empty:
                            st.markdown("###### Rows with Missing Data")
                            st.dataframe(st.session_state["mis_data"], height=300)
                        else:
                            st.write("No missing data found in the selected rows.")
                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
            return working_df


        @st.fragment
        def render_fix_missing_data_with_ai_substep(working_df):
            st.write("### Existing DataFrame with Missing Values")

            st.session_state.df = pipeline.load_data()
            working_df = st.session_state.df

            st.dataframe(working_df.head())
            st.markdown("##### Perform Missing Data Fix with LLM")
            pwllm = PreprocessWithLLMAgent()
            process_smart_missing_data_agent = SmartMissingDataFiller()
            smart_filled_df = process_smart_missing_data_agent.smart_contextual_fill_ui(working_df)
            st.session_state["smart_filled_result_df"] = smart_filled_df
            working_df = smart_filled_df
            
            # CRITICAL: Update session state immediately
            st.session_state.df = working_df
            if working_df is not None:
                st.success("✅ Missing data fix with AI completed!")
                st.dataframe(working_df.head())
                pipeline.save_data(st.session_state.df, f'missing_data_ai_fix', agent_used='missing_data_ai_fix_agent')
                st.success(f"missing_data_ai_fix")
                st.dataframe(pipeline.load_data().head())
                st.write("### AI Contextual Fill Output")
                st.dataframe(working_df.head())
                st.write("### AI Contextual Testing Fill Output")
                st.dataframe(st.session_state["smart_filled_result_df"].head())
            else:
                st.warning("No missing data found.")
            return working_df


        @st.fragment
        def render_fix_missing_data_without_ai_substep(working_df):
            st.session_state.df = pipeline.load_data()
        
            working_df = st.session_state.df
            fixed_df = crewailpydeamlpipeline.fix_missing_data(working_df)
            if fixed_df is not None:
                working_df = fixed_df
                # CRITICAL: Update session state immediately
                st.session_state.df = working_df
                st.dataframe(working_df.head())
                pipeline.save_data(st.session_state.df, f'missing_data_without_ai_fix', agent_used='replace_agent')
                st.success(f"missing_data_without_ai_fix")
                st.dataframe(pipeline.load_data().head())
            return working_df


        @st.fragment
        def render_replace_missing_data_substep(working_df):
            st.session_state.df = pipeline.load_data()
            working_df = st.session_state.df
            df = crewailpydeamlpipeline.replace_missing_data(working_df)
            if df is not None:
                working_df = df
                # CRITICAL: Update session state immediately
                st.session_state.df = working_df
                st.dataframe(working_df.head())
                pipeline.save_data(st.session_state.df, f'replace_missing_data', agent_used='replace_agent')
                
                if "initial_missing" not in st.session_state:
                    st.session_state.initial_missing = set(working_df.columns[working_df.isnull().any()])
                current_missing = set(working_df.columns[working_df.isnull().any()])
                fixed_cols = list(st.session_state.initial_missing - current_missing)
                
                st.write(f"Missing data replaced successfully for: {fixed_cols}")
                st.write(f"Still needs fixing: {list(current_missing)}")
                st.dataframe(pipeline.load_data().head())
            return working_df


        @st.fragment
        def render_smart_contextual_fill_substep(working_df):
            if st.session_state.df is None or 'df' not in st.session_state.df:
                st.session_state.df = pipeline.load_data()
            working_df = st.session_state.df

            filler = SmartMissingDataFiller()
            result_df = filler.smart_contextual_fill_ui(working_df)
            working_df = result_df
            # CRITICAL: Update session state immediately
            st.session_state.df = working_df
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'smart_contextual_fill', agent_used='smart_contextual_fill_agent')
            st.success(f"smart_contextual_fill")
            st.dataframe(pipeline.load_data().head())
            return working_df
        

        # Main step fragment
        @st.fragment
        def render_data_standardization_with_ai_step():
            # Safety check - ensure data exists
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return

            if "standardization_run_queue" not in st.session_state:
                st.session_state.standardization_run_queue = set()

            
            st.dataframe(st.session_state.df.head())


            if 'standardization_sub_steps' not in st.session_state:
                st.session_state.standardization_sub_steps = [
                    "Auto Standardize All Columns", "Manual Column Selection"
                ]

            if 'standardization_checkbox_states' not in st.session_state:
                st.session_state.standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.standardization_sub_steps}

            if 'standardization_active_order' not in st.session_state:
                st.session_state.standardization_active_order = [] 

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.standardization_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.standardization_checkbox_states.get(sub_step, False),
                        key=f"standardization_{sub_step}_{step}"
                    )
                    st.session_state.standardization_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.standardization_active_order:
                        st.session_state.standardization_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.standardization_active_order:
                        st.session_state.standardization_active_order.remove(sub_step)
                        st.rerun()


            
            # Apply selected sub-steps
            if st.session_state.standardization_active_order:

                for sub_step in st.session_state.standardization_active_order:


                    st.markdown(f"**{sub_step}:**")

                    if sub_step == "Auto Standardize All Columns":
                        render_auto_standardize_all_columns_substep()

                    elif sub_step == "Manual Column Selection":
                        render_manual_column_selection_substep()

                    st.session_state.standardization_run_queue.discard(sub_step)

                    st.markdown("---")
                    
            else:
                st.info("Select at least one standardization sub-step to apply.")



        # Sub-step fragments
        @st.fragment
        def render_auto_standardize_all_columns_substep():

            st.session_state.run_auto_std = False

            try:
                data_standardizer = DataStandardizerAI(use_ai=True)

                if 'working_df' not in st.session_state:
                    st.session_state.working_df = pipeline.load_data()


                all_columns = st.session_state.working_df.columns.tolist()

                if st.button("Perform Auto Standardization run", key="try_auto_std_btn"):
                    
                    with st.spinner("AI is analyzing and standardizing all columns..."):
                        standardized_df = data_standardizer.auto_standardize_dataframe(st.session_state.working_df, all_columns)
                        st.session_state.working_df = standardized_df
                        st.session_state.df = standardized_df
                        st.success("✅ Auto standardization complete!")
                        st.dataframe(st.session_state.working_df.head())
                        
                        # Mark as completed
                        if 'standardization_substep_completed' not in st.session_state:
                            st.session_state.standardization_substep_completed = {}
                        st.session_state.standardization_substep_completed["Auto Standardize All Columns"] = True
                        st.success("✅ Auto Standardize All Columns applied successfully!")

                        pipeline.save_data(st.session_state.df, f'auto_data_standz_with_ai', agent_used='auto_data_standz_with_ai_agent')
                        
                        st.success("Data saved to pipeline!")
                        st.dataframe(pipeline.load_data().head())

                        # Download button for standardized data
                        #st.session_state.df = pipeline.load_data()
                        if st.session_state.df is not None:
                            csv_bytes = st.session_state.df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                "Download Auto Standardized With AI CSV",
                                data=csv_bytes,
                                file_name="auto_standardized_with_ai_data.csv",
                                mime="text/csv"
                            )


            except Exception as e:
                st.error(f"Auto standardization failed: {str(e)}")


        @st.fragment
        def render_manual_column_selection_substep():
            st.dataframe(st.session_state.working_df.head())

            if 'working_df' not in st.session_state:
                st.session_state.working_df = pipeline.load_data()

            st.session_state.run_manual_std = False

            all_columns = st.session_state.working_df.columns.tolist()
            selected_columns = st.multiselect(
                "Select columns to standardize",
                all_columns,
                default=None
            )
            
            user_prompt = st.text_input("Optional AI instructions")
                       
            if st.button("Run Manual Standardization", key="manual_std_btn"):

                st.session_state.run_manual_std = True

                if selected_columns:
                    try:
                        data_standardizer = DataStandardizerAI(use_ai=True)
                        if user_prompt.strip():
                            data_standardizer.ai_advisor.custom_prompt = user_prompt
                        
                        with st.spinner("AI is analyzing and standardizing selected columns..."):
                            standardized_df = data_standardizer.auto_standardize_dataframe(st.session_state.working_df, selected_columns)
                            st.session_state.working_df = standardized_df
                            st.success("✅ Manual standardization complete!")
                            st.dataframe(st.session_state.working_df.head())
                            
                            # Mark as completed
                            if 'standardization_substep_completed' not in st.session_state:
                                st.session_state.standardization_substep_completed = {}
                            st.session_state.standardization_substep_completed["Manual Column Selection"] = True
                            st.success("✅ Manual Column Selection applied successfully!")

                            pipeline.save_data(st.session_state.working_df, f'manual_column_data_standz', agent_used='manual_column_data_standz_agent')
                            st.dataframe(pipeline.load_data().head())
                            st.session_state.df = pipeline.load_data()
                            st.session_state.working_df = st.session_state.df
                            st.success("✅ Data Standardization operations completed!")
                            st.dataframe(st.session_state.df.head())

                            # Download button for standardized data
                            #st.session_state.df = pipeline.load_data()
                            if st.session_state.df is not None:
                                csv_bytes = st.session_state.df.to_csv(index=False).encode("utf-8")
                                st.download_button(
                                    "Download Manual Column Standardized Without AI CSV",
                                    data=csv_bytes,
                                    file_name="manual_column_standardized_without_ai_data.csv",
                                    mime="text/csv"
                                )



                    except Exception as e:
                        st.error(f"Manual standardization failed: {str(e)}")

                else:
                    st.warning("Please select at least one column")


        @st.fragment
        def render_feature_engineering_step():

            #st.dataframe(st.session_state.working_df.head())
            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True)
            st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)

            if 'working_df' not in st.session_state:
                st.session_state.working_df = pipeline.load_data()

            st.session_state.df = st.session_state.working_df 
            st.dataframe(st.session_state.df.head())

            # Initialize feature engineering sub-steps if not present
            if 'fe_sub_steps' not in st.session_state:
                st.session_state.fe_sub_steps = [
                    "Add Headers", "Remove Duplicates", "Assign Data Types", 
                    "Create New Feature", "Rename Features", "View Interval Records", 
                    "Subset Dataframe", "Delete Records", "Delete Columns", "Edit Values", 
                    "Feature Engineering with LLM"
                ]
            
            if 'fe_checkbox_states' not in st.session_state:
                st.session_state.fe_checkbox_states = {sub_step: False for sub_step in st.session_state.fe_sub_steps}
            
            if 'fe_active_order' not in st.session_state:
                st.session_state.fe_active_order = []

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True)
            st.markdown("---")
            st.markdown("### Feature Engineering Summary Statistics")

            pipeline_stats_agent = FeaturesStatisticsAgent()
            pipeline_overall_dataframe_summary, pipeline_group_column = pipeline_stats_agent.single_dataframe_summary(st.session_state.working_df)
            with st.expander("Feature Level Statistics Summary", expanded=False):
                pipeline_stats_agent.display_summary_in_columns(st.session_state.df)

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True)
            st.markdown("---")
            st.markdown("#### Feature Engineering Sub-Steps")
            st.markdown("Select the feature engineering operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.fe_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.fe_checkbox_states.get(sub_step, False),
                        key=f"fe_{sub_step}"
                    )
                    st.session_state.fe_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.fe_active_order:
                        st.session_state.fe_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.fe_active_order:
                        st.session_state.fe_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.fe_active_order:
                st.markdown("##### Applying Selected Feature Engineering Operations")
                
                # Initialize sub-step completion tracking
                if 'fe_substep_completed' not in st.session_state:
                    st.session_state.fe_substep_completed = {sub_step: False for sub_step in st.session_state.fe_sub_steps}
                
                # Initialize headers for each sub-step that requires it
                sub_steps_requiring_headers = ["Add Headers", "Remove Duplicates", "Assign Data Types", 
                    "Create New Feature", "Rename Features", "View Interval Records", 
                    "Subset Dataframe", "Delete Records", "Delete Columns", "Edit Values", "Feature Engineering with LLM"]
                for s_step_rh in sub_steps_requiring_headers:
                    key = f'fe_headers_{s_step_rh}'
                    if key not in st.session_state:
                        st.session_state[key] = ""
                
                # Initialize agents
                data_agent = DataReadAgent()
                fea_eng_agt = FeaturesEngineeringAgent()
                
                # Initialize working_df in session state if not present
                if 'fe_working_df' not in st.session_state or st.session_state.get('fe_reset_working_df', False):
                    st.session_state.fe_working_df = st.session_state.df.copy()
                    st.session_state.fe_reset_working_df = False
                
                if 'working_df' not in st.session_state:
                    st.session_state.working_df = pipeline.load_data()

                # Process each selected sub-step
                for sub_step in st.session_state.fe_active_order:
                    st.markdown(f" ##### {sub_step}:")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Add Headers":
                        render_fe_add_headers_substep(data_agent)
                    elif sub_step == "Remove Duplicates":
                        render_fe_remove_duplicates_substep()
                    elif sub_step == "Assign Data Types":
                        render_fe_assign_data_types_substep(data_agent)
                    elif sub_step == "Create New Feature":
                        render_fe_create_new_feature_substep(fea_eng_agt)
                    elif sub_step == "Rename Features":
                        render_fe_rename_features_substep()
                    elif sub_step == "View Interval Records":
                        render_fe_view_interval_records_substep()
                    elif sub_step == "Subset Dataframe":
                        render_fe_subset_dataframe_substep()
                    elif sub_step == "Delete Records":
                        render_fe_delete_records_substep()
                    elif sub_step == "Delete Columns":
                        render_fe_delete_columns_substep()
                    elif sub_step == "Edit Values":
                        render_fe_edit_values_substep()
                    elif sub_step == "Feature Engineering with LLM":
                        render_fe_with_llm_substep()
                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.df = pipeline.load_data()
                st.success("✅ Feature Engineering operations completed!")
            else:
                st.info("Select at least one feature engineering sub-step to apply.")



        @st.fragment
        def render_fe_add_headers_substep(data_agent):
            if f'fe_headers_Add Headers' not in st.session_state:
                st.session_state[f'fe_headers_Add Headers'] = ""
            
            headers_input = st.text_input(
                "Enter headers for Add Headers (comma-separated):",
                value=st.session_state[f'fe_headers_Add Headers'],
                key="headers_input_Add Headers"
            )
            st.session_state[f'fe_headers_Add Headers'] = headers_input

            if 'working_df' not in st.session_state:
                st.session_state.working_df = pipeline.load_data()

            if st.button("Apply Add Headers"):
                if headers_input:
                    headers = [h.strip() for h in headers_input.split(",")]
                    st.session_state.df = data_agent.add_headers_to_df(st.session_state.df, headers)
                    pipeline.save_data(st.session_state.df, f'add_headers', agent_used='add_headers_agent')
                    st.write(f"Headers: {headers} applied successfully:")
                    st.session_state.working_df = st.session_state.df
                    st.dataframe(st.session_state.pipeline.load_data().head())

                else:
                    st.warning("Please enter headers first")

                pipeline.save_data(st.session_state.df, f'add_headers', agent_used='add_headers_agent')
                st.dataframe(pipeline.load_data().head())
                st.session_state.df = pipeline.load_data()
                st.session_state.working_df = st.session_state.df
                st.success("✅ Add Headers operations completed!")
                st.dataframe(st.session_state.df.head())


        @st.fragment
        def render_fe_remove_duplicates_substep():
            num_dup = st.session_state.working_df.duplicated().sum()
            
            if num_dup > 0:
                st.info(f"Found {num_dup} duplicate rows")
            else:
                st.info("No duplicate rows found")
            
            if st.button(f"Apply Remove Duplicates", key="apply_fe_dedup_Remove Duplicates", type="primary"):
                if num_dup > 0:
                    st.session_state.working_df = st.session_state.working_df.drop_duplicates()
                    st.success(f"Removed {num_dup} duplicate rows")
                else:
                    st.info("No duplicates to remove - step completed")

                pipeline.save_data(st.session_state.working_df, f'remove_duplicates', agent_used='remove_duplicates_agent')
                st.success(f"✅ Remove Duplicates applied successfully!")
                st.dataframe(pipeline.load_data().head())
                st.session_state.df = st.session_state.working_df
                st.session_state.fe_substep_completed["Remove Duplicates"] = True
                st.rerun()


        @st.fragment
        def render_fe_assign_data_types_substep(data_agent):
            import math
            st.session_state.df = pipeline.load_data()
            column_names = st.session_state.df.columns.tolist()
            column_dtypes = {}
            data_types = [
                'float64', 'float32', 'int32', 'int64', 'uint8', 'object', 'string', 
                'boolean', 'bool', 'datetime64[ns]', 'timedelta64[ns]', 'category', 'period[M]'
            ]

            if "max_rows_per_col" not in st.session_state:
                st.session_state["max_rows_per_col"] = 5

            col_label, col_select, spacer = st.columns([1.5, 1, 6])

            with col_label:
                st.markdown("**Max rows per column:**")

            with col_select:
                max_rows_options = list(range(1, 11))
                max_rows_per_col = st.selectbox(
                    label="max_rows_label",
                    options=max_rows_options,
                    index=max_rows_options.index(st.session_state["max_rows_per_col"]),
                    key="max_rows_per_col_selectbox",
                    label_visibility="collapsed"
                )
                st.session_state["max_rows_per_col"] = max_rows_per_col

            st.markdown("""
            <style>
                div[data-testid="column"] {
                    min-width: 0 !important;
                    max-width: 220px !important;
                    flex: unset !important;
                }
                .stSelectbox label {
                    display: none !important;
                }
                .stSelectbox, .stTextInput {
                    margin-bottom: 0 !important;
                    padding-top: 0 !important;
                    padding-bottom: 0 !important;
                }
            </style>
            """, unsafe_allow_html=True)

            def chunks(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i + n]

            num_cols = math.ceil(len(column_names) / max_rows_per_col)
            column_chunks = list(chunks(column_names, max_rows_per_col))
            cols = st.columns(num_cols, gap="large")

            for col_idx, chunk in enumerate(column_chunks):
                with cols[col_idx]:
                    for col in chunk:
                        st.markdown(f"<b>{col}</b>", unsafe_allow_html=True)
                        curr_dtype = str(st.session_state.df[col].dtype)
                        try:
                            idx = data_types.index(curr_dtype)
                        except ValueError:
                            idx = 0
                        column_dtypes[col] = st.selectbox(
                            label="",
                            options=data_types,
                            index=idx,
                            key=f"dtype_select_{col}"
                        )

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True)

            if st.button("Apply Data Types"):
                st.session_state.df = data_agent.assign_features_dtypes(
                    st.session_state.df, column_dtypes
                )
                pipeline.save_data(
                    st.session_state.df,
                    f'assign_data_types',
                    agent_used='assign_data_types_agent'
                )
                st.session_state.pipeline = pipeline

                st.write("Data types applied successfully:")
                st.dataframe(st.session_state.pipeline.load_data().head())

                parquet_df = st.session_state.df
                parquet_pyarrow_df = pipeline.load_data()
                common_cols = [c for c in parquet_df.columns if c in parquet_pyarrow_df.columns]

                comparison_data = []

                for col_name in common_cols:
                    parquet_dtype = parquet_df.dtypes[col_name]
                    parquet_pyarrow = parquet_pyarrow_df.dtypes[col_name]

                    comparison_data.append({
                        "Feature": col_name,
                        "Parquet Pyarrow Dtype": str(parquet_pyarrow),
                        "Parquet Dtype": str(parquet_dtype)
                    })

                st.table(pd.DataFrame(comparison_data))


        @st.fragment
        def render_fe_create_new_feature_substep(fea_eng_agt):
            pipeline.save_data(
                st.session_state.df,
                f'create_new_feature',
                agent_used='create_new_feature_agent'
            )
            st.session_state.pipeline = pipeline

            if f'fe_feature_name_Create New Feature' not in st.session_state:
                st.session_state[f'fe_feature_name_Create New Feature'] = ""
            if f'fe_feature_expr_Create New Feature' not in st.session_state:
                st.session_state[f'fe_feature_expr_Create New Feature'] = ""
            
            col1, col2 = st.columns(2)
            with col1:
                feature_name = st.text_input(
                    "Feature name for Create New Feature:",
                    value=st.session_state[f'fe_feature_name_Create New Feature'],
                    key="feature_name_Create New Feature"
                )
            with col2:
                feature_expr = st.text_input(
                    "Expression for Create New Feature:",
                    value=st.session_state[f'fe_feature_expr_Create New Feature'],
                    key="feature_expr_Create New Feature"
                )
            
            st.session_state[f'fe_feature_name_Create New Feature'] = feature_name
            st.session_state[f'fe_feature_expr_Create New Feature'] = feature_expr
            
            with st.expander("📚 Feature Expression Help", expanded=False):
                st.markdown("### Mathematical Operations")
                st.markdown("""
                **Basic Math:**
                - `+` Addition: `column1 + column2`
                - `-` Subtraction: `column1 - column2`
                - `*` Multiplication: `column1 * column2`
                - `/` Division: `column1 / column2`
                - `**` Exponentiation: `column1 ** 2`
                - `%` Modulo: `column1 % 10`
                
                **Examples:**
                - `price * quantity` - Calculate total cost
                - `age + 5` - Add 5 years to age
                - `salary / 12` - Monthly salary from annual
                - `temperature * 9/5 + 32` - Convert Celsius to Fahrenheit
                - `distance / time` - Calculate speed
                
                **Column References:**
                - Use exact column names from your dataset
                - Examples: `Age`, `Price`, `Quantity`, `Temperature`
                
                **Functions:**
                - `abs(column)` - Absolute value
                - `sqrt(column)` - Square root
                - `log(column)` - Natural logarithm
                - `exp(column)` - Exponential
                - `sin(column)`, `cos(column)`, `tan(column)` - Trigonometric functions
                
                **Conditional Expressions:**
                - `where(condition, value_if_true, value_if_false)`
                - Example: `where(age > 18, 'Adult', 'Minor')`
                """)
                
                st.markdown("### Available Columns in Your Dataset:")
                working_df = st.session_state.df.copy()
                cols_per_row = 4
                for i in range(0, len(working_df.columns), cols_per_row):
                    cols = st.columns(cols_per_row)
                    for j in range(cols_per_row):
                        if i + j < len(working_df.columns):
                            col = working_df.columns[i + j]
                            with cols[j]:
                                st.markdown(f"- `{col}`")
            
            if st.button(f"Create Create New Feature", key="create_Create New Feature"):
                if feature_name and feature_expr:
                    try:
                        new_feature_series = fea_eng_agt.evaluate_feature_expr(
                            feature_expr,
                            working_df
                        )
                        working_df[feature_name] = new_feature_series
                        st.session_state.df = working_df

                        pipeline.save_data(
                            st.session_state.df,
                            f'create_new_feature',
                            agent_used='create_new_feature_agent'
                        )

                        st.success(f"✅ Feature '{feature_name}' created successfully!")
                        st.info(f"Expression: `{feature_expr}`")
                        st.markdown("### 🆕 New Feature Created:")
                        st.dataframe(working_df[[feature_name]].head())
                        st.markdown("### 📊 Updated DataFrame (with new feature):")
                        st.dataframe(st.session_state.df.head())
                        st.info(f"Total columns now: {len(st.session_state.df.columns)} (was {len(st.session_state.df.columns)-1})")
                        
                        st.session_state.fe_substep_completed["Create New Feature"] = True
                        st.success(f"✅ Create New Feature applied successfully!")
                        
                        st.rerun()
                                                
                    except Exception as e:
                        st.error(f"❌ Error creating feature: {str(e)}")
                        st.info("💡 Tips:")
                        st.markdown("""
                        - Check that column names are spelled correctly
                        - Use exact column names from your dataset
                        - Ensure mathematical operations make sense for your data types
                        - Try a simpler expression first to test
                        """)
                else:
                    st.warning("Please enter both feature name and expression")


        @st.fragment
        def render_fe_rename_features_substep():
            if 'df' not in st.session_state or st.session_state.df is None:
                st.session_state.df = pipeline.load_data()
            
            if st.button("🔄 Force Reset This Step"):
                st.session_state.fe_substep_completed["Rename Features"] = False
                mapping_key = f"fe_rename_mapping_Rename Features"
                if mapping_key in st.session_state:
                    del st.session_state[mapping_key]
                st.session_state.df = pipeline.load_data()
                st.rerun()

            st.markdown("#### Rename Columns")
            
            mapping_key = f"fe_rename_mapping_Rename Features"
            if mapping_key not in st.session_state:
                st.session_state[mapping_key] = {}
            
            st.markdown("##### Current Data")
            st.dataframe(st.session_state.df.head())
            
            st.markdown("##### Current Columns")
            cols = st.multiselect(
                "Select columns to rename:",
                options=list(st.session_state.df.columns),
                key=f"cols_to_rename_Rename Features"
            )
            
            with st.container():
                for col in cols:
                    col1, col2, spacer = st.columns([1, 1, 2]) 
                    with col1:
                        st.markdown(f"<div style='margin-top: 5px;'>Current Name: <code>{col}</code></div>", unsafe_allow_html=True)
                    with col2:
                        new_name = st.text_input(
                            f"New name for {col}:", 
                            placeholder=f"Enter new name for {col}",
                            key=f"rename_Rename Features_{col}",
                            label_visibility="collapsed"
                        )
                        if new_name and new_name != col:
                            st.session_state[mapping_key][col] = new_name

            if st.session_state[mapping_key]:
                st.markdown("##### Mappings to apply:")
                for old, new in st.session_state[mapping_key].items():
                    st.write(f"`{old}` → `{new}`")

                if st.button("✅ Apply Renames", key="apply_fe_rename_features", type="primary"):
                    st.session_state.df = st.session_state.df.rename(
                        columns=st.session_state[mapping_key]
                    )
                    st.session_state.fe_substep_completed["Rename Features"] = True
                    
                    pipeline.save_data(st.session_state.df, f'rename_features', agent_used='rename_agent')
                    st.success(f"Renamed {len(st.session_state[mapping_key])} columns!")

                    st.dataframe(st.session_state.df.head())
                    st.rerun()

            else:
                st.info("Select columns and enter new names above")

            if st.session_state.fe_substep_completed.get("Rename Features", False):
                st.success("✅ Rename step completed!")
                st.session_state.df = pipeline.load_data()
                st.dataframe(st.session_state.df.head())
                st.write("**Current columns:**", list(st.session_state.df.columns))
                
                if st.button("Undo completion"):
                    st.session_state.fe_substep_completed["Rename Features"] = False


        @st.fragment
        def render_fe_view_interval_records_substep():
            st.info("Viewing interval records")
            st.write(st.session_state.working_df.describe())
            
            if st.button(f"Apply View Interval Records", key="apply_fe_interval_View Interval Records", type="primary"):
                st.success(f"✅ View Interval Records applied successfully!")
                st.session_state.fe_substep_completed["View Interval Records"] = True


        @st.fragment
        def render_fe_subset_dataframe_substep():
            if f'fe_subset_rows_Subset Dataframe' not in st.session_state:
                st.session_state[f'fe_subset_rows_Subset Dataframe'] = ""
            
            subset_rows = st.text_input(
                "Row subset condition for Subset Dataframe (e.g., 'column > 0'):",
                value=st.session_state[f'fe_subset_rows_Subset Dataframe'],
                key="subset_rows_Subset Dataframe"
            )
            st.session_state[f'fe_subset_rows_Subset Dataframe'] = subset_rows
            
            if st.button(f"Apply Subset Dataframe", key="subset_Subset Dataframe", type="primary"):
                if subset_rows:
                    try:
                        working_df = st.session_state.working_df.query(subset_rows)
                        st.success(f"Dataframe subsetted with condition: {subset_rows}")
                        st.session_state.df = working_df
                        pipeline.save_data(st.session_state.df, f'subset', agent_used='subset_agent')
                        st.dataframe(pipeline.load_data().head())
                        st.rerun()
                        
                        st.session_state.fe_substep_completed["Subset Dataframe"] = True
                        st.success(f"✅ Subset Dataframe applied successfully!")
                        
                    except Exception as e:
                        st.error(f"Error subsetting: {str(e)}")
                else:
                    st.warning("Please enter a subset condition first")


        @st.fragment
        def render_fe_delete_records_substep():
            st.session_state.df = pipeline.load_data()
            st.dataframe(st.session_state.df.head())

            indices_to_delete = st.text_input("Enter indices to delete (comma-separated):")

            if indices_to_delete:
                try:
                    indices = [int(idx.strip()) for idx in indices_to_delete.split(',')]

                    if st.button(f"Apply Delete Records", key="delete_Delete Records", type="primary"):               
                        st.session_state.df = st.session_state.df.drop(indices)
                        st.session_state.df.reset_index(drop=True, inplace=True)
                        
                        st.success(f"Deleted rows with indices: {indices} and reset the index.")
                        st.write("Updated DataFrame after deleting records:")
                        pipeline.save_data(st.session_state.df, f'delete_records', agent_used='delete_records_agent')
                        st.dataframe(st.session_state.df.head())
                        st.dataframe(pipeline.load_data().head())
                        st.rerun()
                except ValueError:
                    st.error("Please enter valid integer indices separated by commas.")
                except KeyError as e:
                    st.error(f"Index {e} not found in the DataFrame.")


        @st.fragment
        def render_fe_delete_columns_substep():
            if f'fe_delete_cols_Delete Columns' not in st.session_state:
                st.session_state[f'fe_delete_cols_Delete Columns'] = []
            
            cols_to_delete = st.multiselect(
                "Select columns to delete for Delete Columns:",
                options=st.session_state.working_df.columns.tolist(),
                default=st.session_state[f'fe_delete_cols_Delete Columns'],
                key="delete_cols_Delete Columns"
            )
            st.session_state[f'fe_delete_cols_Delete Columns'] = cols_to_delete
            
            if st.button(f"Delete {len(cols_to_delete)} columns", key="delete_cols_btn_Delete Columns"):
                if cols_to_delete:
                    working_df = st.session_state.working_df.drop(columns=cols_to_delete)
                    st.success(f"Deleted columns: {', '.join(cols_to_delete)}")
                    st.session_state.df = working_df
                    pipeline.save_data(st.session_state.df, f'delete_columns', agent_used='delete_agent')
                    st.dataframe(pipeline.load_data().head())
                    st.rerun()
                    
                    st.session_state.fe_substep_completed["Delete Columns"] = True
                    st.success(f"✅ Delete Columns applied successfully!")
                else:
                    st.warning("Please select columns to delete first")


        @st.fragment
        def render_fe_edit_values_substep():
            st.info("Value editing requires specific column and condition configuration")
            
            if st.button(f"Apply Edit Values", key="apply_fe_editval_Edit Values", type="primary"):
                st.success(f"✅ Edit Values applied successfully!")
                st.session_state.fe_substep_completed["Edit Values"] = True


        @st.fragment
        def render_fe_with_llm_substep():
            if "new_df" not in st.session_state:
                st.session_state.new_df = None
            if "old_df" not in st.session_state:
                st.session_state.old_df = None

            if "df" not in st.session_state:
                st.session_state.df = original_df

            if "transformed" not in st.session_state:
                st.session_state.transformed = False

            pwllm = PreprocessWithLLMAgent()
            output_capture = OutputCapture()
            st.session_state.new_df, original_df = pwllm.run(output_capture)

            if st.session_state.new_df is None:
                st.write("")
            else:
                for col in st.session_state.new_df.columns:
                    if st.session_state.new_df[col].dtype == 'object':
                        st.session_state.new_df[col] = st.session_state.new_df[col].astype(str)

            if original_df is None:
                st.write("")
            else:
                for col in original_df.columns:
                    if original_df[col].dtype == 'object':
                        original_df[col] = original_df[col].astype(str)

            if st.session_state.new_df is not None and not st.session_state.new_df.empty:
                st.session_state.df = st.session_state.new_df
                st.success("✅ Preprocessing with LLM completed successfully!")
            else:
                st.warning("⚠️ No new DataFrame was generated or it is empty.")

            col1, col2 = st.columns(2)

            with col1:
                def activate_transformation():
                    st.session_state.df = st.session_state.new_df
                    st.session_state.transformed = True

                st.button("Confirm Transformation", on_click=activate_transformation)

                if st.session_state.transformed:
                    st.success("✅ Transformed DataFrame Activated!")
                    st.dataframe(st.session_state.df)

            with col2:
                if st.button("Revert to Old DataFrame", use_container_width=True, key="reset_trnfm_df_btn"):
                    st.session_state.df = original_df
                    st.write(original_df.shape)


        @st.fragment
        def render_standardize_column_data_step():
            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            st.dataframe(st.session_state.df.head())

            # Initialize column standardization sub-steps if not present
            if 'column_standardization_sub_steps' not in st.session_state:
                st.session_state.column_standardization_sub_steps = [
                    "Standardize Single Column", "Standardize Multiple Columns",
                    "Missing Data Standardization"
                ]
            
            if 'column_standardization_checkbox_states' not in st.session_state:
                st.session_state.column_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.column_standardization_sub_steps}
            
            if 'column_standardization_active_order' not in st.session_state:
                st.session_state.column_standardization_active_order = []

            st.markdown("#### Column Standardization Sub-Steps")
            st.markdown("Select the column standardization operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.column_standardization_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.column_standardization_checkbox_states.get(sub_step, False),
                        key=f"column_standardization_{sub_step}"
                    )
                    st.session_state.column_standardization_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.column_standardization_active_order:
                        st.session_state.column_standardization_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.column_standardization_active_order:
                        st.session_state.column_standardization_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.column_standardization_active_order:
                st.markdown("##### Applying Selected Column Standardization Operations")
                
                # Create a copy of the dataframe to work with
                working_df = st.session_state.df.copy()
                
                col_standardizer = ColumnStandardizationAgent()
                
                for sub_step in st.session_state.column_standardization_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Standardize Single Column":
                        working_df = render_col_std_single_column_substep(col_standardizer, working_df)
                    elif sub_step == "Standardize Multiple Columns":
                        working_df = render_col_std_multiple_columns_substep(col_standardizer, working_df)
                    elif sub_step == "Missing Data Standardization":
                        working_df = render_col_std_missing_data_substep(col_standardizer, working_df)
                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.df = working_df
                st.success("✅ Column Standardization operations completed!")
                st.dataframe(st.session_state.df.head())
            else:
                st.info("Select at least one column standardization sub-step to apply.")


        @st.fragment
        def render_col_std_single_column_substep(col_standardizer, working_df):
            columns = working_df.columns.tolist()
            st.markdown("###### Select column to standardize")
            column = st.selectbox("", columns, label_visibility="collapsed", key=f"single_col_Standardize Single Column")
            st.markdown("###### Select reference column")
            reference_column = st.selectbox("Select reference column", columns, key=f"single_ref_Standardize Single Column")
            prompt_template = st.text_input("Prompt template", value="Provide a standardized value for the {term}", key=f"single_prompt_Standardize Single Column")
            
            if st.button("Standardize Single Column", key=f"single_std_Standardize Single Column"):
                try:
                    standardized_series = col_standardizer.standardize_column(working_df, column, reference_column, prompt_template)
                    new_col_name = f"{column}_standardized"
                    working_df[new_col_name] = standardized_series
                    st.success(f"Standardized column '{column}'")
                    st.dataframe(working_df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")

                # Update session state with processed dataframe
                st.session_state.df = working_df
                st.success("✅ Standardize Single Column completed!")
                pipeline.save_data(st.session_state.df, f'standardize_single_column', agent_used='standardize_single_column_agent')
                st.dataframe(pipeline.load_data().head())
                pipeline.metadata['current_data'] = 'standardize_single_column'
            
            return working_df


        @st.fragment
        def render_col_std_multiple_columns_substep(col_standardizer, working_df):
            columns = working_df.columns.tolist()
            selected_columns = st.multiselect("Select columns to standardize", columns, key=f"multi_cols_Standardize Multiple Columns")
            reference_column = st.selectbox("Select reference column", columns, key=f"multi_ref_Standardize Multiple Columns")
            prompt_template = st.text_input("Prompt template", value="Provide a standardized value", key=f"multi_prompt_Standardize Multiple Columns")
            
            if st.button("Standardize Multiple Columns", key=f"multi_std_Standardize Multiple Columns"):
                if selected_columns:
                    try:
                        for column in selected_columns:
                            standardized_series = col_standardizer.standardize_column(working_df, column, reference_column, prompt_template)
                            new_col_name = f"{column}_standardized"
                            working_df[new_col_name] = standardized_series
                        st.success(f"Standardized {len(selected_columns)} columns")
                        st.dataframe(working_df.head())
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    st.warning("Please select at least one column")
            
            return working_df


        @st.fragment
        def render_col_std_missing_data_substep(col_standardizer, working_df):
            columns = working_df.columns.tolist()
            column = st.selectbox("Select column with missing data", columns, key=f"missing_col_Missing Data Standardization")
            reference_column = st.selectbox("Select reference column", columns, key=f"missing_ref_Missing Data Standardization")
            prompt_template = st.text_input("Prompt template", value="Provide a standardized value", key=f"missing_prompt_Missing Data Standardization")
            
            if st.button("Standardize Missing Data", key=f"missing_std_Missing Data Standardization"):
                try:
                    standardized_series = col_standardizer.standardize_missing_data(working_df, column, reference_column, prompt_template)
                    new_col_name = f"{column}_standardized"
                    working_df[new_col_name] = standardized_series
                    st.success(f"Standardized missing data in column '{column}'")
                    st.dataframe(working_df.head())
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            
            return working_df



        @st.fragment
        def render_inflection_ai_standard_feature_name_step():
            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            # Load the DataFrame from the pipeline            
            st.dataframe(pipeline.load_data().head())
            st.session_state.df = pipeline.load_data()

            # Initialize inflection standardization sub-steps if not present
            if 'inflection_standardization_sub_steps' not in st.session_state:
                st.session_state.inflection_standardization_sub_steps = [
                    "Underscore Conversion", "Camelize Conversion", 
                    "Dasherize Conversion", "Humanize Conversion",
                    "Titleize Conversion", "Pluralize/Singularize",
                    "Anomaly Visualization"
                ]
            
            if 'inflection_standardization_checkbox_states' not in st.session_state:
                st.session_state.inflection_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.inflection_standardization_sub_steps}
            
            if 'inflection_standardization_active_order' not in st.session_state:
                st.session_state.inflection_standardization_active_order = []

            st.markdown("#### Inflection AI Standard Feature Name Sub-Steps")
            st.markdown("Select the inflection standardization operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.inflection_standardization_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.inflection_standardization_checkbox_states.get(sub_step, False),
                        key=f"inflection_standardization_{sub_step}"
                    )
                    st.session_state.inflection_standardization_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.inflection_standardization_active_order:
                        st.session_state.inflection_standardization_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.inflection_standardization_active_order:
                        st.session_state.inflection_standardization_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.inflection_standardization_active_order:
                st.markdown("###### Applying Selected Inflection Standardization Operations")
                
                # Create a copy of the dataframe to work with
                working_df = st.session_state.df.copy()
                
                human_standard_agent = HumanDataStandardizationAgent()
                
                for sub_step in st.session_state.inflection_standardization_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Underscore Conversion":
                        working_df = render_inflection_underscore_substep(working_df)
                    elif sub_step == "Camelize Conversion":
                        working_df = render_inflection_camelize_substep(working_df)
                    elif sub_step == "Dasherize Conversion":
                        working_df = render_inflection_dasherize_substep(working_df)
                    elif sub_step == "Humanize Conversion":
                        working_df = render_inflection_humanize_substep(working_df)
                    elif sub_step == "Titleize Conversion":
                        working_df = render_inflection_titleize_substep(working_df)
                    elif sub_step == "Pluralize/Singularize":
                        working_df = render_inflection_plural_singularize_substep(working_df)
                    elif sub_step == "Anomaly Visualization":
                        working_df = render_inflection_anomaly_visualization_substep(working_df)
                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.df = working_df
                st.success("✅ Inflection Standardization operations completed!")
                st.dataframe(st.session_state.df.head())
            else:
                st.info("Select at least one inflection standardization sub-step to apply.")

            # Keep the original AI standardization functionality
            render_inflection_ai_standardization_section()


        @st.fragment
        def render_inflection_underscore_substep(working_df):
            # Apply underscore conversion to all columns
            working_df.columns = [inflection.underscore(col) for col in working_df.columns]
            st.session_state.df = working_df 
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'inflection_underscore_conversion', agent_used='inflection_underscore_conversion_agent')
            st.success("Applied underscore conversion to all column names")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_camelize_substep(working_df):
            # Apply camelize conversion to all columns
            working_df.columns = [inflection.camelize(col) for col in working_df.columns]
            st.success("Applied camelize conversion to all column names")
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'inflection_camelize_conversion', agent_used='inflection_camelize_conversion_agent')
            st.success("Camelize Conversion Completed")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_dasherize_substep(working_df):
            # Apply dasherize conversion to all columns
            working_df.columns = [inflection.dasherize(col) for col in working_df.columns]
            st.success("Applied dasherize conversion to all column names")
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'inflection_dasherize_conversion', agent_used='inflection_dasherize_conversion_agent')
            st.success("Dasherize Conversion Completed")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_humanize_substep(working_df):
            # Apply humanize conversion to all columns
            working_df.columns = [inflection.humanize(col) for col in working_df.columns]
            st.success("Applied humanize conversion to all column names")
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'inflection_humanize_conversion', agent_used='inflection_humanize_conversion_agent')
            st.success("Humanize Conversion Completed")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_titleize_substep(working_df):
            # Apply titleize conversion to all columns
            working_df.columns = [inflection.titleize(col) for col in working_df.columns]
            st.success("Applied titleize conversion to all column names")
            st.dataframe(working_df.head())
            pipeline.save_data(st.session_state.df, f'inflection_titleize_conversion', agent_used='inflection_titleize_conversion_agent')
            st.success("Titleize Conversion Completed")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_plural_singularize_substep(working_df):
            # Apply pluralize/singularize conversion to all columns
            working_df.columns = [inflection.singularize(col) for col in working_df.columns]
            pipeline.save_data(st.session_state.df, f'inflection_plural_singularize_conversion', agent_used='inflection_plural_singularize_conversion_agent')
            st.success("Pluralize/Singularize Conversion Completed")
            st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_anomaly_visualization_substep(working_df):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                wrapper_dataanomalyagent = WrapperDataAnomalyAgent()
                features = st.multiselect("Select features for anomaly visualization", options=df.columns.tolist())
                models = st.multiselect("Select models for anomaly visualization", options=wrapper_dataanomalyagent.wrapper_model_dict().keys())
                wrapper_dataanomalyagent.show_anomaly_plot(df, features, models)
                st.session_state.df = working_df
                pipeline.save_data(st.session_state.df, f'inflection_anomaly_visualization', agent_used='inflection_anomaly_visualization_agent')
                st.success("Anomaly Visualization Completed")
                st.dataframe(st.session_state.df.head())
            return working_df


        @st.fragment
        def render_inflection_ai_standardization_section():
            if 'df' in st.session_state and st.session_state.df is not None:                         
                st.write("---")

                # Initialize the agent first
                human_standard_agent = HumanDataStandardizationAgent()

                # --- Interactivity inside main ---
                inflection_method = st.selectbox(
                    "Choose an inflection method:",
                    ['underscore', 'camelize', 'dasherize', 'humanize', 'titleize', 'pluralize', 'singularize', 'parameterize']
                )

                prompt_template = st.text_input(
                    "Enter the AI prompt (use `{term}` as a placeholder). Edit prompt and hit enter to rerun:",
                    "Provide the inflection standardized alternative underscore of the {term}, no repetition of terms error, no signs, no symbols and only one term or compound term should be returned.",
                    placeholder="Provide the inflection standardized alternative underscore of the {term}, no repetition of terms error, no signs, no symbols and only one term or compound term should be returned."
                )

                if prompt_template:
                    df_columns = list(st.session_state.df.columns)
                    inflection_standardized = [getattr(inflection, inflection_method)(col) for col in df_columns]

                    # Store in session state
                    if "ai_standardized" not in st.session_state:
                        st.session_state.ai_standardized = human_standard_agent.get_openai_corrections(df_columns, set(), prompt_template)
                    
                    # Regenerate button with callback
                    regenerate_clicked = st.button("Regenerate AI Suggestions")
                    
                    if regenerate_clicked:
                        # Clear cache and regenerate
                        st.session_state.ai_standardized = human_standard_agent.get_openai_corrections(df_columns, set(), prompt_template)
                        st.success("AI suggestions regenerated!")
                    
                    # Always use current value from session state
                    ai_standardized = st.session_state.ai_standardized

                    st.write("---")

                    standardized_data = []
                    for orig, infl in zip(df_columns, inflection_standardized):
                        ai_name = ai_standardized.get(orig, infl)
                        standardized_data.append({
                            'original_feature_name': orig,
                            'inflection_standardized_feature': infl,
                            'ai_standardized_feature': ai_name
                        })
                    
                    # Display current results
                    st.write("### Standardization Results")
                    st.dataframe(pd.DataFrame(standardized_data))

                    st.session_state.standard_names_df = pd.DataFrame(standardized_data)
                    st.write("##### AI and Inflection Output")
                    st.dataframe(st.session_state.standard_names_df)
                    
                    st.write("---")
                
                    # --- Editable table for corrections ---
                    st.write("##### Manually Edit AI Standardized Feature Names")

                    # Init session state
                    if 'standard_names_df' not in st.session_state:
                        st.session_state.standard_names_df = pd.DataFrame()

                    if "corrections_applied" not in st.session_state:
                        st.session_state.corrections_applied = False

                    if "dataset_applied" not in st.session_state:
                        st.session_state.dataset_applied = False

                    # Editable table
                    editable_df = st.data_editor(
                        st.session_state.standard_names_df[
                            ['original_feature_name', 'inflection_standardized_feature', 'ai_standardized_feature']
                        ],
                        use_container_width=True,
                        key=f"Inflection AI Standard Feature Name_None_editable_ai_features"
                    )

                    # Apply manual corrections
                    if st.button("Apply Manual Corrections", key="Inflection AI Standard Feature Name_None_appmancorr"):
                        st.session_state.standard_names_df['ai_standardized_feature'] = editable_df['ai_standardized_feature']
                        st.session_state.corrections_applied = True
                        st.success("Manual corrections applied!")

                    # Run correction pipeline ONLY after button click
                    if st.session_state.corrections_applied:
                        st.session_state.standard_names_df = crewailpydeamlpipeline._st_human_inflxn_ai_correction(
                            st.session_state.standard_names_df
                        )
                        st.session_state.corrections_applied = False  # prevent rerun loop

                    st.write("---")
                    st.write("##### Apply Standard Names to Your Dataset")

                    standard = st.radio(
                        "Select which standard to apply to the original dataset:",
                        ['inflection_standardized_feature', 'ai_standardized_feature'],
                        key="Inflection AI Standard Feature Name_None_standard_radio"
                    )

                    # Apply renaming ONLY when button clicked
                    if st.button("Apply to Dataset", key="Inflection AI Standard Feature Name_None_apply_dataset"):

                        feature_map = (
                            st.session_state.standard_names_df
                            .set_index('original_feature_name')[standard]
                            .to_dict()
                        )

                        updated_df = st.session_state.df.rename(columns=feature_map)
                        st.session_state.df = updated_df
                        st.session_state.dataset_applied = True

                        pipeline.save_data(
                            st.session_state.df,
                            'inflection_standardized_output_for_download',
                            agent_used='inflection_standardized_output_for_download_agent'
                        )

                        st.success(f"Columns renamed using `{standard}` successfully!")

                    # Show results ONLY after dataset applied
                    if st.session_state.dataset_applied:
                        st.dataframe(st.session_state.df.head())

                        st.success("Inflection Standardized Output Ready For Download")

                        # Download final CSV
                        csv = st.session_state.df.to_csv(index=False).encode("utf-8")

                        st.download_button(
                            "Download Final CSV",
                            data=csv,
                            file_name="standardized_output.csv",
                            mime="text/csv",
                            key="Inflection AI Standard Feature Name_None_download"
                        )


        @st.fragment
        def render_human_ai_standard_feature_values_step():
            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True)
            st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)
            st.dataframe(st.session_state.df.head())

            # Initialize human AI standardization sub-steps if not present
            if 'human_ai_standardization_sub_steps' not in st.session_state:
                st.session_state.human_ai_standardization_sub_steps = [
                    "Human-AI Standardization", "Manual Value Standardization"
                ]
            
            if 'human_ai_standardization_checkbox_states' not in st.session_state:
                st.session_state.human_ai_standardization_checkbox_states = {sub_step: False for sub_step in st.session_state.human_ai_standardization_sub_steps}
            
            if 'human_ai_standardization_active_order' not in st.session_state:
                st.session_state.human_ai_standardization_active_order = []

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True)
            st.markdown("---")

            st.markdown("#### Human AI Standard Feature Values Sub-Steps")
            st.markdown("Select the human AI standardization operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.human_ai_standardization_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.human_ai_standardization_checkbox_states.get(sub_step, False),
                        key=f"human_ai_standardization_{sub_step}"
                    )
                    st.session_state.human_ai_standardization_checkbox_states[sub_step] = checked
                    
                    # Update the active_order list based on checkbox state
                    if checked and sub_step not in st.session_state.human_ai_standardization_active_order:
                        st.session_state.human_ai_standardization_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.human_ai_standardization_active_order:
                        st.session_state.human_ai_standardization_active_order.remove(sub_step)
                        st.rerun()


            # Initialize sub-step completion tracking
            if 'human_ai_standardization_substep_completed' not in st.session_state:
                st.session_state.human_ai_standardization_substep_completed = {sub_step: False for sub_step in st.session_state.human_ai_standardization_sub_steps}
            


            # Mark sub-steps as active if they're in the order
            for sub_step in st.session_state.human_ai_standardization_active_order:
                st.session_state.human_ai_standardization_substep_completed[sub_step] = True


#############################

            # Apply selected sub-steps
            if st.session_state.human_ai_standardization_active_order:
                st.markdown("##### Applying Selected Human AI Standardization Operations")
                
                for sub_step in st.session_state.human_ai_standardization_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Human-AI Standardization":
                        render_human_ai_standardization_substep()
                    elif sub_step == "Manual Value Standardization":
                        render_manual_value_standardization_substep()
                    
                    st.markdown("---")


################################


        @st.fragment
        def render_human_ai_standardization_substep():
            # Call the streamlit method which will display the full UI
            crewailpydeamlpipeline.streamlit_human_ai_standardized_data(st.session_state.df)
            pipeline.save_data(st.session_state.df, f'human_ai_feature_value', agent_used='human_ai_feature_value_agent')
            #st.success("Human AI Standard Feature Values Successful")
            st.dataframe(st.session_state.df.head())


        @st.fragment
        def render_manual_value_standardization_substep():
            # Manual value standardization UI
            st.markdown("##### Manual Value Standardization")

            #with st.expander("👨‍🦱 Manual Value Standardization", expanded=False):

            # --- Step 2: Manual Adjustment ---
            st.write("###### Manual Value Standardization")

            df = st.session_state.df

            selected_column = st.selectbox("Select a column to standardize", df.columns, key="manual_correct_column_select")

            # Initialize manual standardization state
            if 'manual_standard_names_df' not in st.session_state:
                # Create initial DataFrame with unique values only (for easier editing)
                # Convert all values to strings for editing compatibility
                unique_values = st.session_state.df[selected_column].astype(str).drop_duplicates().reset_index(drop=True)
                st.session_state.manual_standard_names_df = pd.DataFrame({
                    'original_feature_name': unique_values,
                    'human_corrected_feature': unique_values.copy()  # Start identical to original
                })
                
                # Initialize manual accuracy tracking
                st.session_state.manual_accuracy_results = {}

            # Update the manual DataFrame if column changes
            if selected_column != st.session_state.get('manual_selected_column', None):
                unique_values = st.session_state.df[selected_column].astype(str).drop_duplicates().reset_index(drop=True)
                st.session_state.manual_standard_names_df = pd.DataFrame({
                    'original_feature_name': unique_values,
                    'human_corrected_feature': unique_values.copy()
                })
                st.session_state.manual_selected_column = selected_column

            # Show current manual standardization state
            if 'manual_standard_names_df' in st.session_state and not st.session_state.manual_standard_names_df.empty:
                
                st.write("##### Manual Standardization - Edit Unique Values")
                st.write("Edit the 'Human Corrected Feature' column below to standardize values. Changes affect **all occurrences** of that original value.")
                
                # Show count of unique values
                st.info(f"✏️ Editing {len(st.session_state.manual_standard_names_df)} unique value(s). Edit multiple values, then click Apply.")

                # Create editable table for manual corrections (unique values only)
                editable_manual_df = st.data_editor(
                    st.session_state.manual_standard_names_df,
                    use_container_width=True,
                    key="editable_manual_values",
                    num_rows="dynamic",
                    column_config={
                        "original_feature_name": st.column_config.TextColumn(
                            "Original Feature Name",
                            help="Original feature name (read-only)",
                            disabled=True
                        ),
                        "human_corrected_feature": st.column_config.TextColumn(
                            "Human Corrected Value",
                            help="Edit this value to replace ALL occurrences in the column"
                        )
                    }
                )

                # Update session state with edited values
                st.session_state.manual_standard_names_df = editable_manual_df

                # Calculate accuracy for manual corrections (Original vs Human only - no AI)
                def calculate_manual_accuracy(df, column_name):
                    total_values = len(df)
                    matching_values = (df['original_feature_name'] == df['human_corrected_feature']).sum()
                    accuracy = (matching_values / total_values) * 100 if total_values > 0 else 100
                    return accuracy

                manual_accuracy = calculate_manual_accuracy(st.session_state.manual_standard_names_df, selected_column)
                
                # Store accuracy result
                st.session_state.manual_accuracy_results[selected_column] = {
                    'Original_Accuracy': manual_accuracy,
                    'Human_Accuracy': 100.0  # Human is always 100% (reference)
                }

                # Display accuracy
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.write("##### Manual Standardization Accuracy")
                    accuracy_table = pd.DataFrame({
                        "Accuracy": [
                            f"{manual_accuracy:.2f}%",
                            "100.00%"
                        ]
                    }, index=["Original", "Human"])
                    st.table(accuracy_table)

                with col2:
                    # Plot accuracy (Original vs Human only)
                    fig, ax = plt.subplots(figsize=(5, 3))
                    ax.bar(
                        ['Original', 'Human'],
                        [manual_accuracy, 100.0],
                        color=['#FF9999', '#99FF99']
                    )
                    ax.set_ylim(0, 100)
                    ax.set_ylabel('Accuracy (%)', fontsize=8)
                    ax.set_title(f'Manual Standardization Accuracy for {selected_column}', fontsize=9)
                    ax.tick_params(axis='both', labelsize=7)
                    fig.tight_layout()
                    st.pyplot(fig, bbox_inches="tight")

                # Show unique values summary
                st.write("##### Unique Values Summary")
                unique_summary = st.session_state.manual_standard_names_df[['original_feature_name', 'human_corrected_feature']].drop_duplicates()
                st.dataframe(unique_summary)

                # Apply Manual Correction button
                if st.button("✅ Apply Manual Corrections to DataFrame", key="apply_manual_correction"):
                    # Apply the human corrections to the main DataFrame
                    # Create a mapping from original to corrected values
                    correction_map = dict(zip(
                        st.session_state.manual_standard_names_df['original_feature_name'],
                        st.session_state.manual_standard_names_df['human_corrected_feature']
                    ))
                    
                    # Apply mapping to the selected column in the main DataFrame
                    st.session_state.df[selected_column] = st.session_state.df[selected_column].map(correction_map).fillna(st.session_state.df[selected_column])
                    
                    # Update human_ai_data for consistency
                    st.session_state.human_ai_data[selected_column] = st.session_state.manual_standard_names_df['human_corrected_feature'].copy()
                    
                    st.success(f"✅ Manual corrections applied to column '{selected_column}'!")
                    
                    # Show updated DataFrame
                    st.write("##### Updated DataFrame")
                    st.dataframe(st.session_state.df)
                    
                    # Download button
                    csv = st.session_state.df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download DataFrame with Manual Corrections",
                        data=csv,
                        file_name=f"manual_corrected_{selected_column}.csv",
                        mime="text/csv"
                    )

            

            # # --- Step 3: Final Output and Download ---
            # if st.button("Finalize and Export"):
            #     if "human_ai_data" in st.session_state and not st.session_state.human_ai_data.empty:
            #         # Update the main DataFrame with human-corrected values
            #         for col in st.session_state.human_ai_data.columns:
            #             if col in st.session_state.df.columns:
            #                 st.session_state.df[col] = st.session_state.human_ai_data[col].copy()
                    
            #         st.success("✅ Main DataFrame updated with human-corrected values!")
                    
            #         final_df = st.session_state.human_ai_data.copy()
            #         st.success("Final human-AI standardized dataset is ready.")

            #         st.write("### Preview:")
            #         st.dataframe(final_df.head())

            #         st.write("### Final Output:")
            #         st.dataframe(st.session_state.human_ai_data)
                    
            #         st.write("### Updated Main DataFrame:")
            #         st.dataframe(st.session_state.df.head())

            #         # Download
            #         csv = final_df.to_csv(index=False).encode("utf-8")
            #         st.download_button("⬇️ Download as CSV", csv, "human_ai_data.csv", "text/csv")
            #     else:
            #         st.warning("No finalized data found. Please run standardization and corrections first.")


                # Plot accuracy
                                    
                
                crewailpydeamlpipeline.plot_accuracy_streamlit(
                    df=df,
                    cleaned_data=crewailpydeamlpipeline.cleaned_data,
                    human_ai_data=st.session_state.human_ai_data,
                    column_name=selected_column,
                    accuracy_results=crewailpydeamlpipeline.accuracy_results,
                    is_human_corrected=True
                )




            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available.")
                return
            
            df = st.session_state.df.copy()
            
            # Select column to standardize
            column = st.selectbox("Select column to standardize values:", df.columns.tolist())
            
            # Show unique values
            unique_values = df[column].unique()
            st.write(f"**Unique values in '{column}':**")
            st.write(unique_values[:20])  # Show first 20
            
            # Manual mapping
            st.markdown("##### Create Value Mappings")
            col1, col2 = st.columns(2)
            with col1:
                original_value = st.text_input("Original value to replace:")
            with col2:
                new_value = st.text_input("New standardized value:")
            
            if st.button("Add Mapping"):
                if original_value and new_value:
                    if original_value not in st.session_state.get('manual_value_mappings', {}):
                        st.session_state.manual_value_mappings = st.session_state.get('manual_value_mappings', {})
                        st.session_state.manual_value_mappings[original_value] = new_value
                        st.success(f"Added mapping: '{original_value}' → '{new_value}'")
            
            # Show current mappings
            if st.session_state.get('manual_value_mappings', {}):
                st.markdown("##### Current Mappings:")
                for orig, new in st.session_state.manual_value_mappings.items():
                    st.write(f"`{orig}` → `{new}`")
                
                if st.button("Apply All Mappings"):
                    for orig, new in st.session_state.manual_value_mappings.items():
                        df[column] = df[column].replace(orig, new)
                    st.session_state.df = df
                    pipeline.save_data(st.session_state.df, f'manual_value_standardization', agent_used='manual_value_standardization_agent')
                    st.success("Manual value standardization applied!")
                    st.dataframe(st.session_state.df.head())


        @st.fragment
        def render_data_anomaly_evaluation_step():

            if "anomaly_agent" in st.session_state:
                del st.session_state["anomaly_agent"]

            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            st.dataframe(st.session_state.df.head())

            # Initialize anomaly evaluation sub-steps if not present
            if 'anomaly_sub_steps' not in st.session_state:
                st.session_state.anomaly_sub_steps = [
                    "Anomaly Detection", "Anomaly Dictionary", 
                    "Model Anomaly Dictionary", "Batch Anomaly Fix",
                    "Human-AI Anomaly Review", "Anomaly Visualization"
                ]
            
            if 'anomaly_checkbox_states' not in st.session_state:
                st.session_state.anomaly_checkbox_states = {sub_step: False for sub_step in st.session_state.anomaly_sub_steps}
            
            if 'anomaly_active_order' not in st.session_state:
                st.session_state.anomaly_active_order = []

            st.markdown("#### Anomaly Evaluation Sub-Steps")
            st.markdown("Select the anomaly evaluation operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.anomaly_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.anomaly_checkbox_states.get(sub_step, False),
                        key=f"anomaly_{sub_step}"
                    )
                    st.session_state.anomaly_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.anomaly_active_order:
                        st.session_state.anomaly_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.anomaly_active_order:
                        st.session_state.anomaly_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.anomaly_active_order:
                st.markdown("##### Applying Selected Anomaly Evaluation Operations")
                
                # Create a copy of the dataframe to work with
                working_df = st.session_state.df.copy()
                
                wrapper_dataanomalyagent = WrapperDataAnomalyAgent()
                
                for sub_step in st.session_state.anomaly_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Anomaly Detection":
                        render_anomaly_detection_substep(wrapper_dataanomalyagent)
                    elif sub_step == "Anomaly Dictionary":
                        render_anomaly_dictionary_substep(wrapper_dataanomalyagent)
                    elif sub_step == "Model Anomaly Dictionary":
                        render_model_anomaly_dictionary_substep(wrapper_dataanomalyagent)
                    elif sub_step == "Batch Anomaly Fix":
                        working_df = render_batch_anomaly_fix_substep(wrapper_dataanomalyagent, working_df)
                    elif sub_step == "Human-AI Anomaly Review":
                        render_human_ai_anomaly_review_substep(wrapper_dataanomalyagent)
                    elif sub_step == "Anomaly Visualization":
                        render_anomaly_visualization_substep(wrapper_dataanomalyagent, working_df)
                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.df = working_df
                st.success("✅ Data Anomaly Evaluation operations completed!")
                #st.dataframe(st.session_state.df.head())
            else:
                st.info("Select at least one anomaly evaluation sub-step to apply.")


        @st.fragment
        def render_anomaly_detection_substep(wrapper_dataanomalyagent):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                st.write("### Anomaly Detection Output")
                #st.dataframe(df.head())
                
                if df is None:
                    st.warning("No dataframe available in session.")
                else:
                    st.dataframe(df.head())
                    features, models = wrapper_dataanomalyagent.choose_features_models(df, key_prefix="anomaly_detect")
                    
                    # Store previous selections under different keys to avoid clashing with widget keys
                    prev_features = st.session_state.get('anomaly_features_prev', [])
                    prev_models = st.session_state.get('anomaly_models_prev', [])

                    features_changed = sorted(prev_features) != sorted(features)
                    models_changed = sorted(prev_models) != sorted(models)


                if st.button("Select Parameters and Run Anomaly Detection", key="run_anomaly_detection_button"):
                
                    if ('anomaly_dict' not in st.session_state) or features_changed or models_changed:
                        new_result = wrapper_dataanomalyagent.anomaly_dictionary(df, features, models)
                        
                        st.session_state['anomaly_dict'] = new_result
                        
                        # Store previous used selections under different keys
                        st.session_state['anomaly_features_prev'] = features
                        st.session_state['anomaly_models_prev'] = models
                        
                        st.success("Anomaly dictionary calculated due to new or changed inputs.")
                    else:
                        st.info("Inputs unchanged — using cached anomaly dictionary.")
                
                if 'anomaly_dict' in st.session_state:
                    st.markdown(f"<h3 style='font-size:20px; color: white;'>Anomaly Dictionary</h3>", unsafe_allow_html=True)
                    st.dataframe(st.session_state['anomaly_dict'])
                
                st.markdown("<hr style='margin: 25px 0;'>", unsafe_allow_html=True)


        @st.fragment
        def render_anomaly_dictionary_substep(wrapper_dataanomalyagent):
            apply_key = f"apply_Anomaly Dictionary"
            
            if apply_key not in st.session_state:
                st.session_state[apply_key] = False
            
            df_for_anomaly = st.session_state.df.copy()

            for col in df_for_anomaly.columns:
                if df_for_anomaly[col].dtype == 'object':
                    try:
                        df_for_anomaly[col] = pd.to_numeric(df_for_anomaly[col])
                    except:
                        pass
            
            features, models = wrapper_dataanomalyagent.choose_features_models(
                df_for_anomaly, 
                key_prefix="anomaly_dict"
            )

            st.write(f"**Selected Features:** {features}")
            st.write(f"**Selected Models:** {models}")            
            
            # Button to run anomaly detection
            if st.button("Run Anomaly Detection Summary", key="run_anomaly_summary_button"):
                with st.spinner("Running anomaly detection across all models..."):
                    summary_df = wrapper_dataanomalyagent.create_anomaly_summary(
                        df_for_anomaly, features, models
                    )
                    
                    if summary_df is not None and not summary_df.empty:
                        st.success("✅ Anomaly detection summary complete!")
                        st.dataframe(summary_df)
                        
                        # Store in session state for later use
                        st.session_state['anomaly_summary'] = summary_df
                        
                    else:
                        st.info("No anomalies detected or error occurred.")
            
            # Display previous results if available
            with st.expander("Previous results if already detected and available:"):
                if 'anomaly_summary' in st.session_state:
                    st.markdown("---")
                    st.markdown("##### Previous Results")
                    st.dataframe(st.session_state['anomaly_summary'])

        @st.fragment
        def render_model_anomaly_dictionary_substep(wrapper_dataanomalyagent):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                features, models = wrapper_dataanomalyagent.choose_features_models(df, key_prefix="anomaly_model")
                
                st.session_state['anomaly_models_prev'] = models

                diff_model_results_df = wrapper_dataanomalyagent.run_detection_across_models(df, models if models else None)                               
                st.session_state['model_anomaly_dict'] = diff_model_results_df

            if 'model_anomaly_dict' in st.session_state:
                st.markdown(f"<h3 style='font-size:20px; color: white;'>Anomaly By Different Models Across The Data</h3>", unsafe_allow_html=True)
                st.dataframe(st.session_state['model_anomaly_dict'])
            
            st.markdown("<hr style='margin: 25px 0;'>", unsafe_allow_html=True)


        @st.fragment
        def render_batch_anomaly_fix_substep(wrapper_dataanomalyagent, working_df):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                features, models = wrapper_dataanomalyagent.choose_features_models(df, key_prefix="anomaly_fix")
                anomaly_fixed_df = wrapper_dataanomalyagent.fix_anomaly_flow(df, features, models)
                working_df = anomaly_fixed_df
                st.session_state.df = working_df
                st.dataframe(working_df.head())
            
            st.markdown("<hr style='margin: 25px 0;'>", unsafe_allow_html=True)
            return working_df


        @st.fragment
        def render_human_ai_anomaly_review_substep(wrapper_dataanomalyagent):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                features, models = wrapper_dataanomalyagent.choose_features_models(df, key_prefix="anomaly_review")
                default_models = ["mean"]
                wrapper_dataanomalyagent.human_ai_anomaly_workflow(df, features, default_models)
            
            st.markdown("<hr style='margin: 25px 0;'>", unsafe_allow_html=True)


        @st.fragment
        def render_anomaly_visualization_substep(wrapper_dataanomalyagent, working_df):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = st.session_state.df
                #wrapper_dataanomalyagent.plot_anomaly_interactive(df)
                features, models = wrapper_dataanomalyagent.choose_features_models(df, key_prefix="anomaly_visualization")
                default_models = ["mean"]
                wrapper_dataanomalyagent.show_anomaly_plot(df, features, models)
                st.session_state.df = working_df

                pipeline.save_data(st.session_state.df, f'anomaly_visualization', agent_used='anomaly_visualization_agent')
                st.success("✅ Anomaly Evaluation operations completed!")
                st.dataframe(st.session_state.df.head())
            st.markdown("<hr style='margin: 25px 0;'>", unsafe_allow_html=True)


##################################################################################

        @st.fragment
        def render_time_series_evaluation_step():

            # Safety check
            if 'timeseries_df' not in st.session_state or st.session_state.timeseries_df is None:
                st.session_state.timeseries_df = st.session_state.df.copy()

            st.dataframe(st.session_state.timeseries_df.head())

            # Initialize session state
            if 'timeseries_sub_steps' not in st.session_state:
                st.session_state.timeseries_sub_steps = [
                    "Time Series Data Review", 
                    "Time Series Analysis",
                    "Time Series Change Point Detection", 
                    "Time Series Protocol Change Analysis", 
                    "Time Series Decomposition",
                    "Time Series Stationarity", 
                    "Time Series Forecasting"]

            if 'timeseries_checkbox_states' not in st.session_state:
                st.session_state.timeseries_checkbox_states = {
                    sub_step: False for sub_step in st.session_state.timeseries_sub_steps}

            if 'timeseries_active_order' not in st.session_state:
                st.session_state.timeseries_active_order = []

            st.markdown("#### Time Series Evaluation Sub-Steps")
            st.markdown("Select the time series evaluation operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.timeseries_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.timeseries_checkbox_states.get(sub_step, False),
                        key=f"timeseries_{sub_step}"
                    )
                    st.session_state.timeseries_checkbox_states[sub_step] = checked

                    if checked:
                        st.session_state.statuses[sub_step] = 'active'  
                        st.session_state.substep = 'is_substep'
                    else:
                        st.session_state.statuses[sub_step] = 'idle'

                    #if is_substep

                    if checked and sub_step not in st.session_state.timeseries_active_order:
                        st.session_state.timeseries_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.timeseries_active_order:
                        st.session_state.timeseries_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.timeseries_active_order:
                st.markdown("##### Applying Selected Time Series Evaluation Operations")

                # Only reset timeseries_df if it doesn't already have a DatetimeIndex
                # This preserves the index set in render_data_preview()
                if 'timeseries_df' not in st.session_state or st.session_state.timeseries_df is None or not isinstance(st.session_state.timeseries_df.index, pd.DatetimeIndex):
                    st.session_state.timeseries_df = st.session_state.df.copy()

                # Create a copy of the dataframe to work with
                timeseries_working_df = st.session_state.timeseries_df.copy()
                
                
                for sub_step in st.session_state.timeseries_active_order:
                    st.markdown(f"**{sub_step}:**")

                    # Call the appropriate sub-step fragment
                    if sub_step == "Time Series Data Review":
                        render_data_preview()
                    elif sub_step == "Time Series Analysis":
                        render_analysis_config()
                    elif sub_step == "Time Series Change Point Detection":
                        render_change_point_detection()
                    elif sub_step == "Time Series Protocol Change Analysis":
                        render_protocol_audit()
                    elif sub_step == "Time Series Decomposition":
                        render_decomposition_results()
                    elif sub_step == "Time Series Stationarity":
                        render_stationarity_results()
                    elif sub_step == "Time Series Forecasting":
                        render_forecasting_section()

                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.timeseries_df = timeseries_working_df
                st.success("✅ Time Series Evaluation operations completed!")
                #st.dataframe(st.session_state.timeseries_df.head())
            else:
                st.info("Select at least one time series evaluation sub-step to apply.")




        # ==================== FRAGMENT: DATA PREVIEW ====================


        @st.fragment
        def render_data_preview():
            """
            Data Preview and Time Index Setup for Time Series Analysis.
            This function allows users to select a date/time column and set it as the DataFrame index.
            """
            # Initialize timeseries_df if not exists
            if "timeseries_df" not in st.session_state or st.session_state.timeseries_df is None:
                st.session_state.timeseries_df = st.session_state.df.copy() if st.session_state.df is not None else None

            df = st.session_state.timeseries_df
            active_col = st.session_state.get('active_index_col')

            st.markdown("### 📅 Time Series Data Preview & Index Setup")
            st.markdown("Select a date/time column to use as the time index for all time series analysis.")

            # Check if df already has a DatetimeIndex - if so, show current status
            if df is not None and isinstance(df.index, pd.DatetimeIndex):
                st.success(f"✅ Time Index is already set to: **{df.index.name or 'DatetimeIndex'}**")
                st.info("You can change the Time Index below if needed.")
            
            if df is None:
                st.warning("No data available. Please upload a file first.")
                return

            # Show current dataframe preview
            st.markdown("##### Current Data Preview:")
            st.dataframe(df.head(10))

            # Find potential date columns (object or datetime types)
            potential_date_cols = []
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    potential_date_cols.append(col)
                elif df[col].dtype == 'object':
                    # Check if it might be a date column by sampling
                    sample = df[col].dropna().head(10)
                    try:
                        pd.to_datetime(sample, errors='raise')
                        potential_date_cols.append(col)
                    except:
                        pass

            if not potential_date_cols:
                st.warning("⚠️ No date/time columns detected in your data. Time series analysis requires a date or datetime column.")
                return

            st.markdown("---")
            st.markdown("**Interactive Time Index Setup**")
            
            # Let user choose ONE column to be the primary index
            date_col = st.selectbox(
                "Which column should be your primary Timeline (X-axis)?",
                options=[col for col in df.columns if col != df.index.name],
                help="Select the specific date column you want to use for analysis.",
                key="ts_date_col_select"
            )

            is_day_first = st.toggle("Day comes before Month? (e.g., DD-MM-YYYY)", value=True, key="ts_day_first_toggle")

            # Use session state flag instead of st.rerun() inside fragment
            if st.button("Set as Active Index", key="ts_set_index_btn"):
                try:
                    # Convert chosen column to datetime
                    datetime_series = pd.to_datetime(df[date_col], dayfirst=is_day_first, errors='coerce')

                    if datetime_series.isna().all():
                        st.error(f"Could not parse dates from '{date_col}'. Check format.")
                    else:
                        # Set the new index WITHOUT dropping the original column
                        new_df = df.copy()
                        new_df = new_df.set_index(datetime_series)
                        new_df.index.name = date_col
                        
                        # CRITICAL: Ensure the index is a proper DatetimeIndex
                        new_df.index = pd.to_datetime(new_df.index)
                        
                        # Update Session State - MUST update st.session_state.df too!
                        st.session_state.timeseries_df = new_df
                        st.session_state.active_index_col = date_col
                        st.session_state.df = new_df  # This is critical - the sub-steps check st.session_state.df
                        st.session_state.ts_index_set = True
                        st.success(f"Success! Indexed by '{date_col}'. Original column preserved.")
                        
                except Exception as e:
                    st.error(f"Error: {e}")
            
            st.dataframe(df.head())

        # Outside the fragment, check if index was set and rerun if needed
        if st.session_state.get('ts_index_set', False):
            st.session_state.ts_index_set = False
            st.rerun()




        # ==================== FRAGMENT: ANALYSIS CONFIGURATION ====================

        @st.fragment
        def render_analysis_config():
            df = st.session_state.timeseries_df
            active_col = st.session_state.get('active_index_col')

            # Check if df exists and has a DatetimeIndex
            if df is None or not isinstance(df.index, pd.DatetimeIndex):
                st.warning("""
                ⚠️ **Time Index Not Set**
                
                Before using Time Series Analysis, you must first set a Time Index:
                
                1. Go to the **Time Series Evaluation** step in your pipeline
                2. Select **Time Series Data Review** sub-step
                3. Choose a date/time column and click **Set as Active Index**
                
                Once the Time Index is set, this analysis will be available.
                """)
                return

            with st.expander(f"Analyzing Timelines: {active_col}", expanded=True):            
                with st.container(border=True):
                    st.markdown(f"<h3 style='font-size:20px; color: white;'>Analysing: {active_col}</h3>", unsafe_allow_html=True)
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Value Column selection
                        available_cols = df.select_dtypes(include=['number']).columns.tolist()
                        available_cols = [c for c in available_cols if c != active_col]
                        selected_cols = st.multiselect("Select Data to Plot", options=available_cols, default=available_cols[:1])

                    with col2:
                        # Frequency mapping
                        freq_map = {"Year": "YE", "Month": "ME", "Week": "W", "Day": "D", "Hour": "h", "Minute": "min", "Second": "s"}
                        freq_label = st.selectbox("Granularity", list(freq_map.keys()), index=4)
                        selected_freq = freq_map[freq_label] 
                        
                        # Dynamic styling
                        l_width = st.slider("Line & Dot Size", 1, 15, 3)
                        f_size = 16

                if selected_cols:
                    # 1. Resample & Interpolate
                    resampled_df = df[selected_cols].resample(selected_freq).mean().interpolate(method='linear')
                    
                    # 2. Main Trend Plot (Interactive)
                    fig_trend = px.line(
                        resampled_df, 
                        x=resampled_df.index, 
                        y=selected_cols,
                        title=f"Trends by {freq_label}"
                    )
                    
                    # Sync line width to slider
                    fig_trend.update_traces(line=dict(width=l_width))
                    fig_trend.update_layout(
                        height=600, 
                        template="plotly_dark",
                        xaxis_title=active_col,
                        yaxis_title="Values",
                        font=dict(size=f_size)
                    )
                    
                    st.plotly_chart(fig_trend, use_container_width=True)

                    # 3. Lag Plot (Interactive)
                    st.markdown("---")
                    st.subheader(f"Lag Analysis: {selected_cols[0]}")
                    
                    # We take the first selected column for the lag plot
                    target_col = selected_cols[0]
                    
                    # Create the lag dataframe
                    lag_df = pd.DataFrame({
                        f'{target_col} (t)': resampled_df[target_col].shift(1),
                        f'{target_col} (t+1)': resampled_df[target_col]
                    }).dropna().tail(500)

                    fig_lag = px.scatter(
                        lag_df, 
                        x=f'{target_col} (t)', 
                        y=f'{target_col} (t+1)', 
                        title=f"Lag Plot: {target_col}",
                        opacity=0.7
                    )

                    # Sync dot size to slider (multiplied by 3 for visual balance)
                    fig_lag.update_traces(marker=dict(size=l_width * 3))
                    fig_lag.update_layout(
                        height=600, 
                        template="plotly_dark",
                        font=dict(size=f_size)
                    )
                    
                    st.plotly_chart(fig_lag, use_container_width=True)


            with st.expander(f"Model Configuration: {active_col}", expanded=True):    
                with st.container(border=True):
                    st.markdown(f"<h3 style='font-size:20px; color: white;'>Model Configuration: {active_col}</h3>", unsafe_allow_html=True)
                    m_col1, m_col2 = st.columns(2)
                    
                    with m_col1:
                        # 1. Select the Model Type
                        model_choice = st.selectbox("Select Forecast Model", ["None", "Simple Moving Average", "ARIMA"])
                        
                    with m_col2:
                        # 2. Dynamic Hyperparameters based on model choice
                        if model_choice == "Simple Moving Average":
                            window = st.slider("Window Size", 2, 100, 20)
                        elif model_choice == "ARIMA":
                            p = st.number_input("p (AR)", 0, 5, 1)
                            d = st.number_input("d (Diff)", 0, 2, 1)
                            q = st.number_input("q (MA)", 0, 5, 1)

                if selected_cols:
                    # Prepare data
                    resampled_df = df[selected_cols].resample(selected_freq).mean().interpolate()
                    target_series = resampled_df[selected_cols[0]]

                    # --- MODEL LOGIC ---
                    if model_choice == "Simple Moving Average":
                        resampled_df['Forecast'] = target_series.rolling(window=window).mean()
                    
                    elif model_choice == "ARIMA":
                        try:
                            model = sm.tsa.ARIMA(target_series, order=(p, d, q))
                            results = model.fit()
                            # Forecast next 20 steps
                            resampled_df['Forecast'] = results.fittedvalues
                        except Exception as e:
                            st.error(f"ARIMA failed: {e}")

                    # --- PLOTTING ---
                    fig_trend = px.line(resampled_df, y=resampled_df.columns, title=f"Model: {model_choice}")
                    st.plotly_chart(fig_trend, use_container_width=True)



        @st.fragment
        def render_change_point_detection():
            """Detect structural breaks in time series data"""
            import ruptures as rpt
            
            df = st.session_state.timeseries_df
            active_col = st.session_state.get('active_index_col')
            
            if df is None:
                st.warning("Please set a Time Index in the Preview section first.")
                return
            
            with st.expander("Structural Stability Analysis - Change Point Detection", expanded=True):
                with st.container(border=True):
                    
                    # Get numeric columns (excluding the index since it's now the time feature)
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if not numeric_cols:
                        st.warning("No numeric columns available for analysis.")
                        return
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        target_col = st.selectbox("Select Time Series to Analyze", options=numeric_cols, key="cp_target")
                        model_type = st.selectbox(
                            "Detection Method",
                            options=["Mean Shift (L1)", "Mean + Variance Shift (L2)", "Distribution Shift (RBF)"],
                            index=1,
                            key="cp_model"
                        )
                        model_map = {"Mean Shift (L1)": "l1", "Mean + Variance Shift (L2)": "l2", "Distribution Shift (RBF)": "rbf"}
                        selected_model = model_map[model_type]
                    
                    with col2:
                        min_segment = st.number_input("Minimum Segment Size", min_value=5, max_value=100, value=10, key="cp_minseg")
                        penalty_factor = st.slider("Sensitivity (lower = more breaks)", min_value=0.5, max_value=10.0, value=2.0, step=0.5, key="cp_penalty")
                    
                    # Add a max change points limit to prevent excessive breaks
                    max_change_points = st.slider("Max Change Points to Show", min_value=10, max_value=200, value=50, key="cp_max")
                    
                    if st.button("Detect Change Points", use_container_width=True, key="cp_btn"):
                        with st.spinner("Detecting structural breaks..."):
                            try:
                                # Use the column directly (index is already datetime)
                                series = df[target_col].dropna()
                                
                                if len(series) < 50:
                                    st.error(f"Need at least 50 data points. Only {len(series)} available.")
                                    return
                                
                                signal = series.values
                                algo = rpt.Pelt(model=selected_model, min_size=min_segment).fit(signal)
                                penalty = np.std(signal) * penalty_factor
                                change_points = algo.predict(pen=penalty)
                                
                                # Filter change points to be within bounds
                                change_points = [cp for cp in change_points if 0 < cp < len(signal)]
                                
                                # Limit number of change points to prevent clutter
                                if len(change_points) > max_change_points:
                                    step = len(change_points) // max_change_points
                                    change_points = change_points[::step][:max_change_points]
                                    st.info(f"Showing {len(change_points)} representative change points")
                                
                                st.success(f"Detected {len(change_points)} change points")
                                
                                # Create segments for display
                                segments_data = []
                                start_idx = 0
                                
                                for i, cp in enumerate(change_points):
                                    end_idx = cp
                                    segment_vals = signal[start_idx:end_idx]
                                    if len(segment_vals) > 0:
                                        segments_data.append({
                                            "Segment": i+1,
                                            "Start": series.index[start_idx].strftime('%Y-%m-%d %H:%M:%S'),
                                            "End": series.index[end_idx-1].strftime('%Y-%m-%d %H:%M:%S'),
                                            "Mean": f"{np.mean(segment_vals):.2f}",
                                            "Std": f"{np.std(segment_vals):.2f}",
                                            "Samples": len(segment_vals)
                                        })
                                    start_idx = cp
                                
                                # Add final segment
                                if start_idx < len(signal):
                                    segment_vals = signal[start_idx:]
                                    segments_data.append({
                                        "Segment": len(change_points)+1,
                                        "Start": series.index[start_idx].strftime('%Y-%m-%d %H:%M:%S'),
                                        "End": series.index[-1].strftime('%Y-%m-%d %H:%M:%S'),
                                        "Mean": f"{np.mean(segment_vals):.2f}",
                                        "Std": f"{np.std(segment_vals):.2f}",
                                        "Samples": len(segment_vals)
                                    })
                                
                                # Show preview of segments
                                display_df = pd.DataFrame(segments_data)
                                if len(display_df) > 20:
                                    st.info(f"Showing first 20 of {len(display_df)} segments")
                                    display_df = display_df.head(20)
                                
                                st.dataframe(display_df, use_container_width=True)
                                
                                # Plot with change points - USING THE INDEX FOR X-AXIS
                                fig, ax = plt.subplots(figsize=(14, 5))
                                # The index is already datetime, so we can plot directly
                                ax.plot(series.index, signal, 'b-', alpha=0.7, linewidth=1)
                                
                                # Add vertical lines at change points
                                for cp in change_points:
                                    if 0 < cp < len(series):
                                        ax.axvline(x=series.index[cp], color='r', linestyle='--', alpha=0.7, linewidth=1)
                                
                                ax.set_title(f"Change Point Detection - {target_col} ({len(change_points)} breaks)")
                                ax.set_xlabel("Time (Index)")
                                ax.set_ylabel(target_col)
                                plt.xticks(rotation=45)
                                plt.tight_layout()
                                st.pyplot(fig)
                                
                                # Download button for full results
                                full_segments_df = pd.DataFrame(segments_data)
                                csv = full_segments_df.to_csv(index=False)
                                st.download_button(
                                    label="Download Full Change Point Report (CSV)",
                                    data=csv,
                                    file_name=f"change_points_{target_col}.csv",
                                    mime="text/csv"
                                )
                                
                            except Exception as e:
                                st.error(f"Error: {e}")
                                st.exception(e)
        
        
        @st.fragment
        def render_protocol_audit():
            """Audit for protocol changes: encoding shifts, data property changes, and feature consistency"""
            import matplotlib.pyplot as plt
            from scipy import stats as scipy_stats
            
            df = st.session_state.get('timeseries_df')
            active_col = st.session_state.get('active_index_col')
            
            if df is None:
                st.warning("⚠️ Please set a Time Index in the Preview section first.")
                return
            
            with st.expander("📋 Protocol Change Audit", expanded=True):
                with st.container(border=True):
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if not numeric_cols:
                        st.warning("No numeric columns available for audit.")
                        return
                    
                    # Interactive controls
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        target_col = st.selectbox("Select Column to Audit", options=numeric_cols, key="pa_target")
                    
                    with col2:
                        window_days = st.number_input("Window Size (days)", min_value=1, max_value=90, value=14, key="pa_window")
                    
                    with col3:
                        overlap_pct = st.slider("Window Overlap (%)", min_value=0, max_value=90, value=50, key="pa_overlap",
                                            help="Sliding window overlap for smoother transition detection")
                        detect_encodings = st.checkbox("Detect Encoding Changes", value=True, 
                                                    help="Check for unit scaling, quantization shifts, or data type changes")
                    
                    if st.button("🔍 Run Protocol Audit", use_container_width=True, key="pa_btn"):
                        with st.spinner("Analyzing protocol stability..."):
                            # Get the series (index is already datetime)
                            series = df[target_col].dropna()
                            start_date = series.index.min()
                            end_date = series.index.max()
                            
                            # Create sliding windows with overlap
                            step_days = window_days * (1 - overlap_pct/100)
                            windows = []
                            current_start = start_date
                            
                            while current_start <= end_date:
                                window_end = current_start + pd.Timedelta(days=window_days)
                                if window_end > end_date:
                                    window_end = end_date
                                windows.append((current_start, window_end))
                                current_start += pd.Timedelta(days=step_days)
                            
                            # Analyze each window
                            results = []
                            prev_stats = None
                            
                            for i, (win_start, win_end) in enumerate(windows):
                                window_data = series[(series.index >= win_start) & (series.index < win_end)]
                                
                                if len(window_data) < 10:
                                    continue
                                
                                vals = window_data.values
                                unique_vals = np.unique(vals)
                                
                                # Calculate comprehensive statistical properties
                                stats_dict = {
                                    'Window_Start': win_start.strftime('%Y-%m-%d'),
                                    'Window_End': win_end.strftime('%Y-%m-%d'),
                                    'Days': (win_end - win_start).days,
                                    'Count': len(vals),
                                    'Mean': np.mean(vals),
                                    'Std': np.std(vals),
                                    'Min': np.min(vals),
                                    'Max': np.max(vals),
                                    'Median': np.median(vals),
                                    'Q1': np.percentile(vals, 25),
                                    'Q3': np.percentile(vals, 75),
                                    'IQR': np.percentile(vals, 75) - np.percentile(vals, 25),
                                    'Unique_Values': len(unique_vals),
                                    'Zeros_Pct': (vals == 0).mean() * 100,
                                    'Negatives_Pct': (vals < 0).mean() * 100 if np.any(vals < 0) else 0,
                                    'CV': np.std(vals) / np.mean(vals) if np.mean(vals) != 0 else 0,
                                    'Skewness': scipy_stats.skew(vals) if len(vals) > 2 else 0,
                                    'Kurtosis': scipy_stats.kurtosis(vals) if len(vals) > 3 else 0
                                }
                                
                                # Detect property changes (Question 1)
                                property_alerts = []
                                if i > 0 and results:
                                    prev = results[-1]
                                    
                                    # Mean shift detection
                                    mean_change_pct = abs(stats_dict['Mean'] - prev['Mean']) / prev['Mean'] * 100 if prev['Mean'] != 0 else 0
                                    if mean_change_pct > 30:
                                        property_alerts.append(f"Mean {'↑' if stats_dict['Mean'] > prev['Mean'] else '↓'} {mean_change_pct:.0f}%")
                                    
                                    # Std deviation shift
                                    std_change_pct = abs(stats_dict['Std'] - prev['Std']) / prev['Std'] * 100 if prev['Std'] != 0 else 0
                                    if std_change_pct > 50:
                                        property_alerts.append(f"Std {'↑' if stats_dict['Std'] > prev['Std'] else '↓'} {std_change_pct:.0f}%")
                                    
                                    # Range expansion/contraction
                                    current_range = stats_dict['Max'] - stats_dict['Min']
                                    prev_range = prev['Max'] - prev['Min']
                                    if prev_range > 0:
                                        range_ratio = current_range / prev_range
                                        if range_ratio > 2:
                                            property_alerts.append(f"Range expanded {range_ratio:.1f}x")
                                        elif range_ratio < 0.5:
                                            property_alerts.append(f"Range contracted {range_ratio:.1f}x")
                                    
                                    # IQR change (distribution spread)
                                    iqr_change_pct = abs(stats_dict['IQR'] - prev['IQR']) / prev['IQR'] * 100 if prev['IQR'] != 0 else 0
                                    if iqr_change_pct > 75:
                                        property_alerts.append(f"IQR {'↑' if stats_dict['IQR'] > prev['IQR'] else '↓'} {iqr_change_pct:.0f}%")
                                
                                # Detect encoding changes (Question 2)
                                encoding_alerts = []
                                if detect_encodings and len(unique_vals) > 10:
                                    # Detect quantization steps (possible unit scaling)
                                    sorted_unique = np.sort(unique_vals)
                                    steps = np.diff(sorted_unique[:min(100, len(sorted_unique))])
                                    typical_step = np.median(steps) if len(steps) > 0 else 0
                                    stats_dict['Typical_Step'] = typical_step
                                    
                                    # Detect integer vs float encoding
                                    is_integer_window = np.all(vals == vals.astype(int))
                                    stats_dict['Is_Integer'] = is_integer_window
                                    
                                    if i > 0 and results and prev_stats:
                                        # Step size change (unit scaling)
                                        if 'Typical_Step' in prev_stats and typical_step > 0 and prev_stats['Typical_Step'] > 0:
                                            step_ratio = typical_step / prev_stats['Typical_Step']
                                            if 0.05 < step_ratio < 0.2:
                                                encoding_alerts.append(f"Unit scaled DOWN ~{1/step_ratio:.0f}x")
                                            elif 5 < step_ratio < 20:
                                                encoding_alerts.append(f"Unit scaled UP ~{step_ratio:.0f}x")
                                        
                                        # Integer to float conversion
                                        if 'Is_Integer' in prev_stats and prev_stats['Is_Integer'] != is_integer_window:
                                            encoding_alerts.append(f"Encoding changed: {'Integer→Float' if not is_integer_window else 'Float→Integer'}")
                                        
                                        # Precision change (unique values spike/drop)
                                        unique_ratio = stats_dict['Unique_Values'] / prev_stats['Unique_Values'] if prev_stats['Unique_Values'] > 0 else 1
                                        if unique_ratio > 3:
                                            encoding_alerts.append(f"Precision increased: {stats_dict['Unique_Values']} vs {prev_stats['Unique_Values']}")
                                        elif unique_ratio < 0.33:
                                            encoding_alerts.append(f"Precision decreased: {stats_dict['Unique_Values']} vs {prev_stats['Unique_Values']}")
                                
                                # Combine alerts
                                all_alerts = []
                                if property_alerts:
                                    all_alerts.append(f"📊 Property: {', '.join(property_alerts)}")
                                if encoding_alerts:
                                    all_alerts.append(f"🔢 Encoding: {', '.join(encoding_alerts)}")
                                
                                stats_dict['Alert'] = ' | '.join(all_alerts) if all_alerts else ''
                                results.append(stats_dict)
                                prev_stats = stats_dict
                            
                            # Clean up before display (remove internal values)
                            display_results = []
                            for r in results:
                                display_row = {k: v for k, v in r.items() if not k.startswith('_')}
                                display_results.append(display_row)
                            
                            results_df = pd.DataFrame(display_results)
                            
                            # ========== ANSWERS TO QUESTIONS ==========
                            st.success(f"✅ Analyzed {len(results_df)} time windows over {len(series)} data points")
                            
                            # Answer questions
                            has_property_changes = results_df['Alert'].str.contains('Property', na=False).any() if 'Alert' in results_df.columns else False
                            has_encoding_changes = results_df['Alert'].str.contains('Encoding', na=False).any() if 'Alert' in results_df.columns else False
                            
                            col_ans1, col_ans2 = st.columns(2)
                            with col_ans1:
                                if has_property_changes:
                                    st.error("❌ **Properties DO change sequentially**")
                                    st.caption("Detected mean shifts, std deviations, or range changes")
                                else:
                                    st.success("✅ **Properties remain stable**")
                                    st.caption("No significant sequential property changes detected")
                            
                            with col_ans2:
                                if has_encoding_changes:
                                    st.error("❌ **Different feature encodings detected**")
                                    st.caption("Unit scaling, precision, or data type changes found")
                                else:
                                    st.success("✅ **Consistent feature encoding**")
                                    st.caption("No encoding shifts across consecutive windows")
                            
                            # ========== TABS FOR DIFFERENT VIEWS ==========
                            tab_summary, tab_alerts, tab_viz, tab_heatmap = st.tabs(["📊 Summary Statistics", "⚠️ Alerts & Changes", "📈 Visualizations", "🔥 Heatmap"])
                            
                            with tab_summary:
                                st.subheader("Window Statistics Overview")
                                # Show key metrics
                                summary_metrics = results_df[['Window_Start', 'Window_End', 'Days', 'Count', 'Mean', 'Std', 'Median', 'IQR', 'Unique_Values', 'Zeros_Pct', 'CV', 'Skewness']].copy()
                                st.dataframe(summary_metrics, use_container_width=True)
                                
                                # Download button for full report
                                csv = results_df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Full Audit Report (CSV)",
                                    data=csv,
                                    file_name=f"protocol_audit_{target_col}.csv",
                                    mime="text/csv",
                                    key="pa_download_summary"
                                )
                            
                            with tab_alerts:
                                # Collect all alerts
                                alerts_found = []
                                if 'Alert' in results_df.columns:
                                    for idx, row in results_df.iterrows():
                                        if pd.notna(row['Alert']) and row['Alert'] != '':
                                            # Parse alert type
                                            alert_type = []
                                            if 'Property' in row['Alert']:
                                                alert_type.append('Property')
                                            if 'Encoding' in row['Alert']:
                                                alert_type.append('Encoding')
                                            
                                            alerts_found.append({
                                                'Window': idx + 1,
                                                'Start Date': row['Window_Start'],
                                                'End Date': row['Window_End'],
                                                'Alert Type': ' + '.join(alert_type),
                                                'Message': row['Alert']
                                            })
                                
                                if alerts_found:
                                    st.warning(f"⚠️ Found {len(alerts_found)} windows with protocol changes")
                                    
                                    # Interactive filter for alert type
                                    alert_filter = st.radio(
                                        "Filter alerts by type:",
                                        ["All", "Property Changes Only", "Encoding Changes Only", "Both Types"],
                                        horizontal=True,
                                        key="pa_filter"
                                    )
                                    
                                    filtered = alerts_found
                                    if alert_filter == "Property Changes Only":
                                        filtered = [a for a in alerts_found if 'Property' in a['Alert Type'] and 'Encoding' not in a['Alert Type']]
                                    elif alert_filter == "Encoding Changes Only":
                                        filtered = [a for a in alerts_found if 'Encoding' in a['Alert Type'] and 'Property' not in a['Alert Type']]
                                    elif alert_filter == "Both Types":
                                        filtered = [a for a in alerts_found if 'Property' in a['Alert Type'] and 'Encoding' in a['Alert Type']]
                                    
                                    st.dataframe(pd.DataFrame(filtered), use_container_width=True)
                                else:
                                    st.success("✅ No protocol changes detected across all windows")
                            
                            with tab_viz:
                                st.subheader("Interactive Protocol Analysis Visualizations")
                                
                                # Plot type selector
                                plot_options = st.multiselect(
                                    "Select metrics to visualize:",
                                    options=['Mean', 'Std', 'Median', 'IQR', 'Unique_Values', 'Zeros_Pct', 'CV', 'Skewness', 'Kurtosis', 'Negatives_Pct'],
                                    default=['Mean', 'Std', 'Zeros_Pct'],
                                    key="pa_plots"
                                )
                                
                                if plot_options:
                                    n_plots = len(plot_options)
                                    fig, axes = plt.subplots(n_plots, 1, figsize=(14, 4 * n_plots))
                                    if n_plots == 1:
                                        axes = [axes]
                                    
                                    for idx, metric in enumerate(plot_options):
                                        ax = axes[idx]
                                        ax.plot(range(len(results_df)), results_df[metric], 'o-', markersize=4, linewidth=2, 
                                            color=plt.cm.tab10(idx % 10))
                                        ax.fill_between(range(len(results_df)), results_df[metric], alpha=0.2, color=plt.cm.tab10(idx % 10))
                                        ax.set_title(f'{metric} over Time', fontsize=12, fontweight='bold')
                                        ax.set_ylabel(metric)
                                        ax.set_xlabel('Window Number')
                                        ax.grid(True, alpha=0.3)
                                        
                                        # Highlight alert windows with red dashed lines
                                        if 'Alert' in results_df.columns:
                                            alert_indices = results_df[results_df['Alert'] != ''].index
                                            for alert_idx in alert_indices:
                                                ax.axvline(x=alert_idx, color='red', alpha=0.5, linewidth=2, linestyle='--')
                                        
                                        # Add threshold lines for specific metrics
                                        if metric == 'Mean':
                                            overall_mean = results_df['Mean'].mean()
                                            ax.axhline(y=overall_mean, color='gray', linestyle=':', alpha=0.7, 
                                                    label=f'Overall Mean: {overall_mean:.2f}')
                                            ax.legend()
                                        
                                        if metric == 'CV':
                                            ax.axhline(y=0.5, color='orange', linestyle='--', alpha=0.5, 
                                                    label='High variability threshold (0.5)')
                                            ax.legend()
                                        
                                        if metric == 'Zeros_Pct':
                                            ax.axhline(y=30, color='red', linestyle='--', alpha=0.5, 
                                                    label='Suspicious threshold (30%)')
                                            ax.axhline(y=90, color='darkred', linestyle='-', alpha=0.3, 
                                                    label='Sensor failure (90%)')
                                            ax.legend(fontsize=8)
                                    
                                    plt.tight_layout()
                                    st.pyplot(fig)
                            
                            with tab_heatmap:
                                st.subheader("Protocol Change Heatmap")
                                st.caption("Red = Higher values, Blue = Lower values - Look for sudden color shifts between windows")
                                
                                # Create heatmap data
                                heatmap_metrics = ['Mean', 'Std', 'Unique_Values', 'Zeros_Pct', 'CV', 'Skewness']
                                heatmap_data = results_df[heatmap_metrics].T
                                
                                fig, ax = plt.subplots(figsize=(14, max(6, len(heatmap_metrics) * 0.5)))
                                im = ax.imshow(heatmap_data.values, aspect='auto', cmap='RdYlBu_r')
                                
                                # Add window numbers as x-ticks
                                ax.set_xticks(range(len(results_df)))
                                ax.set_xticklabels(range(1, len(results_df)+1), rotation=45)
                                
                                # Add metric names as y-ticks
                                ax.set_yticks(range(len(heatmap_metrics)))
                                ax.set_yticklabels(heatmap_metrics)
                                
                                ax.set_title('Protocol Metrics Heatmap (Red=High, Blue=Low)', fontsize=12)
                                plt.colorbar(im, ax=ax)
                                
                                # Add vertical lines for alerts
                                if 'Alert' in results_df.columns:
                                    alert_indices = results_df[results_df['Alert'] != ''].index
                                    for alert_idx in alert_indices:
                                        ax.axvline(x=alert_idx, color='black', alpha=0.8, linewidth=1, linestyle='-')
                                
                                plt.tight_layout()
                                st.pyplot(fig)
                                
                                # Add annotation for heatmap interpretation
                                st.info("💡 **Interpretation**: Each column is a time window. Sudden vertical color shifts indicate protocol changes. Black vertical lines mark windows with detected alerts.")
        
        
    
        # ==================== FRAGMENT: DECOMPOSITION RESULTS ====================
        @st.fragment
        def render_decomposition_results():
            """Render pattern decomposition results - analyzes trend, seasonality, and residuals"""
            df = st.session_state.get('timeseries_df')
            active_col = st.session_state.get('active_index_col')
            
            if df is None:
                st.warning("⚠️ Please set a Time Index in the Preview section first.")
                return
            
            with st.expander("📊 Pattern Decomposition (Trend, Seasonality, Residuals)", expanded=True):
                with st.container(border=True):
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if not numeric_cols:
                        st.warning("No numeric columns available for decomposition.")
                        return
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        target_col = st.selectbox("Select column to decompose", options=numeric_cols, key="decomp_col")
                    
                    with col2:
                        model_type = st.selectbox("Model Type", ["additive", "multiplicative"], key="decomp_model",
                                                help="Additive: Seasonal fluctuations constant. Multiplicative: Seasonal fluctuations scale with trend")
                    
                    with col3:
                        period_options = {"Weekly (7 days)": 7, "Monthly (30 days)": 30, "Quarterly (90 days)": 90, "Yearly (365 days)": 365}
                        period_choice = st.selectbox("Seasonal Period", list(period_options.keys()), key="decomp_period")
                        period = period_options[period_choice]
                    
                    if st.button("🔍 Run Decomposition", use_container_width=True, key="run_decomp"):
                        with st.spinner("Decomposing time series..."):
                            try:
                                from statsmodels.tsa.seasonal import seasonal_decompose
                                
                                series = df[target_col].dropna()
                                
                                # Need enough data points
                                if len(series) < period * 2:
                                    st.error(f"Need at least {period * 2} data points. Only {len(series)} available.")
                                    return
                                
                                # Perform decomposition
                                decomposition = seasonal_decompose(series, model=model_type, period=period)
                                
                                # Calculate strength metrics
                                trend_vals = decomposition.trend.dropna()
                                seasonal_vals = decomposition.seasonal.dropna()
                                resid_vals = decomposition.resid.dropna()
                                
                                trend_strength = 1 - np.var(resid_vals) / np.var(trend_vals) if len(trend_vals) > 0 and np.var(trend_vals) > 0 else 0
                                seasonal_strength = 1 - np.var(resid_vals) / np.var(seasonal_vals) if len(seasonal_vals) > 0 and np.var(seasonal_vals) > 0 else 0
                                
                                # Detect anomalies in residuals (values > 3 standard deviations)
                                resid_mean = resid_vals.mean()
                                resid_std = resid_vals.std()
                                anomalies = resid_vals[np.abs(resid_vals - resid_mean) > 3 * resid_std]
                                
                                # Display metrics
                                st.success("✅ Decomposition Complete")
                                c1, c2, c3 = st.columns(3)
                                c1.metric("Trend Strength", f"{trend_strength:.3f}", 
                                        help="1 = Strong trend, 0 = No trend")
                                c2.metric("Seasonal Strength", f"{seasonal_strength:.3f}",
                                        help="1 = Strong seasonality, 0 = No seasonality")
                                c3.metric("Anomalies Detected", len(anomalies))
                                
                                # Create decomposition plots
                                fig, axes = plt.subplots(4, 1, figsize=(14, 10), sharex=True)
                                
                                axes[0].plot(series.index, series.values, 'b-', linewidth=1)
                                axes[0].set_title('Original Series', fontsize=12, fontweight='bold')
                                axes[0].set_ylabel('Value')
                                axes[0].grid(True, alpha=0.3)
                                
                                axes[1].plot(decomposition.trend.index, decomposition.trend.values, 'g-', linewidth=1)
                                axes[1].set_title('Trend Component', fontsize=12, fontweight='bold')
                                axes[1].set_ylabel('Trend')
                                axes[1].grid(True, alpha=0.3)
                                
                                axes[2].plot(decomposition.seasonal.index, decomposition.seasonal.values, 'orange', linewidth=1)
                                axes[2].set_title('Seasonal Component', fontsize=12, fontweight='bold')
                                axes[2].set_ylabel('Seasonal')
                                axes[2].grid(True, alpha=0.3)
                                
                                axes[3].plot(decomposition.resid.index, decomposition.resid.values, 'r-', linewidth=1)
                                axes[3].axhline(y=0, color='black', linestyle='-', alpha=0.5)
                                axes[3].axhline(y=3*resid_std, color='red', linestyle='--', alpha=0.5, label=f'+3σ ({3*resid_std:.2f})')
                                axes[3].axhline(y=-3*resid_std, color='red', linestyle='--', alpha=0.5, label=f'-3σ ({-3*resid_std:.2f})')
                                
                                # Highlight anomalies
                                if len(anomalies) > 0:
                                    anomaly_times = anomalies.index
                                    anomaly_values = anomalies.values
                                    axes[3].scatter(anomaly_times, anomaly_values, color='red', s=50, zorder=5, label='Anomalies')
                                
                                axes[3].set_title('Residual Component (Anomalies marked)', fontsize=12, fontweight='bold')
                                axes[3].set_ylabel('Residual')
                                axes[3].legend()
                                axes[3].grid(True, alpha=0.3)
                                
                                plt.xlabel('Time')
                                plt.tight_layout()
                                st.pyplot(fig)
                                
                                # Show anomalies if any
                                if len(anomalies) > 0:
                                    with st.expander(f"⚠️ Anomalies Detected ({len(anomalies)})"):
                                        anomalies_df = pd.DataFrame({
                                            'Timestamp': anomalies.index.strftime('%Y-%m-%d %H:%M:%S'),
                                            'Residual Value': anomalies.values,
                                            'Deviation (σ)': np.abs((anomalies.values - resid_mean) / resid_std)
                                        })
                                        st.dataframe(anomalies_df, use_container_width=True)
                                        
                                        csv = anomalies_df.to_csv(index=False)
                                        st.download_button("📥 Download Anomalies CSV", csv, f"anomalies_{target_col}.csv", "text/csv")
                                else:
                                    st.info("✅ No significant anomalies detected in residuals")
                                
                            except ImportError:
                                st.error("❌ statsmodels not installed. Run: `pip install statsmodels`")
                            except Exception as e:
                                st.error(f"Error: {e}")


        # ==================== FRAGMENT: STATIONARITY RESULTS ====================
        @st.fragment
        def render_stationarity_results():
            """Render stationarity test results - ADF, KPSS, and ACF/PACF plots"""
            df = st.session_state.get('timeseries_df')
            active_col = st.session_state.get('active_index_col')
            
            if df is None:
                st.warning("⚠️ Please set a Time Index in the Preview section first.")
                return
            
            with st.expander("📐 Stationarity Analysis", expanded=True):
                with st.container(border=True):
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if not numeric_cols:
                        st.warning("No numeric columns available for stationarity testing.")
                        return
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        target_col = st.selectbox("Select column to test", options=numeric_cols, key="stationarity_col")
                    
                    with col2:
                        diff_level = st.number_input("Differencing Level", min_value=0, max_value=2, value=0, key="diff_level",
                                                    help="0 = Original series, 1 = First difference, 2 = Second difference")
                    
                    if st.button("📊 Run Stationarity Tests", use_container_width=True, key="run_stationarity"):
                        with st.spinner("Performing stationarity tests..."):
                            try:
                                from statsmodels.tsa.stattools import adfuller, kpss
                                from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
                                
                                series = df[target_col].dropna()
                                
                                # Apply differencing if requested
                                if diff_level > 0:
                                    series = series.diff(diff_level).dropna()
                                    st.caption(f"📌 Testing differenced series (order {diff_level})")
                                
                                if len(series) < 10:
                                    st.error(f"Not enough data points after differencing. Only {len(series)} available.")
                                    return
                                
                                # ADF Test (Null hypothesis: series is non-stationary)
                                adf_result = adfuller(series, autolag='AIC')
                                
                                # KPSS Test (Null hypothesis: series is stationary)
                                try:
                                    kpss_result = kpss(series, regression='c', nlags='auto')
                                    kpss_stat = kpss_result[0]
                                    kpss_pvalue = kpss_result[1]
                                    kpss_critical = kpss_result[3]
                                    kpss_stationary = kpss_pvalue > 0.05
                                except Exception as e:
                                    kpss_stat = None
                                    kpss_pvalue = None
                                    kpss_critical = None
                                    kpss_stationary = None
                                    st.warning(f"KPSS test failed: {e}")
                                
                                # Display results
                                st.success("✅ Stationarity Tests Complete")
                                
                                # Results in columns
                                col_a, col_b = st.columns(2)
                                
                                with col_a:
                                    st.markdown("**Augmented Dickey-Fuller (ADF) Test**")
                                    st.metric("Test Statistic", f"{adf_result[0]:.4f}")
                                    st.metric("p-value", f"{adf_result[1]:.4f}")
                                    st.metric("Critical Value (5%)", f"{adf_result[4]['5%']:.4f}")
                                    
                                    is_stationary_adf = adf_result[1] < 0.05
                                    if is_stationary_adf:
                                        st.success("✅ Stationary (reject non-stationarity)")
                                    else:
                                        st.warning("❌ Non-stationary (fail to reject non-stationarity)")
                                
                                with col_b:
                                    st.markdown("**Kwiatkowski-Phillips-Schmidt-Shin (KPSS) Test**")
                                    if kpss_stat is not None:
                                        st.metric("Test Statistic", f"{kpss_stat:.4f}")
                                        st.metric("p-value", f"{kpss_pvalue:.4f}")
                                        if kpss_critical:
                                            st.metric("Critical Value (5%)", f"{kpss_critical['5%']:.4f}")
                                        
                                        if kpss_stationary:
                                            st.success("✅ Stationary (fail to reject stationarity)")
                                        else:
                                            st.warning("❌ Non-stationary (reject stationarity)")
                                    else:
                                        st.info("KPSS test not available")
                                
                                # Combined verdict
                                st.markdown("---")
                                st.subheader("🎯 Final Verdict")
                                
                                if kpss_stationary is not None:
                                    if is_stationary_adf and kpss_stationary:
                                        st.success("✅ **Series is STATIONARY** - Both tests agree")
                                    elif not is_stationary_adf and not kpss_stationary:
                                        st.warning("⚠️ **Series is NON-STATIONARY** - Both tests agree")
                                    else:
                                        st.warning("⚠️ **AMBIGUOUS RESULTS** - Tests disagree (series may have unit root with deterministic trend)")
                                else:
                                    if is_stationary_adf:
                                        st.success("✅ **Series is STATIONARY** (based on ADF test)")
                                    else:
                                        st.warning("⚠️ **Series is NON-STATIONARY** (based on ADF test)")
                                
                                # ACF and PACF plots
                                st.markdown("---")
                                st.subheader("Autocorrelation Analysis")
                                
                                fig, axes = plt.subplots(1, 2, figsize=(14, 5))
                                
                                # ACF Plot
                                plot_acf(series, lags=min(40, len(series)//2), ax=axes[0], alpha=0.05)
                                axes[0].set_title('Autocorrelation Function (ACF)', fontsize=12, fontweight='bold')
                                axes[0].set_xlabel('Lags')
                                axes[0].set_ylabel('Autocorrelation')
                                axes[0].grid(True, alpha=0.3)
                                
                                # PACF Plot
                                plot_pacf(series, lags=min(40, len(series)//2), ax=axes[1], alpha=0.05, method='ywm')
                                axes[1].set_title('Partial Autocorrelation Function (PACF)', fontsize=12, fontweight='bold')
                                axes[1].set_xlabel('Lags')
                                axes[1].set_ylabel('Partial Autocorrelation')
                                axes[1].grid(True, alpha=0.3)
                                
                                plt.tight_layout()
                                st.pyplot(fig)
                                
                                # Interpretation guidance
                                with st.expander("📖 How to interpret these results"):
                                    st.markdown("""
                                    **Stationarity Tests:**
                                    - **ADF Test**: Null = Non-stationary. p-value < 0.05 → Stationary
                                    - **KPSS Test**: Null = Stationary. p-value > 0.05 → Stationary
                                    
                                    **ACF/PACF Plots:**
                                    - **Slowly decaying ACF** → Sign of non-stationarity (needs differencing)
                                    - **Sharp cutoff in PACF after lag p** → AR(p) model
                                    - **Sharp cutoff in ACF after lag q** → MA(q) model
                                    - **Exponential decay in both** → ARMA model
                                    """)
                                
                            except ImportError:
                                st.error("❌ statsmodels not installed. Run: `pip install statsmodels`")
                            except Exception as e:
                                st.error(f"Error: {e}")


        # ==================== FRAGMENT: FORECASTING SECTION ====================
        @st.fragment
        def render_forecasting_section():
            """
            Interactive time series forecasting fragment
            Uses session state timeseries_df with datetime index
            """
            df = st.session_state.get('timeseries_df')
            active_col = st.session_state.get('active_index_col')
            
            if df is None:
                st.warning("⚠️ Please set a Time Index in the Preview section first.")
                return
            
            with st.expander("🔮 Time Series Forecasting", expanded=True):
                with st.container(border=True):
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if not numeric_cols:
                        st.warning("No numeric columns available for forecasting.")
                        return
                    
                    # Initialize session state for this fragment
                    if 'forecast_results' not in st.session_state:
                        st.session_state.forecast_results = None
                    
                    # ==================== FORECASTING CONFIGURATION ====================
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        target_col = st.selectbox("Select column to forecast", options=numeric_cols, key="forecast_col")
                        forecast_horizon = st.slider("Forecast Horizon (steps ahead)", 1, 50, 10, key="horizon")
                        confidence_level = st.slider("Confidence Level (%)", 80, 99, 95, key="confidence")
                    
                    with col2:
                        train_pct = st.slider("Training Data %", 50, 95, 80, key="train_pct")
                        train_size = int(len(df) * train_pct / 100)
                        train_df = df.iloc[:train_size]
                        test_df = df.iloc[train_size:]
                        st.metric("Training Points", len(train_df))
                        st.metric("Test Points", len(test_df))
                    
                    with col3:
                        forecast_method = st.selectbox(
                            "Forecast Method",
                            ["Simple Moving Average", "Exponential Smoothing", "Naive (Last Value)"],
                            key="forecast_method"
                        )
                    
                    # ==================== METHOD-SPECIFIC PARAMETERS ====================
                    params = {}
                    
                    if forecast_method == "Simple Moving Average":
                        col_a, col_b = st.columns(2)
                        with col_a:
                            window = st.slider("Window Size", 2, 30, 5, key="ma_window")
                        params = {'window': window}
                    
                    elif forecast_method == "Exponential Smoothing":
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            trend = st.selectbox("Trend", [None, "add", "mul"], key="es_trend")
                        with col_b:
                            seasonal = st.selectbox("Seasonal", [None, "add", "mul"], key="es_seasonal")
                        with col_c:
                            seasonal_periods = st.number_input("Seasonal Periods", 1, 52, 7, key="es_sp")
                        params = {
                            'trend': trend if trend != "None" else None,
                            'seasonal': seasonal if seasonal != "None" else None,
                            'seasonal_periods': seasonal_periods
                        }
                    
                    # ==================== FORECASTING FUNCTIONS ====================
                    def moving_average_forecast(series, steps, window, confidence_level):
                        last_values = series.iloc[-window:]
                        forecast_value = last_values.mean()
                        forecast_std = last_values.std()
                        
                        forecast = [forecast_value] * steps
                        z_score = -np.percentile(np.random.standard_normal(10000), (100 - confidence_level) / 2)
                        ci = z_score * forecast_std
                        
                        return forecast, ci, forecast_std
                    
                    def exponential_smoothing_forecast(series, steps, confidence_level, **kwargs):
                        try:
                            from statsmodels.tsa.holtwinters import ExponentialSmoothing
                            
                            model = ExponentialSmoothing(
                                series,
                                trend=kwargs.get('trend'),
                                seasonal=kwargs.get('seasonal'),
                                seasonal_periods=kwargs.get('seasonal_periods', 7)
                            )
                            fitted_model = model.fit()
                            forecast = fitted_model.forecast(steps)
                            
                            # Approximate confidence intervals from residuals
                            residuals = fitted_model.resid.dropna()
                            sigma = np.std(residuals)
                            z_score = -np.percentile(np.random.standard_normal(10000), (100 - confidence_level) / 2)
                            ci = z_score * sigma
                            
                            return forecast.values, ci, sigma
                        except Exception as e:
                            st.error(f"Exponential Smoothing error: {str(e)}")
                            return None, None, None
                    
                    def naive_forecast(series, steps, confidence_level):
                        last_value = series.iloc[-1]
                        last_std = series.iloc[-10:].std() if len(series) >= 10 else series.std()
                        
                        forecast = [last_value] * steps
                        z_score = -np.percentile(np.random.standard_normal(10000), (100 - confidence_level) / 2)
                        ci = z_score * last_std
                        
                        return forecast, ci, last_std
                    
                    # ==================== RUN FORECAST BUTTON ====================
                    if st.button("🚀 Run Forecast", type="primary", use_container_width=True, key="run_forecast"):
                        with st.spinner("Generating forecast..."):
                            series = df[target_col]
                            train = series.iloc[:train_size]
                            test = series.iloc[train_size:train_size + forecast_horizon] if len(series) > train_size else None
                            
                            # Run selected forecast method
                            if forecast_method == "Simple Moving Average":
                                forecast, ci, sigma = moving_average_forecast(train, forecast_horizon, params['window'], confidence_level)
                            elif forecast_method == "Exponential Smoothing":
                                forecast, ci, sigma = exponential_smoothing_forecast(train, forecast_horizon, confidence_level, **params)
                            else:
                                forecast, ci, sigma = naive_forecast(train, forecast_horizon, confidence_level)
                            
                            if forecast is not None:
                                # Calculate metrics if test data available
                                mae = None
                                rmse = None
                                if test is not None and len(test) == len(forecast):
                                    mae = np.mean(np.abs(test.values - forecast))
                                    rmse = np.sqrt(np.mean((test.values - forecast)**2))
                                
                                # Store results
                                st.session_state.forecast_results = {
                                    'forecast': forecast,
                                    'forecast_index': range(1, forecast_horizon + 1),
                                    'ci': ci,
                                    'train': train,
                                    'test': test,
                                    'mae': mae,
                                    'rmse': rmse,
                                    'method': forecast_method,
                                    'horizon': forecast_horizon,
                                    'target_col': target_col,
                                    'sigma': sigma
                                }
                                st.rerun()
                    
                    # ==================== DISPLAY FORECAST RESULTS ====================
                    if st.session_state.forecast_results:
                        results = st.session_state.forecast_results
                        forecast = results['forecast']
                        ci = results['ci']
                        train = results['train']
                        test = results.get('test')
                        
                        st.markdown("---")
                        st.subheader("📈 Forecast Results")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(14, 6))
                        
                        # Historical data
                        ax.plot(range(len(train)), train.values, 'b-', linewidth=2, label='Historical (Training)')
                        
                        # Test data if available
                        if test is not None:
                            test_idx = range(len(train), len(train) + len(test))
                            ax.plot(test_idx, test.values, 'g-', linewidth=2, marker='o', markersize=4, label='Actual (Test)')
                        
                        # Forecast
                        forecast_idx = range(len(train), len(train) + len(forecast))
                        ax.plot(forecast_idx, forecast, 'r-', linewidth=2, marker='s', markersize=5, label=f'Forecast ({results["method"]})')
                        
                        # Confidence interval
                        lower_bound = np.array(forecast) - ci
                        upper_bound = np.array(forecast) + ci
                        ax.fill_between(forecast_idx, lower_bound, upper_bound, color='red', alpha=0.2, label=f'{confidence_level}% Confidence Interval')
                        
                        ax.set_title(f'Time Series Forecast - {results["target_col"]}', fontsize=14, fontweight='bold')
                        ax.set_xlabel('Time Steps')
                        ax.set_ylabel('Value')
                        ax.legend()
                        ax.grid(True, alpha=0.3)
                        
                        st.pyplot(fig)
                        
                        # Display metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Forecast Method", results["method"])
                        with col2:
                            if results['mae']:
                                st.metric("MAE", f"{results['mae']:.2f}")
                            else:
                                st.metric("MAE", "N/A")
                        with col3:
                            if results['rmse']:
                                st.metric("RMSE", f"{results['rmse']:.2f}")
                            else:
                                st.metric("RMSE", "N/A")
                        with col4:
                            st.metric("CI Width", f"±{ci:.2f}" if ci else "N/A")
                        
                        # Forecast values table
                        with st.expander("📋 Forecast Values", expanded=False):
                            forecast_df = pd.DataFrame({
                                'Step': results['forecast_index'],
                                'Forecast': forecast,
                                'Lower Bound': np.array(forecast) - ci if ci else np.nan,
                                'Upper Bound': np.array(forecast) + ci if ci else np.nan
                            })
                            st.dataframe(forecast_df, use_container_width=True)
                            
                            csv = forecast_df.to_csv(index=False)
                            st.download_button("📥 Download Forecast CSV", csv, f"forecast_{results['target_col']}.csv", "text/csv")
                        
                        # Clear forecast button
                        if st.button("🗑️ Clear Forecast", key="clear_forecast"):
                            st.session_state.forecast_results = None
                            st.rerun()
                    
                    return st.session_state.forecast_results



        @st.fragment
        def render_data_normalisation_step():
            # Safety check
            if 'df' not in st.session_state or st.session_state.df is None:
                st.warning("No data available. Please complete the Input Data step first.")
                return
            
            st.dataframe(st.session_state.df.head())

            # Initialize normalization sub-steps if not present
            if 'normalization_sub_steps' not in st.session_state:
                st.session_state.normalization_sub_steps = [
                    "Min-Max Normalization", "Z-Score Normalization",
                    "Decimal Scaling", "Custom Normalization Rules"
                ]
            
            if 'normalization_checkbox_states' not in st.session_state:
                st.session_state.normalization_checkbox_states = {sub_step: False for sub_step in st.session_state.normalization_sub_steps}
            
            if 'normalization_active_order' not in st.session_state:
                st.session_state.normalization_active_order = []

            st.markdown("#### Data Normalisation Sub-Steps")
            st.markdown("Select the normalization operations to apply:")

            # Display checkboxes for sub-steps
            cols = st.columns(2)
            for i, sub_step in enumerate(st.session_state.normalization_sub_steps):
                with cols[i % 2]:
                    checked = st.checkbox(
                        sub_step,
                        value=st.session_state.normalization_checkbox_states.get(sub_step, False),
                        key=f"normalization_{sub_step}"
                    )
                    st.session_state.normalization_checkbox_states[sub_step] = checked
                    
                    if checked and sub_step not in st.session_state.normalization_active_order:
                        st.session_state.normalization_active_order.append(sub_step)
                        st.rerun()
                    elif not checked and sub_step in st.session_state.normalization_active_order:
                        st.session_state.normalization_active_order.remove(sub_step)
                        st.rerun()

            # Apply selected sub-steps
            if st.session_state.normalization_active_order:
                st.markdown("##### Applying Selected Normalization Operations")
                
                # Create a copy of the dataframe to work with
                working_df = st.session_state.df.copy()
                
                wrapper_normalise = WrapperDataNormalisationAgent()
                
                for sub_step in st.session_state.normalization_active_order:
                    st.markdown(f"**{sub_step}:**")
                    
                    # Call the appropriate sub-step fragment
                    if sub_step == "Min-Max Normalization":
                        working_df = render_minmax_normalization_substep(working_df)
                    elif sub_step == "Z-Score Normalization":
                        working_df = render_zscore_normalization_substep(working_df)
                    elif sub_step == "Decimal Scaling":
                        working_df = render_decimal_scaling_substep(working_df)
                    elif sub_step == "Custom Normalization Rules":
                        working_df = render_custom_normalization_substep(wrapper_normalise, working_df)
                    
                    st.markdown("---")
                
                # Update session state with processed dataframe
                st.session_state.df = working_df
                st.success("✅ Data Normalisation operations completed!")
                st.dataframe(st.session_state.df.head())
            else:
                st.info("Select at least one normalization sub-step to apply.")


        @st.fragment
        def render_minmax_normalization_substep(working_df):
            from sklearn.preprocessing import MinMaxScaler
            scaler = MinMaxScaler()
            numeric_cols = working_df.select_dtypes(include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                working_df[numeric_cols] = scaler.fit_transform(working_df[numeric_cols])
                st.success(f"Applied Min-Max normalization to {len(numeric_cols)} numeric columns")
            else:
                st.info("No numeric columns found for Min-Max normalization")
            return working_df


        @st.fragment
        def render_zscore_normalization_substep(working_df):
            from sklearn.preprocessing import StandardScaler
            scaler = StandardScaler()
            numeric_cols = working_df.select_dtypes(include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                working_df[numeric_cols] = scaler.fit_transform(working_df[numeric_cols])
                st.success(f"Applied Z-Score normalization to {len(numeric_cols)} numeric columns")
            else:
                st.info("No numeric columns found for Z-Score normalization")
            return working_df


        @st.fragment
        def render_decimal_scaling_substep(working_df):
            import math
            numeric_cols = working_df.select_dtypes(include=['float64', 'int64']).columns
            if len(numeric_cols) > 0:
                for col in numeric_cols:
                    max_abs = working_df[col].abs().max()
                    if max_abs > 0:
                        # Calculate decimal places needed
                        decimal_places = math.ceil(math.log10(max_abs))
                        working_df[col] = working_df[col] / (10 ** decimal_places)
                st.success(f"Applied decimal scaling to {len(numeric_cols)} numeric columns")
            else:
                st.info("No numeric columns found for decimal scaling")
            return working_df


        @st.fragment
        def render_custom_normalization_substep(wrapper_normalise, working_df):
            st.info("Custom normalization rules would be applied here")
            # This would use the wrapper_normalise.custom_normalization_rules if implemented
            return working_df




########################## End of @st.fragment definitions ##########################

        for step in st.session_state.active_steps_order: # the steps in the order they were checked and reflects the flow of the pipeline.
            #if st.session_state.checkbox_states.get(step, True):
            if st.session_state.checkbox_states.get(step, False):
                with st.container():
                    
                    #st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space
                    st.markdown("---")

                    st.markdown(f"<h3 style='font-size:20px; color: white;'>Output for: {step}</h3>", unsafe_allow_html=True)

                    
                    temp_df = st.session_state.df.copy() if st.session_state.df is not None else None
                    
                    st.session_state.temp_df = temp_df


                    st.markdown("---")

                    #####################################################################

                    ### Calling up the fragments for each main step (substeps within) ###

                    #####################################################################

                    if step == "Input Data":
                        render_input_data_step()

                    elif step == "Data Standardization with AI":
                        render_data_standardization_with_ai_step()

                    elif step == "Feature Engineering":
                        render_feature_engineering_step()

                    elif step == "Missing Data":
                        render_missing_data_step()

                    elif step == "Standardize Column Data":
                        render_standardize_column_data_step()

                    elif step == "Infection AI Standard Column Data":
                        render_inflection_ai_standard_feature_name_step()

                    elif step == "Human AI Standard Feature Values":
                        render_human_ai_standard_feature_values_step()

                    elif step == "Data Anomaly Evaluation":
                        render_data_anomaly_evaluation_step()

                    elif step == "Time Series Evaluation":
                        render_time_series_evaluation_step()

                    elif step == "Data Normalisation":
                        render_data_normalisation_step()


                    #####################################################################

                    ##################### Steps and substeps actions ####################

                    ##################################################################### 

                    
                    unique_key = f"step_activation_{step}_{str(uuid.uuid4())}"


                    # Checkbox to toggle step execution and display

                    container = st.empty()

                    # Render checkbox silently in the container
                    is_active = container.checkbox(
                        step,
                        value=st.session_state.statuses.get(step, 'idle') != 'idle',
                        key=unique_key,
                        help=f"Toggle {step} execution and output display"
                    )

                    

                    # Ensure statuses has the step key before accessing
                    if step not in st.session_state.statuses:
                        st.session_state.statuses[step] = 'idle'

                    # Update statuses based on checkbox state
                    if is_active:
                        if st.session_state.statuses[step] != 'completed':
                            st.session_state.statuses[step] = 'current'
                    else:
                        st.session_state.statuses[step] = 'idle'


                    # Clear the container so the widget disappears visually but the value remains in session_state
                    container.empty()


                    # Ensure radio_selections has an entry for each step, with a fallback to "Idle"
                    if step not in st.session_state.radio_selections:
                        st.session_state.radio_selections[step] = "Idle"

                    # Check if step has been permanently accepted
                    step_permanently_accepted = st.session_state.get(f"{step}_permanently_accepted", False)

                    if step_permanently_accepted:
                        # Show accepted state with option to unlock if needed
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.success(f"✅ {step} changes have been permanently accepted")
                        with col2:
                            if st.button("🔓 Unlock", key=f"unlock_{step}", help="Allow changes to this step again"):
                                # Ensure statuses has the step key before accessing
                                if step not in st.session_state.statuses:
                                    st.session_state.statuses[step] = 'idle'
                                st.session_state[f"{step}_permanently_accepted"] = False
                                st.session_state.radio_selections[step] = "Idle"
                                st.session_state.statuses[step] = "idle"
                                #st.rerun()
                    else:
                        # Radio button to choose the action for the step (Idle, Accept, Reject)
                        st.write("---")
                        action = st.radio(
                            f"Action for {step}  ",
                            options=["Idle", "Accept", "Reject"],
                            index=["Idle", "Accept", "Reject"].index(st.session_state.radio_selections[step]),
                            key=f"radio_{step}",
                            on_change=update_radio_selections_states,
                            args=(step,),
                            help=f"Toggle {step} Select 'Accept' to permanently accept changes, 'Reject' to revert, or leave as 'Idle'."
                        )

                        # If Accept is selected, make it permanent
                        if action == "Accept" and st.session_state.radio_selections[step] == "Accept":
                            if st.button(f"🔒 Permanently Accept {step} Changes", key=f"permanent_accept_{step}", type="primary"):
                                st.session_state[f"{step}_permanently_accepted"] = True
                                st.success(f"✅ {step} changes permanently accepted! Step is now locked.")
                                #st.rerun()
                    
                    # Update the pipeline visualization after accept/reject
                    pipeline_graph = pydeamlpipeline.display_horizontal_pipeline(st.session_state.steps, st.session_state.statuses)
                    components.html(pipeline_graph)

                    #components.html(pipeline_graph, height=250) # Match this to your graph size



    else:
        st.sidebar.info("Check box for Pipeline Options.")


################################################################### Pipeline Options ends here #########################################################

    # Timestamp generation function
    def generate_timestamp(format_choice):
        """Generate timestamp based on selected format"""
        now = datetime.now()
        if format_choice == "DDMMYY_HHMMSS":
            return now.strftime("%d%m%y_%H%M%S")
        #elif format_choice == "YYYYMMDD_HHMMSS":
        #    return now.strftime("%Y%m%d_%H%M%S")
        #elif format_choice == "YYYY-MM-DD_HH-MM-SS":
        #    return now.strftime("%Y-%m-%d_%H-%M-%S")
        #elif format_choice == "MMDDYY_HHMMSS":
        #    return now.strftime("%m%d%y_%H%M%S")
        else:  # UNIX timestamp
            return str(int(now.timestamp()))

    # Enhanced download section
    with st.sidebar:
        st.markdown("---")
        st.markdown("**Advanced Export**")
        
        # Available dataframes
        available_dfs = {}
        if st.session_state.get('df') is not None:
            available_dfs["Final DataFrame"] = st.session_state.df
        if st.session_state.get('cleaned_df') is not None:
            available_dfs["Preview DataFrame"] = st.session_state.cleaned_df
        if st.session_state.get('original_df') is not None:
            available_dfs["Original DataFrame"] = st.session_state.original_df
        if st.session_state.get('fixed_df') is not None:
            available_dfs["Fixed DataFrame"] = st.session_state.fixed_df

        if st.session_state.get('input_df') is not None:
            available_dfs["Input DataFrame"] = st.session_state.input_df

        if st.session_state.get("mod_fixed_df") not in st.session_state:
            st.session_state["mod_fixed_df"] = None


        if available_dfs:
            # Data selection
            selected_option = st.selectbox(
                "Select data to export:",
                options=list(available_dfs.keys()),
                help="Choose which dataframe version to download"
            )
            download_df = available_dfs[selected_option]
            
            # Format selection
            format_options = {
                "CSV": "📝", 
                "Excel": "📊", 
                "JSON": "🔤", 
                "XML": "📋", 
                "Parquet": "🗃️", 
                "HTML": "🌐",
                "Markdown": "📋"
            }
            
            selected_format = st.selectbox(
                "Export Format & Advanced Options:",
                options=list(format_options.keys()),
                help="Choose file format for download"
            )
            
            # Timestamp format optionshieven betetr
            timestamp_options = {
                "DDMMYY_HHMMSS": "151223_143022",
                #"YYYYMMDD_HHMMSS": "20231215_143022", 
                #"YYYY-MM-DD_HH-MM-SS": "2023-12-15_14-30-22",
                #"MMDDYY_HHMMSS": "121523_143022",
                "UNIX": "1702643422"
            }
            
            timestamp_format = st.radio(
                "Timestamp format:",
                options=list(timestamp_options.keys()),
                horizontal=True,
                help="Choose how the timestamp appears in filename"
            )
            
            # Export options
            with st.expander("Advanced Options"):
                col1, col2 = st.columns(2)
                with col1:
                    include_index = st.checkbox("Include index", False)
                    if selected_format == "CSV":
                        encoding = st.selectbox("Encoding", ["utf-8", "latin-1", "utf-16", 'ISO-8859-1', 'cp1252'])

                with col2:
                    if selected_format == "JSON":
                        json_orient = st.selectbox("JSON format", ["records", "split", "index", "values"])
            
            # Prefix filename input with smart default
            prefix_name_default = selected_option.split(' ')[-1].lower()  # Get last word (dataframe)
            prefix_filename = st.text_input(
                "Enter Prefix Filename:",
                value=prefix_name_default,
                help="The prefix name for your file (timestamp will be added automatically)"
            )
            
            # Generate final filename
            timestamp = generate_timestamp(timestamp_format)
            file_extension = selected_format.lower()
            final_filename = f"{prefix_filename}_{timestamp}.{file_extension}"
            
            st.success(f"📄 Will save as: `{final_filename}`")
            
            # Download logic
            try:
                if selected_format == "CSV":
                    data = download_df.to_csv(index=include_index, encoding=encoding)
                    mime_type = "text/csv"
                    
                elif selected_format == "Excel":
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        download_df.to_excel(writer, index=include_index, sheet_name='Data')
                    data = buffer.getvalue()
                    mime_type = "application/vnd.ms-excel"
                    
                elif selected_format == "JSON":
                    data = download_df.to_json(orient=json_orient, indent=2, index=include_index)
                    mime_type = "application/json"
                    
                elif selected_format == "XML":
                    data = download_df.to_xml(index=include_index)
                    mime_type = "application/xml"
                    
                elif selected_format == "Parquet":
                    buffer = io.BytesIO()
                    download_df.to_parquet(buffer, index=include_index)
                    data = buffer.getvalue()
                    mime_type = "application/octet-stream"
                    
                elif selected_format == "HTML":
                    data = download_df.to_html(index=include_index)
                    mime_type = "text/html"
                    
                elif selected_format == "Markdown":
                    data = download_df.to_markdown(index=include_index)
                    mime_type = "text/markdown"
                
                # Download button
                st.download_button(
                    label=f"{format_options[selected_format]} Download {selected_format}",
                    data=data,
                    file_name=final_filename,
                    mime=mime_type,
                    #use_container_width=True,
                    help=f"Download as {selected_format} format",
                    key=f"download_{selected_format}_{timestamp}"
                )
                
            except ImportError as e:
                st.error(f"⚠️ Missing dependency: {str(e)}")
                if "openpyxl" in str(e):
                    st.info("Install Excel support: `pip install openpyxl`")
                elif "pyarrow" in str(e):
                    st.info("Install Parquet support: `pip install pyarrow`")
            except Exception as e:
                st.error(f"❌ Export error: {str(e)}")
            
                
        else:
            st.info("📭 No dataframes for export")
            st.caption("Upload/preprocess data before exporting.")


    ##################################################################
    # Import enhanced domain pipeline manager
    from enhanced_pipeline_manager import add_enhanced_domain_pipeline_section
    
    # Add enhanced domain pipeline management to sidebar
    add_enhanced_domain_pipeline_section()
    
    # Rerun Pipeline on New Dataset Section
    st.markdown("---")
    st.markdown("##### **How to Apply Pipeline to New Dataset**")
    st.markdown("Apply your configured pipeline steps to a new dataset")
    
    st.info("💡 To use this feature:")
    st.markdown("""
    1. **First**, upload your main dataset using the file uploader in the sidebar
    2. **Configure** your pipeline steps in the Pipeline Options section
    3. **Accept** the steps you want to apply permanently
    4. **Then** use the main file uploader above to load a new dataset
    5. **Click** 'Apply Configured Pipeline to New Dataset' to process it
    """)
    
    # Button to apply pipeline to current uploaded dataset
    if st.button("🚀 Apply Configured Pipeline to Current Dataset", type="primary"):
        # Check if we have a dataset loaded
        if 'df' not in st.session_state or st.session_state.df is None:
            st.warning("⚠️ No dataset loaded. Please upload a file first using the file uploader in the sidebar.")
        else:
            # Apply the selected pipeline steps to the current dataset
            processed_df = st.session_state.df.copy()
            
            # Get the active steps that are permanently accepted
            active_steps = st.session_state.get('active_steps_order', [])
            permanently_accepted_steps = [
                step for step in active_steps 
                if st.session_state.get(f"{step}_permanently_accepted", False)
            ]
            
            if not permanently_accepted_steps:
                st.warning("⚠️ No pipeline steps have been permanently accepted. Please:")
                st.markdown("""
                1. Go to **Pipeline Options** in the sidebar
                2. Select the steps you want to apply
                3. Click **Accept** for each step
                4. Click **Permanently Accept** to lock them in
                """)
            else:
                st.markdown("### Applying Permanently Accepted Pipeline Steps...")
                st.info(f"📋 Will apply {len(permanently_accepted_steps)} permanently accepted steps: {', '.join(permanently_accepted_steps)}")
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_steps = len(permanently_accepted_steps)
                
                for i, step in enumerate(permanently_accepted_steps):
                    status_text.text(f"Processing: {step}")
                    
                    if step == "Input Data":
                        # For current dataset, this is already done
                        pass
                        
                    elif step == "Data Standardization with AI":
                        # Apply data standardization
                        try:
                            standardized_df = crewailpydeamlpipeline.perform_data_standardization(processed_df)
                            processed_df = standardized_df


                            data_standardizer = DataStandardizerAI(use_ai=True)
                            
                            # Get all columns for standardization
                            all_columns = df.columns.tolist()
                            
                            # Standardize all columns
                            standardized_df = data_standardizer.auto_standardize_dataframe(df, all_columns)
                            
                            st.success("✅ Data standardization complete!")

                        except Exception as e:
                            st.error(f"Error in Data Standardization: {str(e)}")
                            
                    elif step == "Feature Engineering":
                        # Apply selected feature engineering sub-steps
                        fe_active = st.session_state.get('fe_active_order', [])
                        if fe_active:
                            data_agent = DataReadAgent()
                            for sub_step in fe_active:
                                if sub_step == "Remove Duplicates":
                                    if processed_df.duplicated().any():
                                        processed_df = processed_df.drop_duplicates()
                                elif sub_step == "Assign Data Types":
                                    processed_df = processed_df.convert_dtypes()
                                elif sub_step == "Create New Feature":
                                    feature_name = st.session_state.get(f'fe_feature_name_{sub_step}', '')
                                    feature_expr = st.session_state.get(f'fe_feature_expr_{sub_step}', '')
                                    if feature_name and feature_expr:
                                        try:
                                            processed_df[feature_name] = processed_df.eval(feature_expr)
                                        except:
                                            pass  # Skip if error
                                elif sub_step == "Subset Dataframe":
                                    subset_condition = st.session_state.get(f'fe_subset_rows_{sub_step}', '')
                                    if subset_condition:
                                        try:
                                            processed_df = processed_df.query(subset_condition)
                                        except:
                                            pass
                                elif sub_step == "Delete Records":
                                    delete_condition = st.session_state.get(f'fe_delete_condition_{sub_step}', '')
                                    if delete_condition:
                                        try:
                                            processed_df = processed_df.query(f"not ({delete_condition})")
                                        except:
                                            pass
                                elif sub_step == "Delete Columns":
                                    cols_to_delete = st.session_state.get(f'fe_delete_cols_{sub_step}', [])
                                    if cols_to_delete:
                                        processed_df = processed_df.drop(columns=[col for col in cols_to_delete if col in processed_df.columns])

                            
                    elif step == "Missing Data":
                        # Apply missing data handling
                        try:
                            missing_agent = WrapperMissingDataAgent()
                            processed_df = missing_agent.fill_missing_data(processed_df)
                        except Exception as e:
                            st.error(f"Error in Missing Data handling: {str(e)}")
                            
                    elif step == "Standardize Column Data":
                        # Apply column standardization
                        try:
                            col_std_agent = ColumnStandardizationAgent()
                            processed_df = col_std_agent.standardize_columns(processed_df)
                        except Exception as e:
                            st.error(f"Error in Column Standardization: {str(e)}")
                            
                    elif step == "Inflection AI Standard Feature Name":
                        # Apply inflection standardization
                        try:
                            inflection_agent = HumanDataStandardizationAgent()
                            processed_df = inflection_agent.standardize_feature_names(processed_df)
                        except Exception as e:
                            st.error(f"Error in Inflection Standardization: {str(e)}")
                            
                    elif step == "Human AI Standard Feature Values":
                        # Apply human AI standardization
                        try:
                            human_std_agent = APIAgent()
                            processed_df = human_std_agent.standardize_values(processed_df)
                        except Exception as e:
                            st.error(f"Error in Human AI Standardization: {str(e)}")


                    elif step == "Time Series Evaluation":
                        # Apply timeseries evaluation
                        try:
                            timeseries_agent = get_timeseries_wrapper()
                            processed_df = timeseries_agent.get_current_dataframe(processed_df)
                        except Exception as e:
                            st.error(f"Error in Time Series Evaluation: {str(e)}")


                    elif step == "Data Anomaly Evaluation":
                        # Apply anomaly detection
                        try:
                            anomaly_agent = WrapperDataAnomalyAgent()
                            processed_df = anomaly_agent.detect_anomalies(processed_df)
                        except Exception as e:
                            st.error(f"Error in Anomaly Evaluation: {str(e)}")
                            
                    elif step == "Data Normalisation":
                        # Apply data normalization
                        try:
                            norm_agent = WrapperDataNormalisationAgent()
                            processed_df = norm_agent.normalize_data(processed_df)
                        except Exception as e:
                            st.error(f"Error in Data Normalisation: {str(e)}")
                    
                    progress_bar.progress((i + 1) / total_steps)
                
                progress_bar.empty()
                status_text.empty()
                
                # Store the processed result
                st.session_state.new_processed_df = processed_df
                
                st.success("✅ Pipeline applied successfully to dataset!")
                st.write(f"Processed dataset shape: {processed_df.shape[0]} rows × {processed_df.shape[1]} columns")
                st.dataframe(processed_df.head())
                
                # Download button for processed dataset
                csv_data = processed_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Processed Dataset",
                    data=csv_data,
                    file_name="processed_dataset.csv",
                    mime="text/csv",
                    key="download_processed"
                )


##################################################################



if __name__ == "__main__":
    main()
    #crew_ai = CrewAIPyDEAML()
