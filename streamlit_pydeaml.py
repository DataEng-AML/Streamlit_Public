from typing import Literal

import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
import plotly.express as px
########################################################################################################
# dqllm\Scripts\activate

# Airlow Section
#from airflow.api.client.local_client import Client #as airflow_client
from airflow.models import DagBag
from airflow.configuration import conf
from airflow.utils.dates import days_ago
from airflow.utils.state import State
#import os
import requests
from requests.auth import HTTPBasicAuth


########################################################################################################


# pip install langchain
from langchain.prompts import PromptTemplate

# pip install langchain_experimental
#from langchain_experimental.agents import create_csv_agent
from langchain_experimental.agents.agent_toolkits import create_csv_agent
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain.agents import AgentType, AgentExecutor, initialize_agent
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
import streamlit as st
import streamlit.components.v1 as components


import pandas as pd

# pip install chardet
import chardet


# other things to install
# pip install tabulate

# streamlit extras
from streamlit_extras.dataframe_explorer import dataframe_explorer

from IPython import display
import numpy as np

from sklearn.impute import KNNImputer
from sklearn.linear_model import LinearRegression

from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer

import io
import sys  # Import sys module
import calendar

import re
#import copy

import seaborn as sns
import matplotlib.pyplot as plt

# Custom classes to handle individual LADE needs
from phd_custom_lib.perform_eda import perform_eda
from phd_custom_lib.resolve_missing_data import resolve_missing_data
#from phd_custom_lib.dedup_data_operations import dedup_data_operations


import graphviz
import plotly.express as px
import missingno as msno # for missing data
import math

########################################################################################################
# The main project work and libraries to import 
from data_read_agent import DataReadAgent, DataFrameOutput
from missing_data_agent import MissingDataAgent


from human_data_standardization_agent import HumanDataStandardizationAgent# as hdsa
from column_standardization_agent import ColumnStandardizationAgent
#from .correct_clean_api_agent import CleanAPIAgent as ccapi

from pyDEAML import PyDEAML
#pydeaml = PyDEAML.DataReadAgent
import tempfile



######################################

# The pipeline codes here

import streamlit as st
import pandas as pd
import graphviz
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import numpy as np
import time
from graphviz import Digraph

from typing import Literal

from pyvis.network import Network
import streamlit.components.v1 as components
from streamlit.components.v1 import html

import uuid










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

class PipelineCrewAIPyDEAML:
    def __init__(self):
        self.log = []
        self.data = None

    def detect_missing_data(self, data):
        if 'data' in st.session_state:
            data = st.session_state.data
        
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()
        
        # Call the function
        st.session_state.missing_data_agent.detect_missing_data(data)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Missing Data Report")
        st.dataframe(st.session_state.missing_data_agent.missing_data_report)
        
        return st.session_state.missing_data_agent.cleaned_missing_data
                       

    def display_missing_data(self, data, feature_names):
        if 'data' in st.session_state:
            data = st.session_state.data
        
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()

     
        # Call the function
        dis_mis_data = st.session_state.missing_data_agent.display_missing_data(data, feature_names)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Display Missing Data")
        #st.dataframe(st.session_state.missing_data_agent.display_missing_data)
        
        return dis_mis_data

    def fix_missing_data(self, data):
        if 'data' in st.session_state:
            data = st.session_state.data

        # Store the original DataFrame when a new file is uploaded
        if 'original_data' not in st.session_state or not st.session_state.original_data.equals(data):
            st.session_state.original_data = data.copy(deep=True)

        cleaned_missing_data = data.copy(deep=True)

        # Get user input for the value to replace missing data
        process = st.text_input("Enter the value to populate all missing data, e.g., NoData:", key="missing_data_input")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Replace Missing Values", key="replace_missing_values_button"):
                if process:
                    # Iterate over each column to fill missing values
                    for column in cleaned_missing_data.columns:
                        if pd.api.types.is_datetime64_any_dtype(cleaned_missing_data[column]):
                            cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)
                        else:
                            cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)

                    st.success(f"Missing values in the dataframe replaced with '{process}'")
                    st.write("Updated DataFrame:")
                    st.dataframe(cleaned_missing_data.head())

                    # Update the session state with the cleaned dataframe
                    st.session_state.data = cleaned_missing_data
                    return cleaned_missing_data
                else:
                    st.warning("Please enter a value to replace missing data.")
        
        with col2:
            if st.button("Revert Changes", key="revert_changes_button"):
                if 'original_data' in st.session_state:
                    st.success("Changes reverted. Original DataFrame restored.")
                    st.write("Original DataFrame:")
                    st.dataframe(st.session_state.original_data.head())
                    # Update the session state with the original dataframe
                    st.session_state.data = st.session_state.original_data.copy(deep=True)
                    return st.session_state.original_data
                else:
                    st.warning("No changes to revert.")

        return None



# Column standardization
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()


    def detect_missing_data(self, data):
        if 'data' in st.session_state:
            data = st.session_state.data
        
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()
        
        # Call the function
        st.session_state.missing_data_agent.detect_missing_data(data)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Missing Data Report")
        st.dataframe(st.session_state.missing_data_agent.missing_data_report)
        
        return st.session_state.missing_data_agent.cleaned_missing_data


    def standardize_column(self, data, column, reference_column, prompt_template):
        if 'data' in st.session_state:
            data = st.session_state.data
        if 'column' in st.session_state:
            column = st.session_state.column
        if 'reference_column' in st.session_state:
            data = st.session_state.reference_column
        if 'prompt_template' in st.session_state:
            data = st.session_state.prompt_template

            
        # ColumnStandardizationAgent instance from session state
        if 'column_standardization_agent' not in st.session_state:
            st.session_state.column_standardization_agent = ColumnStandardizationAgent()

        # Call the function
        ai_standardize_column = st.session_state.column_standardization_agent.standardize_column(self, data, column, reference_column, prompt_template)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Standardize Column")
        #st.dataframe(st.session_state.column_standardization_agent.standardize_column)
        
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

        # ColumnStandardizationAgent instance from session state
        if 'column_standardization_agent' not in st.session_state:
            st.session_state.column_standardization_agent = ColumnStandardizationAgent()

        # Call the function
        ai_standardize_missing_data = st.session_state.column_standardization_agent.standardize_missing_data(data, column, reference_column, prompt_template)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Standardize Missing Column Data")
        #st.dataframe(st.session_state.column_standardization_agent.standardize_column)
        
        return ai_standardize_missing_data



# from human_data_standardization_agent import HumanDataStandardizationAgent as hdsa

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
                
                        # # Initialize the agent only once and save it to session state
                        # if 'human_data_standardization_agent' not in st.session_state:
                            # st.session_state.human_data_standardization_agent = HumanDataStandardizationAgent()

                        # agent = st.session_state.human_data_standardization_agent

                        # # Initialize session state variables if they don't exist
                        # inflection_method = 'underscore'
                        # #if 'prompt_template' not in st.session_state:
                            # #st.session_state.prompt_template = ''
                        # if 'standardize_triggered' not in st.session_state:
                            # st.session_state.standardize_triggered = False

                        # # Dropdown for inflection method (widget directly updates st.session_state)
                        # st.selectbox(
                            # "Select the inflection method", 
                            # ['underscore', 'camelize', 'dasherize', 'humanize', 
                             # 'titleize', 'pluralize', 'singularize', 'parameterize'],
                            # index=['underscore', 'camelize', 'dasherize', 'humanize', 
                                   # 'titleize', 'pluralize', 'singularize', 'parameterize'].index(inflection_method),
                            # key='inflection_method'  # Important to link to session state
                        # )
                        
                        # # Text input for AI prompt template (widget directly updates st.session_state)
                        # st.text_input(
                            # "Enter the AI prompt template (use '{term}' as a placeholder for feature names)"
                        # )
                        
                        # # Button to trigger processing
                        # if st.button("Standardize Feature Names"):
                            # st.session_state.standardize_triggered = True

                        # # Process only if the button was clicked
                        # if st.session_state.standardize_triggered:
                            # inf_ai_stnd_df = agent.inflection_ai_standard_feature_name(st.session_state.df)
                            
                            # # Save the resulting DataFrame in session state for later use if needed
                            # st.session_state.inf_ai_stnd_df = inf_ai_stnd_df
                            
                            # # Display the result
                            # st.dataframe(inf_ai_stnd_df)

                            # # Reset the trigger
                            # st.session_state.standardize_triggered = False


        # Call the function
        #inflxn_ai_sfn = st.session_state.human_data_standardization_agent.inflection_ai_standard_feature_name(data)
        
        # Display the Standardized Features report as a Streamlit dataframe
        #st.markdown("##### Standardized Features:")
        #st.dataframe(inflxn_ai_sfn)
        
        #return inflxn_ai_sfn




class PyDEAMLPipeline:
    def __init__(self):
        self.log = []
        self.data = None
        # Initialize the network object for Pyvis
        self.net = Network(height="200px", width="100%", directed=True)
        self.net.set_options('''
        var options = {
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
          }
        }
        ''')


    def _add_node(self, step, statuses):
        status = statuses.get(step, 'idle')
        
        if status == 'active':
            color = 'lightblue'
        elif status == 'completed':
            color = 'lightgreen'
        elif status == 'failed':
            color = 'lightcoral'
        else:
            color = 'lightgray'

        self.net.add_node(step, label=step, color=color, shape='box', title=step)




    def display_horizontal_pipeline(self, steps, statuses):
        for step in steps:
            self._add_node(step, statuses)

        for i in range(len(steps) - 1):
            self.net.add_edge(steps[i], steps[i + 1])

    def generate_html(self):
        html = self.net.generate_html()
        return html
  
        
    def clean_data(self, data):
        st.write("Cleaning Data: Removing duplicates.")
        data = data.drop_duplicates()
        return data

    def perform_feature_engineering(self, data):
        st.write("Feature Engineering: Adding new features.")
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            new_col_name = f"{col}_Squared"
            data[new_col_name] = data[col] ** 2
        return data

    def normalize_data(self, data):
        st.write("Normalizing Data: Standardizing numerical features.")
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            data[col] = (data[col] - data[col].mean()) / data[col].std()
        return data

    def split_data(self, data):
        st.write("Splitting Data: Preparing train/test split.")
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
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



class OutputCapture(list):
    def __init__(self):
        self._stdout = sys.stdout  # Keep the original stdout
        sys.stdout = self  # Redirect stdout to this object

    def write(self, data):
        self.append(data)  # Append printed data into the list

    def flush(self):
        pass  # Required for compatibility, no-op here

    def get_output(self):
        return ''.join(self)  # Join list items into a single string


# Class to handle data operations
class PyLade:
    def __init__(self):
        self.log = []
        self.df = None

    # Function to create a pandas DataFrame agent

    #def generate_create_csv(self, file):
    #    with open("wip.csv", "wb") as f:    # wip file stored temporarily for use
    #        f.write(file.getvalue())
        
    #    create_csv = create_csv_agent(OpenAI(temperature=0), "wip.csv", verbose=True, allow_dangerous_code=True)
    #    return create_csv

    def generate_create_csv(self,uploaded_file):
        # Ensure that 'uploaded_file' is the file-like object, not just the file name string
        if hasattr(uploaded_file, 'getvalue'):
            file_content = uploaded_file.getvalue()  # This works correctly for file-like objects
            with open('wip.csv', 'wb') as f:  # Assuming you want to write as binary
                f.write(file_content)
            #st.write("File has been saved as 'wip.csv'")
        else:
            st.error("The file is not in the expected format.")
            
        create_csv = create_csv_agent(OpenAI(temperature=0), "wip.csv", verbose=True, allow_dangerous_code=True)
        return create_csv          
 

    def load_data(self, uploaded_file):
        # Check if a file is uploaded
        if uploaded_file is None:
            st.error("No file uploaded. Please upload a file to continue.")
            return None  # or handle accordingly

        try:
            # Process and load data
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_file_path = temp_file.name
            
            # Assuming you have a data agent to handle the file reading
            data_agent = DataReadAgent()
            read_input_file = data_agent.read_dir_file(temp_file_path)
            
            if isinstance(read_input_file, DataFrameOutput):
                if 'df' in st.session_state:
                    df = read_input_file.data
                    #st.session_state.df = df  # Store in session state for later use
                    st.write("Data loaded and DataFrame generated successfully:")
                    #st.dataframe(df.head())
                    #st.session_state.df = df 
                    #st.session_state.df.head()
            
            os.remove(temp_file_path)  # Clean up temporary file
            #st.dataframe(df.head())
            return df

        except Exception as e:
            st.error(f"An error occurred while processing the file: {str(e)}")
            return None

    def preprocess_data(self,df):
        #df = crew_ai.llm_explore()  # this provide a dataframe with named features, proper datatypes and other references required for LLM exploration
        if 'df' in st.session_state:
            df = st.session_state.df
                    
            try:
                data_agent = DataReadAgent()
                
                # Radio button to select operation
                selected_preprocess = st.radio(
                    "Select Preprocessing:",
                    ["Initial Statistics", "Add Headers", "Assign Data Types", "Create New Feature", "Rename Features", "View Interval Records", "Delete Records"],#, "Reset Last Operation"]
                    horizontal = True

                )

                # Perform action based on selected operation
                if selected_preprocess == "Initial Statistics":
                    # Option to check initial general statistics of the data / DataFrame
                    num_df_col = len(st.session_state.df.columns)
                    num_df_row = len(st.session_state.df)

                    #st.write(f"Head of the dataframe with {num_df_col} columns and {num_df_row} rows") 
                    st.markdown(f"Head of the dataframe with **:green[{num_df_col} columns]** and **:green[{num_df_row} rows]**")

                    st.dataframe(st.session_state.df.head())  # Display the updated DataFrame


                    # Generate the statistics of the dataframe
                    self.df_statistics()
                         
                # Option to add headers to the DataFrame
                if selected_preprocess == "Add Headers":
                    st.markdown("<hr style='margin: 1px 0;'>", unsafe_allow_html=True)                    
                    st.markdown("This is only for dataframes where a record is the header!<br>It pushes that recorde into index 0, and creates headers with user entries:", unsafe_allow_html=True)
                    #st.write("This is only for dataframes without headers, else use Rename Features")
                    #st.markdown("<p style='color: red;'>This is only for dataframes without headers, else use Rename Features:</p>", unsafe_allow_html=True)
                    #st.markdown(""" <p style='color: red; text-shadow: 1px 1px 1px white;'>This is only for dataframes without headers, else use Rename Features:</p>""", unsafe_allow_html=True)
                    st.markdown("<hr style='margin: 1px 0;'>", unsafe_allow_html=True)
                    st.dataframe(st.session_state.df.head())
                    new_headers = st.text_input("Enter new headers, separate with comma with no space:")
                    if new_headers:
                        headers = new_headers.split(",")  # Split input into list of headers
                        df = data_agent.add_headers_to_df(df, headers)  # Replace df with updated version
                        df.columns = headers
                        st.session_state.df = df
                        st.write("Updated DataFrame with new headers:")
                        df = st.session_state.df
                        st.dataframe(df.head())  # Display the updated DataFrame]
                        
                        #crew_ai.df_statistics()
                
                # Option to assign data types to columns
                if selected_preprocess == "Assign Data Types":
                    st.dataframe(df.head()) 
                    column_names = df.columns.tolist()
                    column_dtypes = {}
                    for col in column_names:
                        new_dtype = st.selectbox(f"Select data type for '{col}'", 
                                                 ['float64', 'int64', 'object', 'bool', 'datetime64[ns]'], 
                                                 index=0)  # Default to 'float64'
                        column_dtypes[col] = new_dtype
                    
                    if st.button("Apply Data Types"):
                        df = data_agent.assign_features_dtypes(df, column_dtypes)  # Replace df with updated version
                        st.write("Data types applied successfully:")
                        st.session_state.df = df
                        df = st.session_state.df
                        st.dataframe(df.head())  # Display the updated DataFrame]

                # Option to create a new feature
                if selected_preprocess == "Create New Feature":
                    st.dataframe(st.session_state.df.head())
                    new_feature_name = st.text_input("New feature name:")
                    feature_expression = st.text_input("Python expression for new feature (use column names):")

                    if new_feature_name and feature_expression:
                        try:
                            # Ensure the column is numeric before performing the operation
                            if 'value' in df.columns:  # Make sure 'value' exists
                                df['value'] = pd.to_numeric(df['value'], errors='coerce')  # Convert to numeric, replace invalid values with NaN

                            # Now create the new feature using the expression
                            df[new_feature_name] = df.eval(feature_expression)
                            st.success(f"New feature '{new_feature_name}' created successfully!")
                            st.session_state.df = df
                            st.write("Updated DataFrame with newly created feature(s):")
                            df = st.session_state.df
                            st.dataframe(df.head())  # Display the updated DataFrame]
                                                                                                 
                        except Exception as e:
                            st.error(f"Error occurred while generating the chart: {str(e)}")
 

                # Option to rename features
                if selected_preprocess == "Rename Features":
                    st.dataframe(st.session_state.df.head())
                    # Allow column selection via dropdown or search
                    selected_column = st.selectbox(
                        "Select a column to rename",
                        options=st.session_state.df.columns,
                        index=0,
                        key="column_selector"
                    )

                    # Input for new feature name with a unique key
                    new_feature_name = st.text_input("New feature name:", key="new_feature_name_input")

                    # Check if the new name already exists
                    if new_feature_name in st.session_state.df.columns:
                        st.warning(f"The name '{new_feature_name}' already exists. Please choose a different name.")
                    elif new_feature_name and st.button("Rename Column"):
                        df = st.session_state.df.rename(columns={selected_column: new_feature_name})
                        st.success(f"Column '{selected_column}' renamed to '{new_feature_name}'")
                        st.session_state.df = df
                        st.write("Updated DataFrame with renamed feature:")
                        st.dataframe(df.head())
                        
                        

                if selected_preprocess == "View Interval Records":
                    
                    st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)     
                    # Create three columns for horizontal display
                    col1, col2 = st.columns(2)

                    # Display the statistics in the respective columns
                    with col1:                           
                        start_1 = st.text_input("Input start index for first range")
                        start_2 = st.text_input("Input start index for second range")

                    with col2:   
                        end_1 = st.text_input("Input end index for first range")
                        end_2 = st.text_input("Input end index for second range")

                    # Convert inputs to integers and display the corresponding DataFrame slice
                    try:                
                        start_1 = int(start_1)
                        end_1 = int(end_1)
                        start_2 = int(start_2)
                        end_2 = int(end_2)
                        
                        # Ensure that the input indices are valid
                        if start_1 >= 0 and end_1 < len(df) and start_1 <= end_1:
                            st.write(f"Displaying records from index {start_1} to {end_1}")
                            st.dataframe(df.iloc[start_1:end_1+1])
                        else:
                            st.write("Invalid range for the first set of indices.")

                        if start_2 >= 0 and end_2 < len(df) and start_2 <= end_2:
                            st.write(f"Displaying records from index {start_2} to {end_2}")
                            st.dataframe(df.iloc[start_2:end_2+1])
                        else:
                            st.write("Invalid range for the second set of indices.")
                            
                    except ValueError:
                        st.write("Please enter valid integer indices.")
                            
                            
                            
                if selected_preprocess == "Delete Records":
                    # Allow user to input indices to delete
                    indices_to_delete = st.text_input("Enter indices to delete (comma-separated):")

                    if indices_to_delete:
                        try:
                            # Convert input string to a list of integers
                            indices = [int(idx.strip()) for idx in indices_to_delete.split(',')]
                            
                            # Delete rows by index
                            df = df.drop(indices)
                            
                            # Reset the index
                            df.reset_index(drop=True, inplace=True)
                            
                            st.success(f"Deleted rows with indices: {indices} and reset the index.")
                            st.session_state.df = df
                            st.write("Updated DataFrame after deleting records:")
                            #df = st.session_state.df
                            st.dataframe(df.head())  # Display the updated DataFrame]
                        except ValueError:
                            st.error("Please enter valid integer indices separated by commas.")
                        except KeyError as e:
                            st.error(f"Index {e} not found in the DataFrame.")


            except Exception as e:
                st.error(f"Error occurred while generating the chart: {str(e)}")
               
                
        return df
    
     # using the create_csv_agent of Langstane LLM, converting df to csv into memory
    def df_csv_io(self,df):
        # Use BytesIO to create a file-like object in memory
        csv_in_memory = io.BytesIO()  # Use BytesIO for binary data (use StringIO for text-based CSVs)
        df.to_csv(csv_in_memory, index=False)  # df to the buffer
        csv_in_memory.getvalue()
        return csv_in_memory   
    
    def feature_engineering(self):
        if 'df' in st.session_state:
            updated_df = st.session_state.df.copy()
            # Feature engineering logic
            #st.write(updated_df.head())
            return updated_df
        else:
            st.error("DataFrame not available.")
            return None


    def llm_explore(self):
        if 'df' in st.session_state:
            df = st.session_state.df
            # Perform LLM exploration using df
            #st.write(df.head())
        else:
            st.error("DataFrame not available.")
        return df
        

    def df_statistics(self):
        if 'df' in st.session_state:
            df = st.session_state.df
                
            # Create three columns for horizontal display
            col1, col2, col3 = st.columns(3)

            # Display the statistics in the respective columns
            #with col1:
             #   st.text("Shape")
             #   st.write(st.session_state.df.shape)

            with col1:
                st.text("Columns and Data Types")
                grp_features = df.dtypes.groupby(df.dtypes.values).groups

                data_dict = {dtype: pd.Series(f) for dtype, f in grp_features.items()}
                feature_df = pd.DataFrame(data_dict)
                feature_df = feature_df.fillna('')
                st.dataframe(feature_df, use_container_width=False, hide_index=True)
                        
            with col2:
                st.text("Summary Statistics")
                st.write(st.session_state.df.describe())

            with col3:
                st.text("Missing Value Percentage")
                st.write((st.session_state.df.isnull().mean() * 100).round(2))
                
                
            col5, col6, col7 = st.columns(3)
              
            with col5:
                st.text("Missing Value Percentage")
                missing_data = st.session_state.df.isnull().sum().reset_index()
                missing_data.columns = ['Column', 'Missing Values']
                missing_data['Percentage'] = (missing_data['Missing Values'] / len(st.session_state.df)) * 100

                fig = px.bar(missing_data, x='Column', y='Percentage', 
                             labels={'Percentage': 'Missing Values (%)'})
                fig.update_layout(
                    xaxis={'categoryorder':'total descending', 'tickangle': -45},
                    margin=dict(b=100)  # Increase bottom margin to prevent label cutoff
                )
                fig.update_traces(texttemplate='%{y:.1f}', textposition='outside')
                st.plotly_chart(fig, width='stretch')


            with col6:
                st.text("Unique Values")
                unq = st.session_state.df.nunique()                      
                fig = px.bar(
                    x=unq.index.astype(str),
                    y=unq.values,
                    text=unq.values,
                    labels={'x': 'Feature Names', 'y': 'Unique Value Count'}
                )
                fig.update_traces(texttemplate='%{text}', textposition='outside')

                fig.update_layout(
                    xaxis=dict(
                        tickangle=315,
                        tickmode='array',
                        tickvals=list(range(len(unq.index))),
                        ticktext=unq.index.astype(str)
                    )
                )

                st.plotly_chart(fig)

                
             
             # Additional stats can be added below or in more columns
            col9, col10, col11, col12 = st.columns(4)


            with col9:
                st.text("Data Types Breakdown")
                data_type_count = st.session_state.df.dtypes.value_counts()
                st.write(data_type_count)   

            numerical_df = st.session_state.df.select_dtypes(include=['number']) 
            with col10:
                st.text("Standard Deviation")
                std_dev = numerical_df.std()
                fig = px.bar(x=std_dev.index, y=std_dev.values, title="Standard Deviation by Numerical Column")
                st.plotly_chart(fig)

            with col11:
                st.text("Skewness")
                st.write(numerical_df.skew())

            with col12:
                st.text("Kurtosis")
                st.write(numerical_df.kurtosis())

            # Additional stats can be added below or in more columns
            col13, col14, col15 = st.columns(3)
            
            with col13:
                st.text("Min/Max Values")
                min_vals = numerical_df.min()
                max_vals = numerical_df.max()
                min_max_df = pd.DataFrame({'Min': min_vals, 'Max': max_vals})
                st.write(min_max_df)
                
                
            cal_df = st.session_state.df.select_dtypes(include=['number']) 
            with col14:
                st.text("Standard Deviation")
                st.write(cal_df.std())
                # Standard Deviation
                st.text("Standard Deviation")
                std_dev = cal_df.std()
                fig = px.bar(x=std_dev.index, y=std_dev.values, title="Standard Deviation by Numerical Column")
                st.plotly_chart(fig)
 
                       
                
        else:
            st.error("DataFrame not available.")


    def fix_duplicate_data(self, df):
        print("Fix Duplicate Data")
        self.log.append("Applied 'Fix Duplicate Data'")
        dedup_data = df.drop_duplicates()
        print(dedup_data.head(20))
        return dedup_data 

    def accuracy_dedup_data(self, df):
        print("Accuracy Deduplicated Data")
        nrows_original = len(df)
        nrows_dedup_data = len(self.fix_duplicate_data(df))
        nrows_difference = nrows_original - nrows_dedup_data

        if nrows_difference == 0:
            accuracy_dedup_data = 1
        else:
            accuracy_dedup_data = nrows_difference / nrows_original

        print(accuracy_dedup_data.head(20))
        return accuracy_dedup_data

    def accuracy_missing_data(self, df):
        print("Missing Data Accuracy:")
        value_count = df.count().sum()
        element_count = df.size
        ratio_missing_data = (element_count - value_count) / element_count
        accuracy_missing_data = 1 - ratio_missing_data
        print(accuracy_missing_data)
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

        print(predict_missing_data_imputed.head(20))
        return predict_missing_data_imputed
    


    def fix_missing_data(self, df, process='replace'):
        print("Fix Missing Data:")
        self.log.append(f"Applied 'Fix Missing Data' using method '{process}'")
        fixed_missing_data = df.copy()
        for column in fixed_missing_data.columns:
            if fixed_missing_data[column].isnull().all():
                fixed_missing_data.drop(column, axis=1, inplace=True)
            else:
                if process == 'replace' and pd.api.types.is_numeric_dtype(fixed_missing_data[column]):
                    fixed_missing_data[column].fillna(9999, inplace=True)
                elif process == 'replace' and pd.api.types.is_object_dtype(fixed_missing_data[column]):
                    fixed_missing_data[column].fillna('NA', inplace=True)
                elif process in ['mean', 'median', 'mode'] and pd.api.types.is_numeric_dtype(fixed_missing_data[column]):
                    fixed_missing_data[column].fillna(
                        fixed_missing_data[column].mean() if process == 'mean' else (
                            fixed_missing_data[column].median() if process == 'median' else fixed_missing_data[column].mode().iloc[0]), inplace=True)
                elif process in ['ffill', 'bfill']:
                    fixed_missing_data[column].fillna(method=process, inplace=True)
                elif process == 'interpolate' and pd.api.types.is_numeric_dtype(fixed_missing_data[column]):
                    fixed_missing_data[column].interpolate(inplace=True)
                #elif process == 'replace with predicted values':
                #    fixed_missing_data = self.predict_missing_data(fixed_missing_data, 1)
                    
        
        print(fixed_missing_data.head(20))
        return fixed_missing_data

    def fix_unstandardized_data(self, df):
        print("Fix Unstandardized Data:")
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

        print(standardized_data)
        return standardized_data

    def accuracy_standardized_data(self, df):
        print("Accuracy Unstandardized Data")
        standardized_data = self.fix_unstandardized_data(df)
        orig_standard_data = (df != standardized_data).astype(int)
        total_number_of_values_X = df.size

        ratio_δi_X = orig_standard_data.sum().sum() / total_number_of_values_X
        accuracy_df = 1 - ratio_δi_X

        print(accuracy_df.head(20))
        return accuracy_df

    def total_accuracy(self, df):
        print("Total Accuracy")
        sumAccuracy = (
            self.accuracy_missing_data(df) +
            self.accuracy_standardized_data(df) +
            self.accuracy_dedup_data(df)
        )
        num_transformation = 3  # increase this number if you add more transformations
        aveAccuracy = sumAccuracy / num_transformation
        print(aveAccuracy)
        return aveAccuracy


#################################################################################
#################################################################################

################################# PyDEAML Agents #################################

#################################################################################
#################################################################################


class CrewAIPyDEAML:
    def __init__(self):
        self.log = []
        self.df = None
    '''
    def missing_data_agent(self,df):
        if 'df' in st.session_state:
            df = st.session_state.df
                    
            # Option to add headers to the DataFrame
            #if st.sidebar.checkbox("Detect Missing Data"):
                
            """Detects missing data and generates a report."""
            original_missing_data = df.copy()
            cleaned_missing_data = original_missing_data.copy()
            
            total_rows = len(original_missing_data)

            missing_count = original_missing_data.isnull().sum()[original_missing_data.isnull().sum() > 0]
            missing_percentage = ((missing_count / total_rows) * 100).round(2)

            missing_data_report = pd.DataFrame({
                'Column': missing_count.index,
                'Number of Missing Data': missing_count.values,
                '% of Missing Data': missing_percentage.values
            })

            #st.subheader("Missing Data Report")
            #st.markdown("##### Missing Data Report")
            st.write("**Columns with missing data**")
            st.dataframe(missing_data_report)

            return original_missing_data, cleaned_missing_data, missing_data_report                   
        '''
 
 
    def detect_missing_data(self, df):
        if 'df' in st.session_state:
            df = st.session_state.df
        
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()
        
        # Call the function
        st.session_state.missing_data_agent.detect_missing_data(df)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Missing Data Report")
        st.dataframe(st.session_state.missing_data_agent.missing_data_report)
        
        return st.session_state.missing_data_agent.cleaned_missing_data
                       

    def display_missing_data(self, df, feature_names):
        if 'df' in st.session_state:
            df = st.session_state.df
        
        # MissingDataAgent instance from session state
        if 'missing_data_agent' not in st.session_state:
            st.session_state.missing_data_agent = MissingDataAgent()

     
        # Call the function
        dis_mis_data = st.session_state.missing_data_agent.display_missing_data(df, feature_names)
        
        # Display the missing data report as a Streamlit dataframe
        st.markdown("##### Display Missing Data")
        #st.dataframe(st.session_state.missing_data_agent.display_missing_data)
        
        return dis_mis_data


     
     
     
     
     
     
     
     
    '''
    def fix_missing_data(self, df):
        #st.write("Fix Missing Data")
        #st.markdown("##### Fix Missing Data")

        # Store the original DataFrame
        original_missing_data = df.copy()
        cleaned_missing_data = df.copy()

        # Get user input for the value to replace missing data
        process = st.text_input("Enter the value to populate all missing data, e.g., NoData:", key="missing_data_input")

        if st.button("Replace Missing Values", key="replace_missing_values_button"):
            if process:
                # Iterate over each column to fill missing values
                for column in cleaned_missing_data.columns:
                    # Check if the column is a datetime type
                    if pd.api.types.is_datetime64_any_dtype(cleaned_missing_data[column]):
                        # Fill NaT values with the user-defined process value as a string
                        cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)
                    else:
                        # Fill NaN values with the user-defined process value
                        cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)

                st.success(f"Missing values in the dataframe replaced with '{process}'")
                st.write("Updated DataFrame:")
                st.dataframe(cleaned_missing_data.head())

                return cleaned_missing_data
            else:
                st.warning("Please enter a value to replace missing data.")
        
        return None
    '''



    def fix_missing_data(self, df):
        if 'df' in st.session_state:
            df = st.session_state.df

        # Store the original DataFrame when a new file is uploaded
        if 'original_df' not in st.session_state or not st.session_state.original_df.equals(df):
            st.session_state.original_df = df.copy(deep=True)

        cleaned_missing_data = df.copy(deep=True)

        # Get user input for the value to replace missing data
        process = st.text_input("Enter the value to populate all missing data, e.g., NoData:", key="missing_data_input")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Replace Missing Values", key="replace_missing_values_button"):
                if process:
                    # Iterate over each column to fill missing values
                    for column in cleaned_missing_data.columns:
                        if pd.api.types.is_datetime64_any_dtype(cleaned_missing_data[column]):
                            cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)
                        else:
                            cleaned_missing_data[column] = cleaned_missing_data[column].fillna(process)

                    st.success(f"Missing values in the dataframe replaced with '{process}'")
                    st.write("Updated DataFrame:")
                    st.dataframe(cleaned_missing_data.head())

                    # Update the session state with the cleaned dataframe
                    st.session_state.df = cleaned_missing_data
                    return cleaned_missing_data
                else:
                    st.warning("Please enter a value to replace missing data.")
        
        with col2:
            if st.button("Revert Changes", key="revert_changes_button"):
                if 'original_df' in st.session_state:
                    st.success("Changes reverted. Original DataFrame restored.")
                    st.write("Original DataFrame:")
                    st.dataframe(st.session_state.original_df.head())
                    # Update the session state with the original dataframe
                    st.session_state.df = st.session_state.original_df.copy(deep=True)
                    return st.session_state.original_df
                else:
                    st.warning("No changes to revert.")

        return None




#################################################################################
#################################################################################

#################################### main function  ##################################

#################################################################################
#################################################################################


def main():  
    load_dotenv()

    # For LangChain OpenAI
    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_API_KEY") == "":
        st.error("OPENAI_API_KEY is not set")
        return
        
 
    ####################### For Apache Airflow configuration and settings ########################
    
    # 1: Configure the Airflow home directory:
    #os.environ['AIRFLOW_HOME'] = '/home/tony/airflow'
        
    # 2: Load the DAGs into a DagBag object
    #dagbag = DagBag()
        
    #####################################################################################

    st.set_page_config(layout="wide")

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


    #st.set_page_config(page_title="Data Quality with Expressions")
    st.header("Data Quality Expressions")
    
    if 'uploaded_file' not in st.session_state:
        st.session_state.uploaded_file = None


    # File uploader widget for CSV, Excel, and JSON files
    uploaded_file = st.sidebar.file_uploader("Upload a file", type=["csv", "xlsx", "json"], key="file_uploader")
    
    #crew_ai = PyLade()

    if 'crew_ai' not in st.session_state:
        st.session_state.crew_ai = PyLade()

    crew_ai = st.session_state.crew_ai

    if uploaded_file is not None and uploaded_file != st.session_state.uploaded_file:
        with st.spinner("Processing uploaded file..."):
            try:
                df = crew_ai.load_data(uploaded_file)
                st.session_state.df = df
                st.session_state.df_reset = True
                st.session_state.data_eda = "EDA Notes"
                st.success("New data uploaded. DataFrame has been reset.")
            except Exception as e:
                st.error(f"Failed to load file: {str(e)}")


    pydeaml = CrewAIPyDEAML() # to replicate the implementation in Jupyter Notebook

    # if uploaded_file is not None and uploaded_file != st.session_state.uploaded_file:
    #     # New file uploaded, reset DataFrame
    #     df = crew_ai.load_data(uploaded_file)
    #     st.session_state.df = df
    #     st.session_state.uploaded_file = uploaded_file
    #     st.session_state.df_reset = True
    #     st.session_state.data_eda = "EDA Notes"
    #     st.success("New data uploaded. DataFrame has been reset.")
    
    
    # Initialise the session_state.df
    if 'df' not in st.session_state:
        st.session_state.df = None  # Initialize with None or an empty DataFrame
        
    df = crew_ai.load_data(uploaded_file)
 
     # Initialise the session_state reset
    if 'df_reset' not in st.session_state:
        st.session_state.df_reset = False

    eda_df = st.sidebar.button("Reset DataFrame")
    if eda_df:
        # Perform LLM exploration on the DataFrame
        df = crew_ai.load_data(uploaded_file)
        st.session_state.df = df
        st.session_state.df_reset = True
        st.session_state.data_eda = "EDA Notes"
        st.write("Initial DataFrame Restored")

       


    #test_lade = PyLade(df)
    #eda_of_data = perform_eda(df)
 
     # Sidebar options for data exploration
    #data_eda = st.sidebar.selectbox("EDA - explore the data", ["EDA Notes", "Preprocess Data", "Explore with LLM", "Data Structure", "Data Statistics", "Feature Engineering"\
     #                                               ,"Missing Data Agent"])

    # Setting for the EDA sidebar
    if 'data_eda' not in st.session_state:
        st.session_state.data_eda = "EDA Notes"
    if 'reset' not in st.session_state:
        st.session_state.reset = False

    # Function to reset selection
    def reset_eda_selection():
        st.session_state.data_eda = "EDA Notes"
        st.session_state.reset_eda_radio = "No"

    # Use st.sidebar.markdown() to set the font size
    #st.sidebar.markdown("## <span style='font-size: 15px;'>EDA Options</span>", unsafe_allow_html=True)

    

    # Sidebar with radio buttons
    data_eda = st.sidebar.radio(
        "EDA - explore the data", 
        options = ["EDA Notes", "Preprocess Data", "Explore with LLM", "Data Structure", "Data Statistics", "Feature Engineering", "Missing Data Agent"], 
        format_func = lambda x: "EDA Notes" if x == "" else x,
        index = 0 if st.session_state.data_eda == "" else ["EDA Notes", "Preprocess Data", "Explore with LLM", "Data Structure", "Data Statistics", "Feature Engineering", "Missing Data Agent"].index(st.session_state.data_eda),
        key = 'de_data_eda'
    )

    # Reset the df_reset flag after the radio button is rendered
    if st.session_state.df_reset:
        st.session_state.df_reset = False
    
    
    
    # st.sidebar.checkbox("Reset EDA Selection", on_change=reset_eda_selection, key='reset_checkbox')

    
    # Checkbox to disable the Disable EDA Notes
    #disable_eda_notes = st.sidebar.checkbox("Disable EDA Notes")

    #if not disable_eda_notes:
    
    space_indent = "&nbsp;" * 50
    #st.markdown(f"{indent}Your indented text here", unsafe_allow_html=True)


    if data_eda == "EDA Notes":

        eda_1 = st.markdown(
        f"""
        ---
        Options under the **EDA - explore the data** selection
        - **Explore with LLM:** {"&nbsp;" * 20} *To use Langchain to explore the data.*
        - **Preprocess Data:** {"&nbsp;" * 22} *To provide some information and basic processing.*
            - **Initial Statistics:** {"&nbsp;" * 15} *Display some data statistsics.*
            - **Add Headers:** {"&nbsp;" * 21} *Add headers to DataFrames without headers.*
            - **Assign Data Types:** {"&nbsp;" * 10} *Provide correct data types to features.*
            - **Create New Feature:** {"&nbsp;" * 6} *Use existing featurs to create a new one.*
            - **Rename Features:** {"&nbsp;" * 11} *To rename exising features.*
            - **View Interval Records:** {"&nbsp;" * 2} *View records within any two set of intervals.*
            - **Delete Records.** {"&nbsp;" * 17} *Delete any record(s).*
        - **Data Structure:** {"&nbsp;" * 27} *To obtain the structure of the data.*
            - **Head of the data:** {"&nbsp;" * 14} *Display the first 5 records.*
            - **Tail of the data:** {"&nbsp;" * 18} *Display the last 5 records.*
            
        - **Reset EDA Selection:** {"&nbsp;" * 16} *To reset all selected options within* **EDA - explore the data**.
        """
        , unsafe_allow_html=True
    )
    
        #st.subheader("Peek of Uploaded DataFrame:")
        #st.write(df.head())

        #       st.markdown("*Streamlit* is **really** ***cool***.")
        #       st.markdown('''
        #           :red[Streamlit] :orange[can] :green[write] :blue[text] :violet[in]
        #           :gray[pretty] :rainbow[colors] and :blue-background[highlight] text.''')
        #       st.markdown("Here's a bouquet &mdash;\
        #                   :tulip::cherry_blossom::rose::hibiscus::sunflower::blossom:")
    


    if data_eda == "Preprocess Data":
        st.sidebar.write("Preprocess Data is selected")
        
        # Check if the DataFrame exists in session state
        if st.session_state.df is not None:
            df = st.session_state.df  # Get the current DataFrame
            
            df = crew_ai.preprocess_data(df)  # Apply preprocessing logic
            df = st.session_state.df  # Get the current DataFrame

        else:
            st.session_state.df = df
            st.warning("No data uploaded yet. Please upload a file first.")
            
        #st.dataframe(st.session_state.df.head())  
        st.session_state.df = df
        
    
    elif data_eda == "Explore with LLM":
        st.dataframe(st.session_state.df.head())  
        if st.session_state.df is not None:
            df = st.session_state.df  # Get the preprocessed DataFrame from session_state
            
            #st.write("check the DF")
            #st.dataframe(st.session_state.df .head()) 
            st.sidebar.write("EDA with LLM is selected")
            eda_1 = st.sidebar.checkbox("Use LLM to explore data")
            #df = st.session_state.df
           # st.write("dfss1")
            #st.dataframe(df.head()) 
          #  st.write("df2")
          #  st.dataframe(st.session_state.df.head())  # Display the DataFrame or pass it to the LLM for exploration
          #  st.sidebar.write("EDA with LLM is selected")
          #  eda_1 = st.sidebar.checkbox("Use LLM to explore data")

            if eda_1:
                # Perform LLM exploration on the DataFrame
                st.write("Exploring the Data with LLM:")
                #st.dataframe(df.head())  # Display the DataFrame or pass it to the LLM for exploration
            
            
            preprocessed_csv = crew_ai.df_csv_io(df)
            
            wip_preprocessed_csv = crew_ai.generate_create_csv(preprocessed_csv)
            
            #result = wip_preprocessed_csv.run(user_question)


            user_question = None
            
            output_capture = OutputCapture() # for screen display of internal data

            if wip_preprocessed_csv is not None:
                if eda_1:
                    st.write("Ask questions about the data or request a chart.")
                    user_question = st.text_input("Text summary of data exploration with LLM: ")

                    if user_question is not None and user_question != "":
                        with st.spinner(text="In progress..."):
                            action_input = user_question
                            response = wip_preprocessed_csv.run(user_question)
                            
                            
                            #output_capture = OutputCapture()
                            
                            st.subheader("Internal Processing (Captured Output):")
                            captured_output = output_capture.get_output()
                            cleaned_output = re.sub(r'\x1b\[[0-9;]*m', '', captured_output)
                            output_lines = cleaned_output.split('\n')

                            if len(output_lines) >= 6:
                                st.text('\n'.join(output_lines[5:]))
                            else:
                                st.text(cleaned_output)

                            st.subheader("Action Input:")
                            st.text(action_input)

                            st.subheader("Final Answer:")
                            for part in response.split("\n\n"):
                                st.write(part)

                            # Check if the user requested a chart                                  
                            if "chart" in user_question.lower() or "graph" in user_question.lower() or "plot" in user_question.lower():
                                st.subheader("Generated Chart:")
                                
                                # Add Apply button
                                #if st.button("Apply and Generate Chart"):
                                
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
                                        fig = px.histogram(df, x=selected_columns, color=color_column)
                                    elif chart_type in ["Box", "Violin"]:
                                        params["x"] = selected_columns[0]
                                        params["y"] = selected_columns[1] if len(selected_columns) > 1 else None
                                        fig = px.box(**params) if chart_type == "Box" else px.violin(**params)
                                    elif chart_type == "Heatmap":
                                        fig = px.density_heatmap(df, x=selected_columns[0], y=selected_columns[1] if len(selected_columns) > 1 else None)
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
                                    elif chart_type == "Parallel Coordinates":
                                        fig = px.parallel_coordinates(df, dimensions=selected_columns, color=color_column)
                                    elif chart_type == "Parallel Categories":
                                        fig = px.parallel_categories(df, dimensions=selected_columns, color=color_column)
                                    
                                    fig.update_layout(width=width*50, height=height*50)
                                    st.plotly_chart(fig, width='stretch')
                            
                                except Exception as e:
                                    st.error(f"Error occurred while generating the chart: {str(e)}")



#########
    elif data_eda == "Feature Engineering":
        st.sidebar.write("Feature Engineering is selected")
        
        df = crew_ai.feature_engineering()
        
        # Load the data
        if df is not None:
            try:
                updated_df = df.copy()
                #df = pd.read_csv(csv_file)
                #st.write("Original DataFrame:")
                #st.dataframe(df.head())

                # Feature Engineering Options
                fe_option = st.sidebar.selectbox(
                    "Choose a Feature Engineering task",
                    ["Select an option", "Rename Feature", "Create Feature", "Handle Values", "Encode Categorical Variables", "Scale Numerical Features", "Bin Numerical Features"]
                )

                if fe_option == "Rename Feature":
                    st.subheader("Rename Feature")
                    #for col in updated_df.columns:
                    for i, col in enumerate(updated_df.columns):
                        new_name = st.text_input(f"New name for '{col}':", col, key=f"new_name{i}")
                        if new_name != col:
                            updated_df = updated_df.rename(columns={col: new_name})

                elif fe_option == "Create Feature":
                    st.subheader("Create Feature")
                    new_feature = st.text_input("Name of new feature:")
                    feature_expression = st.text_input("Python expression for new feature (use column names):", key="new_name_engineered")
                    if new_feature and feature_expression:
                        try:
                            updated_df[new_feature] = updated_df.eval(feature_expression)
                            st.success(f"New feature '{new_feature}' created successfully!")
                        except Exception as e:
                            st.error(f"Error creating new feature: {str(e)}")

                elif fe_option == "Handle Missing Values":
                    st.subheader("Handle Missing Values")
                    for col in updated_df.columns:
                        if updated_df[col].isnull().sum() > 0:
                            method = st.selectbox(f"Handle missing values in '{col}':", 
                                                  ["Drop", "Fill with Mean", "Fill with Median", "Fill with Mode"])
                            if method == "Drop":
                                updated_df = updated_df.dropna(subset=[col])
                            elif method == "Fill with Mean":
                                updated_df[col].fillna(updated_df[col].mean(), inplace=True)
                            elif method == "Fill with Median":
                                updated_df[col].fillna(updated_df[col].median(), inplace=True)
                            elif method == "Fill with Mode":
                                updated_df[col].fillna(updated_df[col].mode()[0], inplace=True)

                elif fe_option == "Encode Categorical Variables":
                    st.subheader("Encode Categorical Variables")
                    cat_columns = updated_df.select_dtypes(include=['object']).columns
                    for col in cat_columns:
                        method = st.selectbox(f"Encoding method for '{col}':", 
                                              ["One-Hot Encoding", "Label Encoding"])
                        if method == "One-Hot Encoding":
                            updated_df = pd.get_dummies(updated_df, columns=[col])
                        elif method == "Label Encoding":
                            updated_df[col] = updated_df[col].astype('category').cat.codes

                elif fe_option == "Scale Numerical Features":
                    st.subheader("Scale Numerical Features")
                    num_columns = updated_df.select_dtypes(include=['float64', 'int64']).columns
                    method = st.selectbox("Scaling method:", ["StandardScaler", "MinMaxScaler"])
                    if method == "StandardScaler":
                        from sklearn.preprocessing import StandardScaler
                        scaler = StandardScaler()
                    else:
                        from sklearn.preprocessing import MinMaxScaler
                        scaler = MinMaxScaler()
                    updated_df[num_columns] = scaler.fit_transform(updated_df[num_columns])

                elif fe_option == "Bin Numerical Features":
                    st.subheader("Bin Numerical Features")
                    num_columns = updated_df.select_dtypes(include=['float64', 'int64']).columns
                    column_to_bin = st.selectbox("Select column to bin:", num_columns)
                    num_bins = st.number_input("Number of bins:", min_value=2, value=5)
                    updated_df[f"{column_to_bin}_binned"] = pd.cut(updated_df[column_to_bin], bins=num_bins)
                   
                # Display the updated DataFrame
                if fe_option != "Select an option":
                    st.write("Updated DataFrame:")
                    st.dataframe(updated_df.head())
                    
                    # Option to download the updated CSV
                    csv = updated_df.to_csv(index=False)
                    st.download_button(
                        label="Download updated CSV",
                        data=csv,
                        file_name="updated_data.csv",
                        mime="text/csv",
                    )
                    
                    if updated_df is not None:
                        if st.button("Confirm Feature Engineering Changes"):
                            st.session_state.df = updated_df
                            st.success("Main dataframe updated with feature engineering changes.")
                            
            except Exception as e:
                st.error(f"Error occurred while performing feature engineering: {str(e)}")
                    
                
             # Add horizontal line after the if condition
            st.sidebar.markdown("<hr>", unsafe_allow_html=True)
            
#########

        
    elif data_eda == "Missing Data Agent":
        st.sidebar.write("Missing Data Agent is selected")
        st.dataframe(st.session_state.df.head())  
        
#        if st.checkbox("Detect Missing Data"):
#            if 'df' in st.session_state and st.session_state.df is not None:
#                original, cleaned, report = pydeaml.missing_data_agent(st.session_state.df)
#                st.session_state.original_missing_data = original
#                st.session_state.cleaned_missing_data = cleaned
#                st.session_state.missing_data_report = report
#            else:
#                st.warning("No data available. Please upload a file first.")   
        
 
        # Use the wrapper function in your Streamlit app
        if st.checkbox("Detect Missing Data"):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = pydeaml.detect_missing_data(st.session_state.df)
                st.session_state.df = df
            else:
                st.warning("No data available. Please upload a file first.")
                
            
         # Use the wrapper function in your Streamlit app               
        if st.checkbox("Display Missing Data"):
            if 'df' in st.session_state and st.session_state.df is not None:
                feature_input = st.text_input("Enter columns to check for missing data (comma-separated):")
                feature_names = [name.strip() for name in feature_input.split(',')] if feature_input else []

                if feature_names:
                    try:
                        mis_data = pydeaml.display_missing_data(st.session_state.df, feature_names)
                        mis_data = mis_data.rename_axis("Index")
                        if not mis_data.empty:
                            st.markdown("###### Rows with Missing Data")
                            st.dataframe(mis_data)
                        else:
                            st.write("No missing data found in the specified columns.")
                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
                else:
                    st.write("Please enter column names to check for missing data.")
            else:
                st.warning("No data available. Please upload a file first.")

                
#        if st.checkbox("Display Missing Data"):
#            if 'df' in st.session_state and st.session_state.df is not None:
#                feature_input = st.text_input("Enter columns to check for missing data (comma-separated):")
#                feature_names = [name.strip() for name in feature_input.split(',')] # convert to list
#                
#                if feature_names:
#                    mis_data = pydeaml.display_missing_data(st.session_state.df ,feature_names)                
#                    mis_data = mis_data.rename_axis("Index")
#                    if not mis_data.empty:
#                        st.subheader("Rows with Missing Data")
#                        st.dataframe(mis_data)
#                    else:
#                        st.write("No missing data found in the specified columns.")
#                else:
#                    st.write("Please enter column names to check for missing data.")
  

        if st.checkbox("Fix Missing Data"):
            if 'df' in st.session_state and st.session_state.df is not None:
                df = pydeaml.fix_missing_data(st.session_state.df)
                if df is not None:
                    st.session_state.df = df
                    st.dataframe(st.session_state.df.head())  

######################################################
  
                
        if data_eda == "Data Statistics":
            st.sidebar.write("Data Statistics is selected")
            eda_stat2 = st.sidebar.checkbox("Head of the data")
            eda_stat3 = st.sidebar.checkbox("Tail of the data")

            if eda_stat2:
                eda_of_data.head_data()

            if eda_stat3:
                eda_of_data.tail_data()

    
        # Add a horizontal line
        st.sidebar.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)

        # Reset the selected options
        # st.sidebar.checkbox("Reset EDA Selection", on_change=reset_eda_selection, key='reset_eda_checkbox')


        reset_eda = st.sidebar.radio(
        "Reset EDA Selection", 
        options=["No", "Yes"],
        index=0,
        key='reset_eda_radio',
        on_change=reset_eda_selection,
        horizontal=True
        )

        # Add a horizontal line
        st.sidebar.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)


        ######################################################################################
        ######################################################################################
        ######################################################################################


        # Setting for the sidebar
        if 'dq_metrics' not in st.session_state:
            st.session_state.dq_metrics = "DQ Notes"
        if 'reset' not in st.session_state:
            st.session_state.reset = False

        # Function to reset selection
        def reset_dq_selection():
            st.session_state.dq_metrics = "DQ Notes"
            #st.session_state.reset = True
            st.session_state.reset_dq_radio= "No"

        # Use st.sidebar.markdown() to set the font size
        st.sidebar.markdown("## <span style='font-size: 15px;'>Data Metrics Options</span>", unsafe_allow_html=True)



        # Sidebar with radio buttons
        dq_metrics = st.sidebar.radio(
            "DQ - produce metrics", 
            options = ["DQ Notes", "Visualize Missing Data", "Missing Data Accuracy"], 
            format_func = lambda x: "DQ Notes" if x == "" else x,
            index = 0 if st.session_state.dq_metrics == "" else ["DQ Notes", "Visualize Missing Data", "Missing Data Accuracy"].index(st.session_state.dq_metrics),
            key = 'dq_metrics'
        )


        #       st.sidebar.checkbox("Reset EDA Selection", on_change=reset_dq_selection, key='reset_checkbox')

        # Checkbox to Disable Data Metrics Instructions
        disable_dmo_notes = st.sidebar.checkbox("Disable DQ Notes")

        if not disable_dmo_notes:

            if dq_metrics == "DQ Notes":
                dq1 = st.markdown(
                """
                ---
                Options under the **DQ - produce metrics** selection
                - **Missing Data Accuracy:** *To determing accuracy for data with missing entries.*
                - **DQ1:** *To obtain the quality of the data.*
                    - **Q1 of the data:** *Display the data accuracy1.*
                    - **Q2 of the data:** *Display the data accuracy2.*
                    
                - **Reset DQ Selection:** *To reset all selected options within **DQ - produce metrics**.
                ---
                """
            )      


        if dq_metrics == "Visualize Missing Data":
            st.sidebar.write("Missing data visualization is selected")
            dq2 = st.sidebar.checkbox("Heatmap of missing data")
            dq3 = st.sidebar.checkbox("Perform EDA using Lux")

            if dq2:
                #rtt = crew_ai.visualise_missing_data()
                # Specify the path to the HTML file
                html_file_path = 'testdq_report.html'

                # Read the HTML file
                with open(html_file_path, 'r', encoding='utf-8') as html_file:
                    html_content = html_file.read()

                # Display the HTML content in Streamlit
                st.components.v1.html(html_content, width=800, height=600, scrolling=True)


            if dq3:
                
                st.write('Visualise missing data')
                # Create a figure and axis for the plot

                                # Create a figure and axis for the plot
 #               fig, ax = plt.subplots()

                # Generate the displot
 #               sns.displot(
 #                   data=self.original_data.isnull().melt(value_name='missing'),
 #                   y='variable',
 #                   hue='missing',
 #                   multiple='fill',
 #                   height=8,
 #                   aspect=1.1,
 #                   ax=ax  # Add the axis to the plot
 #                )

                # Add the threshold line
 #               plt.axvline(0.4, color='r')

                # Display the plot in Streamlit
 #               st.pyplot(fig)
 #               return fig
                df_v=df.isnull().melt(value_name='missing')
                abd = st.bar_chart(df_v)
                st.components.v1.html(abd, width=800, height=600, scrolling=True)
                
                #st.write(output)

                

        if dq_metrics == "Data Statistics":
            st.sidebar.write("Data Statistics is selected")
            eda_2 = st.sidebar.checkbox("Head of the data")
            eda_3 = st.sidebar.checkbox("Tail of the data")

            if eda_2:
                eda_of_data.head_data()

            if eda_3:
                eda_of_data.tail_data()

    
        # Add a horizontal line
        st.sidebar.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)

        # Reset the selected options
        # st.sidebar.checkbox("Reset DQ Selection", on_change=reset_dq_selection, key='reset_dq_checkbox')

        reset_dq = st.sidebar.radio(
        "Reset DQ Notes", 
        options=["No", "Yes"],
        index=0,
        key='reset_dq_radio',
        on_change=reset_dq_selection,
        horizontal=True
        )

        # Add a horizontal line
        st.sidebar.markdown("<hr style='margin: 2px 0;'>", unsafe_allow_html=True)


        dq_metrics = st.sidebar.radio("Select DQ Metrics", ("Missing Data Accuracy", "Predict Missing Data", "Fix Missing Data"))

       # if dq_metrics == "Missing Data Accuracy":
            #crew_ai.accuracy_missing_data(df)
  #      elif dq_metrics == "Predict Missing Data":
 #           column_to_predict = st.selectbox("Select column to predict missing data", df.columns)
  #          if column_to_predict:
  #              predicted_values = crew_ai.predict_missing_data(df[column_to_predict])
  #              st.write(predicted_values)
        #elif dq_metrics == "Fix Missing Data":
            #process = st.selectbox("Select process", ["replace", "mean", "median", "mode", "ffill", "bfill", "interpolate", "replace with predicted values"])
            #raw_missing_data = crew_ai.accuracy_missing_data()
            #cleaned_missing_data = crew_ai.fix_missing_data(process)
            #cleaned_value_count = cleaned_missing_data.count().sum()
            #cleaned_element_count = cleaned_missing_data.size
            #ratio_missing_data = (cleaned_element_count - cleaned_value_count) / cleaned_element_count
            #accuracy_after_fix_missing_data = 1 - ratio_missing_data
            #st.write("Missing Data: Accuracy before fix:", raw_missing_data)
            #st.write("Missing Data: Accuracy after fix:", accuracy_after_fix_missing_data)
            #st.write(accuracy_after_fix_missing_data)
    

    #   csv_file = st.file_uploader("CSV File Upload", type="csv")

    #   if csv_file is not None:
    #       df = pd.read_csv(csv_file)


        #operations = ["Fix Missing Data", "Fix Unstandardized Data", "Fix Duplicate Data", "Show Final Data"]
        #selected_operations = st.multiselect("Select operations to perform", operations)

        # Initialize session state
        if 'processor' not in st.session_state:
            st.session_state.processor = PyLade()

        processor = st.session_state.processor


        # Define the sequence of function names to be executed
        #func_names = ['fix_missing_data', 'fix_unstandardized_data', 'fix_duplicate_data']

        # Define the order of function calls
        # order_of_functions = ['fix_duplicate_data', 'fix_missing_data', 'fix_unstandardized_data']

        selected_operations = st.multiselect(
            "Select Operations: start with :red[Fix Duplicate Data]", 
            ["Fix Duplicate Data", "Fix Missing Data", "Fix Unstandardized Data",  "Show Final Data"]
        )

        for operation in selected_operations:

            if operation == "Fix Duplicate Data": 
                # Initialize session state
                if "confirming" not in st.session_state:
                    st.session_state.confirming = False
                    st.session_state.operation = None
                    st.session_state.last_operation_data = df  # Initial data state
                    st.session_state.cleaned_data_dedup = df  # Current data state

#                operation = "Fix Duplicate Data"

                def apply_operation():
                    # Save the current state before applying the new operation
                    st.session_state.last_operation_data = st.session_state.cleaned_data_dedup
                    # Simulate applying the operation
                    st.session_state.cleaned_data_dedup = processor.fix_duplicate_data(df)
                    st.write(st.session_state.cleaned_data_dedup)

                def undo_operation():
                    # Save the current state before undoing
                    st.session_state.last_operation_data = st.session_state.cleaned_data_dedup
                    # Simulate undoing the operation
                    st.session_state.cleaned_data_dedup = df
                    st.write(st.session_state.cleaned_data_dedup)

                def confirm_operation():
                    st.session_state.confirming = True
                    st.session_state.operation = "Fix Duplicate Data"

              #  def reset_last_operation():
                    # Revert to the last saved state
              #      st.session_state.cleaned_data_dedup = st.session_state.last_operation_data
              #      st.write(st.session_state.cleaned_data_dedup)



                # Radio button to select operation
                selected_operation = st.radio(
                    "Select Operation:",
                    ["Apply", "Undo", "Confirm"],#, "Reset Last Operation"]
                    horizontal = True

                )

                # Perform action based on selected operation
                if selected_operation == "Apply":
                    apply_operation()
                elif selected_operation == "Undo":
                    undo_operation()
                    st.write("Choose **:green[Apply]** to restart the **:red[Fix Duplicate Data]** process")
                elif selected_operation == "Confirm":
                    confirm_operation()
            #    elif selected_operation == "Reset Last Operation":
            #        reset_last_operation()

                # Handle confirmation step
                if st.session_state.confirming and st.session_state.operation == "Fix Duplicate Data":
                    #st.write("Are you sure?")
                    confirm_button = st.radio(
                        "Are you sure?",
                        ["Select", "Yes", "No"],#, "Reset Last Operation"]
                        horizontal = True
                        )
                    if confirm_button =="Select":
                        st.write("Select **:green[Yes]** to **Confirm** or **:green[No]** to revert.")
                        st.session_state.confirming = False
                        st.session_state.operation = None

                    elif confirm_button =="Yes":
                        # Perform the confirmation action
                        cleaned_data = st.session_state.cleaned_data_dedup
                        st.write(cleaned_data)
                        processor.cleaned_data = cleaned_data
                        st.write("**Confirmed.** Next change operations to **:green[Fix Missing Data.]**")
                        # Reset confirmation state
                        st.session_state.confirming = False
                        st.session_state.operation = None
                        return cleaned_data

                    elif confirm_button =="No":
                        # Cancel the confirmation
                        st.write("**Operation cancelled.** Choose **:green[Apply]** to restart the **:red[Fix Duplicate Data]** process")
                        cleaned_data = undo_operation()
                        st.write(cleaned_data)
                        st.session_state.confirming = False
                        st.session_state.operation = None

            
            process = st.selectbox("Select process", ["replace with NA", "mean", "median", "mode", "ffill", "bfill", "interpolate", "replace with predicted values"], key="fix_missing_data") 
            if operation == "Fix Missing Data": 
                #process = st.selectbox("Select process", ["replace with NA", "mean", "median", "mode", "ffill", "bfill", "interpolate", "replace with predicted values"], key="fix_missing_data") 
                cleaned_data = processor.fix_missing_data(cleaned_data, process)
                # Initialize session state
                if "confirming" not in st.session_state:
                    st.session_state.confirming = False
                    st.session_state.operation = None
                    st.session_state.last_operation_data = cleaned_data  # Initial data state
                    st.session_state.cleaned_data_missing = cleaned_data  # Current data state

#                operation = "Fix Duplicate Data"


                def apply_operation():
                    # Save the current state before applying the new operation
                    st.session_state.last_operation_data = st.session_state.cleaned_data_missing
                    # Simulate applying the operation
                    st.session_state.cleaned_data_missing = processor.fix_missing_data(cleaned_data)
                    st.write(st.session_state.cleaned_data_missing)

                def undo_operation():
                    # Save the current state before undoing
                    st.session_state.last_operation_data = st.session_state.cleaned_data_missing
                    # Simulate undoing the operation
                    st.session_state.cleaned_data_missing = cleaned_data
                    st.write(st.session_state.cleaned_data_missing)

                def confirm_operation():
                    st.session_state.confirming = True
                    st.session_state.operation = "Fix Missing Data"

            #  def reset_last_operation():
                    # Revert to the last saved state
            #      st.session_state.cleaned_data_dedup = st.session_state.last_operation_data
            #      st.write(st.session_state.cleaned_data_dedup)



                # Radio button to select operation
                selected_operation = st.radio(
                    "Select Operation:",
                    ["Apply", "Undo", "Confirm"],#, "Reset Last Operation"]
                    horizontal = True

                )

                # Perform action based on selected operation
                if selected_operation == "Apply":
                    apply_operation()
                elif selected_operation == "Undo":
                    undo_operation()
                    st.write("Choose **:green[Apply]** to restart the **:red[Fix Missing Data]** process")
                elif selected_operation == "Confirm":
                    confirm_operation()
            #    elif selected_operation == "Reset Last Operation":
            #        reset_last_operation()

                # Handle confirmation step
                if st.session_state.confirming and st.session_state.operation == "Fix Missing Data":
                    #st.write("Are you sure?")
                    confirm_button = st.radio(
                        "Are you sure?",
                        ["Select", "Yes", "No"],#, "Reset Last Operation"]
                        horizontal = True
                        )
                    if confirm_button =="Select":
                        st.write("Select **:green[Yes]** to **Confirm** or **:green[No]** to revert.")
                        st.session_state.confirming = False
                        st.session_state.operation = None

                    elif confirm_button =="Yes":
                        # Perform the confirmation action
                        cleaned_data = st.session_state.cleaned_data_missing
                        st.write(cleaned_data)
                        processor.cleaned_data = cleaned_data
                        st.write("**Confirmed.** Next change operations to **:green[Fix Unstandardized Data.]**")
                        # Reset confirmation state
                        st.session_state.confirming = False
                        st.session_state.operation = None
                        return cleaned_data

                    elif confirm_button =="No":
                        # Cancel the confirmation
                        st.write("**Operation cancelled.** Choose **:green[Apply]** to restart the **:red[Fix Missing Data]** process")
                        cleaned_data = undo_operation()
                        st.write(cleaned_data)
                        st.session_state.confirming = False
                        st.session_state.operation = None

                
            elif operation == "Fix Unstandardized Data":
                cleaned_data = processor.cleaned_data
                if st.button("Apply Fix Unstandardized Data", key="fix_unstandardized_data_btn"):
                    cleaned_data = processor.fix_unstandardized_data(cleaned_data)
                    st.write("Data after Fix Unstandardized Data:")
                    st.write(cleaned_data)
                    processor.cleaned_data = cleaned_data 
                    return cleaned_data

            
            elif operation == "Show Final Data":
                cleaned_data = processor.cleaned_data
                st.write("Final Data:")
                st.write(cleaned_data)
        




        if st.button("Show Log"):
            st.write("Log of Applied Operations:")
            st.write(processor.log)



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

    # Initialize steps and checkbox states
    if 'steps' not in st.session_state:
        st.session_state.steps = [
            "Clean Data",
            "Feature Engineering",
            "Missing Data",
            "Normalize Data",
            "Standardize Column Data",
            "Inflection AI Standard Feature Name"#,
           # "Evaluate Model",
            #"Evaluate Model1",
            #"Evaluate Model2",
           # "Evaluate Model3"
        ]

    if 'radio_selections' not in st.session_state:
        st.session_state.radio_selections = {step: "Idle" for step in st.session_state.steps}
        
    if 'radio_selections_states' not in st.session_state:
            st.session_state.radio_selections_states = {step: False for step in st.session_state.steps}
    if 'checkbox_states' not in st.session_state:
        # Initialize checkbox states for all steps
        st.session_state.checkbox_states = {step: False for step in st.session_state.steps}

    # File uploader
    # uploaded_file = st.sidebar.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        data = pd.read_csv(uploaded_file)
        if st.session_state.df is None:
            st.session_state.df = data

        # Initialize session state for step statuses
        for step in st.session_state.steps:
            if step not in st.session_state.statuses:
                st.session_state.statuses[step] = 'idle'

        # Main title
        st.header("Pipeline for Data Analysis")
        
        # Pipeline visualization container
        with sticky_container(mode="top", border=True):
            #st.markdown("Pipeline Visualization")
            st.markdown('<h5 style="color: grey;">Pipeline Visualization</h5>', unsafe_allow_html=True)
            pipeline_placeholder = st.empty()
            
            active_steps = [step for step in st.session_state.steps 
                            if st.session_state.statuses.get(step) != 'idle']

            if active_steps:  
                visualizer = PyDEAMLPipeline()
                visualizer.display_horizontal_pipeline(steps=active_steps, statuses=st.session_state.statuses)
                pipeline_graph = visualizer.generate_html()  # Generate Pyvis HTML graph

                # Render the HTML for the pipeline using Pyvis and Streamlit's HTML component
                components.html(pipeline_graph, height=200)
                
            else:
                pipeline_placeholder.info("No components added yet - check steps in sidebar")
   

        # Ensure session state variables are initialized
        if 'checkbox_states' not in st.session_state:
            st.session_state.checkbox_states = {step: False for step in st.session_state.steps}



        if 'statuses' not in st.session_state:
            st.session_state.statuses = {step: 'idle' for step in st.session_state.steps}

        # Callback function to synchronize checkbox states and statuses
        def update_checkbox_state(step):
            st.session_state.checkbox_states[step] = not st.session_state.checkbox_states[step]
            st.session_state.statuses[step] = 'current' if st.session_state.checkbox_states[step] else 'idle'


        for step in st.session_state.steps:
            if f"radio_{step}" not in st.session_state:
                st.session_state[f"radio_{step}"] = "Idle"


         # Define the callback function
        def update_radio_selections_states(step):
            action = st.session_state[f"radio_{step}"]

            if action == "Accept":
                st.session_state.statuses[step] = "completed"
                st.success(f"Changes for {step} have been accepted.", icon="✅")
            elif action == "Reject":
                st.session_state.statuses[step] = "idle"
                st.warning(f"Changes for {step} have been rejected. Reverting to previous state.", icon="⚠️")

                # Reset statuses of subsequent steps
                step_index = st.session_state.steps.index(step)
                for subsequent_step in st.session_state.steps[step_index:]:
                    st.session_state.statuses[subsequent_step] = "idle"
                    st.session_state.radio_selections[subsequent_step] = "Idle"

            # Update the selection state
            st.session_state.radio_selections[step] = action
            

        # Sidebar controls for pipeline steps
        with st.sidebar:
            st.markdown("### Pipeline Steps")
            for idx, step in enumerate(st.session_state.steps):
                key = f"step_activation_{step}_{idx}"  # Unique key for each checkbox

                # Create checkbox with the callback function
                st.checkbox(
                    step,
                    value=st.session_state.checkbox_states[step],
                    key=key,
                    on_change=update_checkbox_state,
                    args=(step,)
                )


        # Display current states for debugging
        #st.write("Checkbox States:", st.session_state.checkbox_states)
        #st.write("Step Statuses:", st.session_state.statuses)
        
        
        # Process and display outputs for active steps
        for step in st.session_state.steps:
            if st.session_state.checkbox_states.get(step, True):
                with st.container():
                    st.header(f"Output for: {step}")
                    
                    temp_df = st.session_state.df.copy()
                    
                    st.session_state.temp_df = temp_df


                    # Step-specific operations
                    if step == "Clean Data":
                        st.session_state.df = pydeamlpipeline.clean_data(st.session_state.df)
                        st.dataframe(st.session_state.df.head())


                    elif step == "Feature Engineering":
                        st.session_state.df = pydeamlpipeline.perform_feature_engineering(st.session_state.df)
                        st.dataframe(st.session_state.df.head())

                    elif step == "Missing Data":
                        #st.session_state.df = crewailpydeamlpipeline.detect_missing_data(st.session_state.df)
                        st.write("It will stay at the bottom as you scroll.")
                        st.dataframe(st.session_state.df.head())

                        if st.checkbox("Detect Missing Data"):
                            if 'df' in st.session_state and st.session_state.df is not None:
                                df = crewailpydeamlpipeline.detect_missing_data(st.session_state.df)
                                st.session_state.df = df
                            else:
                                st.warning("No data available. Please upload a file first.")
                                
                            
                         # Use the wrapper function in your Streamlit app               
                        if st.checkbox("Display Missing Data"):
                            if 'df' in st.session_state and st.session_state.df is not None:
                                feature_input = st.text_input("Enter columns to check for missing data (comma-separated):")
                                feature_names = [name.strip() for name in feature_input.split(',')] if feature_input else []

                                if feature_names:
                                    try:
                                        mis_data = crewailpydeamlpipeline.display_missing_data(st.session_state.df, feature_names)
                                        mis_data = mis_data.rename_axis("Index")
                                        if not mis_data.empty:
                                            st.markdown("###### Rows with Missing Data")
                                            st.dataframe(mis_data)
                                            st.dataframe(st.session_state.mis_data.head())
            
                                        else:
                                            st.write("No missing data found in the specified columns.")
                                    except Exception as e:
                                        st.error(f"An error occurred: {str(e)}")
                                else:
                                    st.write("Please enter column names to check for missing data.")
                            else:
                                st.warning("No data available. Please upload a file first.")


                        if st.checkbox("Fix Missing Data"):
                            if 'df' in st.session_state and st.session_state.df is not None:
                                df = crewailpydeamlpipeline.fix_missing_data(st.session_state.df)
                                if df is not None:
                                    st.session_state.df = df
                                    #st.dataframe(st.session_state.df.head())  
                                    st.dataframe(st.session_state.df.head())



                    elif step == "Standardize Column Data":
                        st.dataframe(st.session_state.df.head())

                        # Define your function that takes three arguments
                        def column_standardization_agent_arg(df, column, reference_column, prompt_template):
                            return f"{df}, {column}, {reference_column}, {prompt_template}"
                            
                        if "standardize_missing_data" not in st.session_state or st.session_state.standardize_missing_data is None:
                            st.session_state.standardize_missing_data = column_standardization_agent_arg
                            
                        if "standardize_column" not in st.session_state or st.session_state.standardize_column is None:
                            st.session_state.standardize_column = column_standardization_agent_arg
                        
                        if "column" not in st.session_state:
                            st.session_state.column = ""

                        if "reference_column" not in st.session_state:
                            st.session_state.reference_column = ""

                        if "prompt_template" not in st.session_state:
                            st.session_state.prompt_template = ""

                        # Define your function that takes three arguments
                        def column_standardization_agent_arg(df, column, reference_column, prompt_template):
                            return f"{df}, {column}, {reference_column}, {prompt_template}"
                                                  # Define the callback function to update state
                        def update_inputs():
                            st.session_state.df = st.session_state.df
                            st.session_state.column = st.session_state.column
                            st.session_state.reference_column = st.session_state.reference_column
                            st.session_state.prompt_template = st.session_state.prompt_template

                        # Collect text inputs from the user
                        column = st.text_input("Enter the column:", key="column", on_change=update_inputs)
                        reference_column = st.text_input("Enter the reference_column:", key="reference_column", on_change=update_inputs)
                        prompt_template = st.text_input("Enter prompt_template:", key="prompt_template", on_change=update_inputs)

                        # Display current state of the inputs
                        st.write(f"column: {st.session_state.column}")
                        st.write(f"reference_column: {st.session_state.reference_column}")
                        st.write(f"prompt_template: {st.session_state.prompt_template}")
                            
                        if st.checkbox("Column Data Standardization"):                              
                            # Define your function that takes three arguments
                            def column_standardization_agent_arg(df, column, reference_column, prompt_template):
                                return f"{df}, {column}, {reference_column}, {prompt_template}"

                            # Trigger the function when a button is pressed
                            if st.button("Submit", key='ai_stan_column'):
                                ai_standardize_column = st.session_state.standardize_column(
                                    st.session_state.df, 
                                    st.session_state.column, 
                                    st.session_state.reference_column, 
                                    st.session_state.prompt_template
                                )
                                #print(ai_standardize_column)
                                #st.write(ai_standardize_column)
                                #st.dataframe(ai_standardize_column)
                                #
                                # Convert the string into a pandas DataFrame
                                dddata = StringIO(ai_standardize_column)  # Using StringIO to simulate reading from a CSV
                                dddf = pd.read_csv(dddata, sep='\t')
                                
                                
                                # Check if the DataFrame is correct
                                st.write("Preview of Standardized DataFrame:")
                                dddf = pd.DataFrame(dddf)
                                st.dataframe(dddf)  # Display the DataFrame using Streamlit

                                def convert_df(df, delimiter):
                                    # Generate CSV as a string
                                    csv_data = df.to_csv(sep=delimiter, index=False)
                                    
                                    # Replace line breaks manually if necessary (e.g., ensure `\n` breaks)
                                    csv_data = csv_data.replace('\r\n', '\n')  # Replace Windows line breaks with Unix line breaks
                                    return csv_data.encode('utf-8')
        
                                 # List of different delimiters
                                delimiters = [" ", ",","\t", ";"]
                                   
                                for delimiter in delimiters:
                                    aacsv = convert_df(dddf, delimiter)
                                    
                                    # Determine file name based on delimiter
                                    if delimiter == " ":
                                        file_name = 'standardized_df_space_delimited.csv'
                                    elif delimiter == ",":
                                        file_name = 'standardized_df_comma_delimited.csv'
                                    elif delimiter == "\t":
                                        file_name = 'standardized_df_tab_delimited.csv'
                                    elif delimiter == ";":
                                        file_name = 'standardized_df_semicolon_delimited.csv'
                                        
                                    # Provide a download button for each delimiter
                                    st.download_button(
                                        label=f"Download {file_name}",
                                        data=aacsv,
                                        file_name=file_name,
                                        mime='text/csv'
                                    )
                                    
                                    
                        if st.checkbox("Missing Data Standardization"):                              
                            # Define your function that takes three arguments
                            def column_standardization_agent_arg(df, column, reference_column, prompt_template):
                                return f"{df}, {column}, {reference_column}, {prompt_template}"
                                
                            # Trigger the function when a button is pressed
                            if st.button("Submit", key='mis_ai_stan_column'):
                                ai_standardize_column = st.session_state.standardize_missing_data(
                                    st.session_state.df, 
                                    st.session_state.column, 
                                    st.session_state.reference_column, 
                                    st.session_state.prompt_template
                                )
                                #print(ai_standardize_column)
                                #st.write(ai_standardize_column)
                                #st.dataframe(ai_standardize_column)
                                #
                                # Convert the string into a pandas DataFrame
                                dddata = StringIO(ai_standardize_column)  # Using StringIO to simulate reading from a CSV
                                dddf = pd.read_csv(dddata, sep='\t')

                                # Check if the DataFrame is correct
                                st.write("Preview of Standardized DataFrame:")
                                dddf = pd.DataFrame(dddf)
                                st.dataframe(dddf)  # Display the DataFrame using Streamlit

                                def convert_df(df, delimiter):
                                    # Generate CSV as a string
                                    csv_data = df.to_csv(sep=delimiter, index=False)
                                    
                                    # Replace line breaks manually if necessary (e.g., ensure `\n` breaks)
                                    csv_data = csv_data.replace('\r\n', '\n')  # Replace Windows line breaks with Unix line breaks
                                    return csv_data.encode('utf-8')
        
                                 # List of different delimiters
                                delimiters = [" ", ",","\t", ";"]
                                   
                                for delimiter in delimiters:
                                    aacsv = convert_df(dddf, delimiter)
                                    
                                    # Determine file name based on delimiter
                                    if delimiter == " ":
                                        file_name = 'mis_standardized_df_space_delimited.csv'
                                    elif delimiter == ",":
                                        file_name = 'standardized_df_comma_delimited.csv'
                                    elif delimiter == "\t":
                                        file_name = 'standardized_df_tab_delimited.csv'
                                    elif delimiter == ";":
                                        file_name = 'standardized_df_semicolon_delimited.csv'
                                        
                                    # Provide a download button for each delimiter
                                    st.download_button(
                                        label=f"Download {file_name}",
                                        data=aacsv,
                                        file_name=file_name,
                                        mime='text/csv'
                                    )



                    elif step == "Inflection AI Standard Feature Name":
                        st.dataframe(st.session_state.df.head())
                        if 'df' in st.session_state and st.session_state.df is not None:

#######################################
                            if 'human_data_standardization_agent' not in st.session_state:
                                st.session_state.human_data_standardization_agent = HumanDataStandardizationAgent()

                            inf_ai_stnd_df = crewailpydeamlpipeline.inflection_ai_standard_feature_name(st.session_state.df)

                            inf_ai_stnd_df
                            #session_state.human_data_standardization_agent.inflection_ai_standard_feature_name((agent, st.session_state.df)
    
#######################################    
    

                    elif step == "Normalize Data":
                        st.session_state.df = pydeamlpipeline.normalize_data(st.session_state.df)
                        st.dataframe(st.session_state.df.head())  

                    unique_key = f"step_activation_{step}_{idx}_{str(uuid.uuid4())}"


                     # Checkbox to toggle step execution and display
                    is_active = st.checkbox(
                        step, 
                        value=st.session_state.statuses.get(step, 'idle') != 'idle',
                        key=unique_key,
                        help=f"Toggle {step} execution and output display"
                        )
 

                    # Ensure radio_selections has an entry for each step, with a fallback to "Idle"
                    if step not in st.session_state.radio_selections:
                        st.session_state.radio_selections[step] = "Idle"

                    # Radio button to choose the action for the step (Idle, Accept, Reject)
                    action = st.radio(
                        f"Action for {step}",
                        options=["Idle", "Accept", "Reject"],
                        index=["Idle", "Accept", "Reject"].index(st.session_state.radio_selections[step]),
                        key=f"radio_{step}_{idx}",  # Use a simpler, unique key for radio button
                        on_change=update_radio_selections_states,
                        args=(step,),
                        help=f"Toggle {step} Select 'Accept' to proceed, 'Reject' to revert, or leave as 'Idle'."
                    )

                    # Process the action based on user selection
                    if action != st.session_state.radio_selections[step]:
                        st.session_state.radio_selections[step] = action  # Update the action
                        
                        if action == "Idle":
                            st.write(f"No changes for {step}, initial data remains the same")
                            #pass
                            
                        elif action == "Accept":
                            st.session_state.statuses[step] = "completed"
                            st.success(f"Changes for {step} have been accepted.", icon="✅")


                        elif action == "Reject":
                            st.session_state.statuses[step] = "idle"
                            st.warning(f"Changes for {step} have been rejected. Reverting to previous state.", icon="⚠️")


                    # Update statuses based on checkbox state
                    if is_active:
                        if st.session_state.statuses[step] != 'completed':
                            st.session_state.statuses[step] = 'current'
                    else:
                        st.session_state.statuses[step] = 'idle'



                        
                    # Update the pipeline visualization after accept/reject
                    pipeline_graph = pydeamlpipeline.display_horizontal_pipeline(st.session_state.steps, st.session_state.statuses)
                    components.html(pipeline_graph)



#############

#################################### Using Apache Airflow ################################
######################################################################################

        

if __name__ == "__main__":
    pydeamlpipeline = PyDEAMLPipeline()
    crewailpydeamlpipeline = PipelineCrewAIPyDEAML()
    main()
#<meta name="csrf_token" content="Ijg0ZWU5ZDRjN2Y0Yjg1Mjk0MDY0ZWRkMGJlZGEzOGVkMTVkMDYyMGQi.Z5xC2Q.0lBj-wC07mAP8l8fmwXHOh_XQik">

#def parse_response(csv_file):
    # Read the uploaded CSV file into a DataFrame
#    df = pd.read_csv(csv_file)
    
    # Display the DataFrame
#    st.subheader("Uploaded DataFrame:")
#    st.write(df)
    
#    filtered_df = dataframe_explorer(df, case=False)
    
#    return filtered_df

#def main():
#    load_dotenv()

    # Load the OpenAI API key from the environment variable
#    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_API_KEY") == "":
#        st.error("OPENAI_API_KEY is not set")
#        return

#    st.set_page_config(page_title="Data Quality with LLM")
#    st.header("Data Quality with LLM")

#    csv_file = st.file_uploader("CSV File Upload", type="csv")

#    if csv_file is not None:
        # Parse the response
#        filtered_df = parse_response(csv_file)
#        if filtered_df is not None:
#            st.dataframe(filtered_df, width='stretch')

#        user_agent = create_csv_agent(OpenAI(temperature=0), csv_file, verbose=True)

#        user_question = st.text_input("Data Quality of your CSV: ")

#        if user_question is not None and user_question != "":
#            with st.spinner(text="In progress..."):
#                response = user_agent.run(user_question)
                
#                st.write(response)


#if __name__ == "__main__":
#    main()



#def parse_response(csv_file):
    # Read the uploaded CSV file into a DataFrame
#    df = pd.read_csv(csv_file)
    
    # Display the DataFrame
#    st.subheader("Uploaded DataFrame:")
#    st.write(df)
    
#    filtered_df = dataframe_explorer(df, case=False)
    
#    return filtered_df

#def main():
#    load_dotenv()

    # Load the OpenAI API key from the environment variable
#    if os.getenv("OPENAI_API_KEY") is None or os.getenv("OPENAI_API_KEY") == "":
#        st.error("OPENAI_API_KEY is not set")
#        return

#    st.set_page_config(page_title="Data Quality with LLM")
#    st.header("Data Quality with LLM")

#    csv_file = st.file_uploader("CSV File Upload", type="csv")

#    if csv_file is not None:
        # Parse the response
#        filtered_df = parse_response(csv_file)
#        if filtered_df is not None:
#            st.dataframe(filtered_df, width='stretch')

#        user_agent = create_csv_agent(OpenAI(temperature=0), csv_file, verbose=True)

#        user_question = st.text_input("Data Quality of your CSV: ")

#        if user_question is not None and user_question != "":
#            with st.spinner(text="In progress..."):
#                response = user_agent.run(user_question)
                
#                st.write(response)


#if __name__ == "__main__":
#    main()




