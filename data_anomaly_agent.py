

import numpy as np
import pandas as pd
import os
import re
import copy
from collections import Counter
from tqdm import tqdm
from IPython.display import display, HTML, clear_output
import inflection
#from langchain_community.llms import OpenAI
from langchain_openai import OpenAI
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import ipywidgets as widgets
from ipywidgets import Checkbox, Output, VBox, HBox, Label, HTML
import inspect
from sklearn.metrics import accuracy_score
import builtins
import streamlit as st

#####
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, PrivateAttr
from crewai import Agent, Crew
from crewai.task import Task
from crewai.tasks.task_output import TaskOutput
from typing import ClassVar, Optional
#####


####################################################################################
# DataAnomalyAgent
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.impute import SimpleImputer
from sklearn.neighbors import KNeighborsClassifier
from scipy import stats


import warnings
import numpy as np
import pandas as pd
import copy
import matplotlib.pyplot as plt

from pyod.models.abod import ABOD
#from pyod.models.cblof import CBLOF
from pyod.models.loda import LODA  #High-precision local outlier factor
from pyod.models.hbos import HBOS       # Histogram-based Outlier Score
from pyod.models.iforest import IForest # Isolation Forest
from pyod.models.knn import KNN
from pyod.models.lof import LOF         # Local Outlier Factor
from pyod.models.mcd import MCD         # Minimum Covariance Determinant
from pyod.models.ocsvm import OCSVM     # One-Class SVM
from pyod.models.pca import PCA         # Principal Component Analysis
from pyod.models.lscp import LSCP
from pyod.models.inne import INNE
from pyod.models.gmm import GMM
from pyod.models.kde import KDE
from pyod.models.dif import DIF
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from pyod.models.suod import SUOD       # Scalable Unsupervised Outlier Detection #'pip install suod'
from pyod.models.qmcd import QMCD
from pyod.models.sampling import Sampling
from pyod.models.mad import MAD
from pyod.models.loci import LOCI
from pyod.models.sos import SOS
from pyod.models.sod import SOD
from pyod.models.rod import ROD


from sklearn.preprocessing import StandardScaler, LabelEncoder 

import math


# Fix anomaly
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.ensemble import RandomForestRegressor
from scipy.stats import zscore, gmean, hmean
from scipy.stats.mstats import winsorize
from scipy import stats

from statistics import mean, median, mode, StatisticsError
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import LocalOutlierFactor
from fancyimpute import KNN as FancyKNN, IterativeImputer, MatrixFactorization #pip install fancyimpute

import plotly.graph_objects as go
from plotly.subplots import make_subplots









####################################################################################


#####       BaseModel

# For Data Read
class DataFrameOutput(BaseModel):
    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


# For API 
class APIOutput(BaseModel):
    client: Optional[Any] = None
    class Config:
        arbitrary_types_allowed = True

    def get_client_repr(self) -> str:
        if self.client is not None:
            client_str = repr(self.client)
            return client_str.replace('APIOutput(client=', 'OpenAI', 1).strip()
        return 'No client available'


# For Standardized Data 
class StandardizedDataFrameOutput(BaseModel):
    standardized_df: pd.DataFrame

    def __init__(self):
        super().__init__(
            role="Data Fetcher", 
            goal="Fetch and read data of different types", 
            verbose=True, 
            backstory="Fetching data from file for standardization"
        )
    
    class Config:
        arbitrary_types_allowed = True


'''
Agents

DataReadAgent
APIAgent
AIStandardizationAgent
HumanStandardizationAgent
DisplayAgent
    -- Tab Displays
    -- Plot Displays

    
class CorrectAPIOutput(BaseModel):
    openai_correction: dict

    class Config:
        arbitrary_types_allowed = True
'''

#############################  DataReadAgent

class DataReadAgent(Agent):
    def __init__(self):
        super().__init__(
            role="Data Fetcher", 
            goal="Fetch and read data of different types", 
            verbose=True, 
            backstory="Fetching data from file for standardization"
        )


    def read_dir_file(self, file_path, file_type=None):
        """
        read_dir_file read file(s) of different types such as 
        (CSV, Excel, JSON, and XML) from a directory and returns a Dataframe.

        Parameters:  
        -----------
        file_path (str): Defines the path to the file.
        
        Returns:
        --------
        pd.DataFrame: dataFrame containing the file's data. Output is generated when combined with method show_files_in_tabs
        """

        if file_type is None:
            _, ext = os.path.splitext(file_path)
            ext = ext.lower()
            if ext == '.csv':
                file_type = 'csv'
            elif ext in ['.xls', '.xlsx']:
                file_type = 'excel'
            elif ext == '.json':
                file_type = 'json'
            else:
                raise ValueError("Unsupported file type. Please specify the file_type parameter.")
        
        if file_type == 'csv':
            encodings = ['utf-8-sig','utf-8','utf-16','utf-16le','utf-16be','utf-32','utf-32le','utf-32be','iso-8859-1',\
                         'iso-8859-2','iso-8859-5','iso-8859-6','iso-8859-7','iso-8859-8','windows-1252',\
                         'ascii','shift_jis','euc-jp','big5','gb2312','gbk','koi8-r','macroman','iso-2022-jp'
                        ]

            for encoding in encodings:
                try:
                    # Attempt to read the file with the current encoding
                    original_data = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                    break
                except UnicodeDecodeError:
                    # If there's a decoding error, move to the next encoding
                    continue

                except UnicodeError:
                    original_data = pd.read_csv(file_path, encoding='unicode_escape', low_memory=False)

            else:
                # If none of the encodings work, try reading with error handling (ignore)
                original_data = pd.read_csv(file_path, encoding='unicode_escape', low_memory=False)
        
        
        elif file_type == 'excel':
            # Try different engines for Excel files
            try:
                original_data = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e:
                try:
                    original_data = pd.read_excel(file_path, engine='xlrd')
                except Exception as e2:
                    st.error(f"Error reading Excel file: {e}\nTried openpyxl and xlrd engines.")
                    return None
                
        elif file_type == 'json':
            original_data = pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}") #return original_data
        
        return  DataFrameOutput(data=original_data)



   # Display the data as tabs in Jupyter Notebook
    def show_directory_files_in_tabs(self, directory, rows=None):
        """
        Displays DataFrames from files in the specified directory as individual tabs in a Jupyter Notebook.
        Allows user to specify how many rows of each DataFrame to show.

        Parameters:
        -----------
        directory (str): Path to the directory containing files.
        rows (int): Number of rows to display from each DataFrame.
        """
        tab_contents = []
        file_names = []

        
        # Minimum default display 5 records
        if rows is None:
            rows = 5
        
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            
            try:
                # Read the file into a DataFrame
                data: DataFrameOutput = self.read_dir_file(file_path)

                # Limit the number of rows to display
                data_to_show = data.data.head(rows)
                
                # Create an Output widget for displaying the DataFrame
                out = widgets.Output()
                with out:
                    display(data_to_show)
                
                # Append the Output widget and filename to the list
                tab_contents.append(out)
                file_names.append(filename)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
        
        # Create a Tab widget and set titles
        tab = widgets.Tab(children=tab_contents)
        for i, name in enumerate(file_names):
            tab.set_title(i, name)
        
        display(tab)


    def show_each_file_output_in_tabs(self, sd_output):
        """
        Displays the standardized features in separate tabs for each file in the DataFrame.

        Parameters:
        -----------
        sd_output (pd.DataFrame): DataFrame containing file names, original feature names, standardized feature names,
                                  AI-contextualized feature names, and counts of similar feature names.
        """
        sd_output_each_file = sd_output['file_name'].unique()
        tab_contents = []
        tab_titles = []

        for file in sd_output_each_file:
            file_df = sd_output[sd_output['file_name'] == file]
            tab_contents.append(file_df.to_html(index=False))
            tab_titles.append(file)

        tabs = widgets.Tab()
        children = [widgets.HTML(content) for content in tab_contents]
        tabs.children = children

        for i in range(len(tab_titles)):
            tabs.set_title(i, tab_titles[i])

        display(tabs)
#############################  APIAgent

class APIAgent(Agent):
    _openai_llm: Optional[APIOutput] = None
    _existing_shortcodes: set = PrivateAttr(default_factory=set)

    def __init__(self):
        super().__init__(
            role="API call", 
            goal="Make an API call and cleanse response", 
            verbose=True, 
            backstory="Getting a clean response from a prompt, output to be standardized"
      )

        _openai_llm = APIAgent._openai_llm 
        APIAgent._existing_shortcodes = set()

        class Config:
            arbitrary_types_allowed = True
    
    def load_openai_api_key(self):
        load_dotenv()
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("API key for OpenAI is required")

        # Initialize the OpenAI client if not already done
        if self._openai_llm  is None:
            openai_client = OpenAI(api_key=api_key)
            self._openai_llm  = openai_client

        return self._openai_llm 

#############################  CorrectCleanAPIAgent

class CorrectCleanAPIAgent(APIAgent):
    #_openai_llm: Optional[APIOutput] = None
    #_existing_shortcodes: set = PrivateAttr(default_factory=set)
    #get_openai_corrections: ClassVar[None] = None
    #openai_llm: None = None
    '''
    def __init__(self):
        super().__init__(
            role="API call", 
            goal="Make an API call and cleanse response", 
            verbose=True, 
            backstory="Getting a clean response from a prompt, output to be standardized"
      )

        _openai_llm = APIAgent._openai_llm 
        APIAgent._existing_shortcodes = set()

    '''
    
    def get_openai_corrections(self, terms, _existing_shortcodes, prompt_template: str, progress_bar=None, num_responses=5):
        #if self.openai_llm is None:
        #if APIAgent.openai_llm is None:
        if self._openai_llm is None:
            self.load_openai_api_key()
        
        #all_existing_shortcodes = StandardizationAgent._existing_shortcodes.union(StandardizationAgent._existing_shortcodes)
        all_existing_shortcodes = self._existing_shortcodes.union(self._existing_shortcodes)

        openai_corrections = {}

        for term in terms:
            prompt = prompt_template.format(term=term)
            responses = []

            for _ in range(num_responses):
                response = self._openai_llm .invoke(input=prompt)
                cleaned_response = self.clean_response(response)
                # Ensure the abbreviation is unique and not already in use
                if cleaned_response in all_existing_shortcodes:
                    continue  # Skip this response if it’s already used

                responses.append(cleaned_response)

            if responses:
                # Determine the most common response (mode)
                mode_response = Counter(responses).most_common(1)[0][0]
                openai_corrections[term] = mode_response
                self._existing_shortcodes.add(mode_response)  # Track this abbreviation globally

            if progress_bar:
                progress_bar.set_postfix_str(f"Processing = {term}")
                progress_bar.update(1)

        return openai_corrections


    def clean_response(self, response):
        return response.strip().replace('\n', ' ').replace('"', '').replace("'", '').replace("Standardization", '').replace("standardized term: ",'').strip()


#############################  ColumnStandardizationAgent

class ColumnStandardizationAgent(CorrectCleanAPIAgent):  
    ai_standardize_column: dict = Field(default_factory=dict)
    
    
    def standardize_column(self, data: pd.DataFrame, column: str, reference_column: str, prompt_template: str) -> pd.Series:

        """
        Standardizes the column names using AI.
        """
        
        self.ai_standardize_column = {}
        terms = [term for term in data[reference_column] if isinstance(term, str) and pd.notna(term)]
        _existing_shortcodes = set(data[column].dropna())
        
        with tqdm(total=len(terms), desc=f"Standardizing the feature '{column}'", unit="term", ncols=135) as pbar:
            openai_corrections = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template, pbar)
            self.ai_standardize_column.update(openai_corrections)
            ai_standardize_column = [
                openai_corrections.get(term, shortcode) if isinstance(term, str) and pd.notna(term) else shortcode 
                for term, shortcode in zip(data[reference_column], data[column])
            ]
            
            pbar.update(len(terms) - pbar.n) # display progress

        return ai_standardize_column


    
    def standardize_missing_data(self, data: pd.DataFrame, column: str, reference_column: str, prompt_template: str) -> pd.Series:
        """
        Standardizes the column values using AI, with a solution for resolving missing data automatically.
        """
    
        self.ai_standardize_missing_data = {}
        
        # Detect non-missing terms for standardization
        terms = [term for term in data[reference_column] if isinstance(term, str) and pd.notna(term)]
        _existing_shortcodes = set(data[column].dropna())
        
        # Detect missing data
        missing_indices = data[column].isna()
        
        # Start progress bar
        with tqdm(total=len(data), desc=f"Standardizing the feature '{column}'", unit="term", ncols=135) as pbar:
            # Get AI suggestions for non-missing terms
            openai_corrections = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template, pbar)
            self.ai_standardize_missing_data.update(openai_corrections)
            
            # Initialize an empty list to store standardized column values
            ai_standardize_missing_data = []
            
            # Loop through each entry in the column to standardize or fill missing values
            for idx, (term, _existing_shortcodes) in enumerate(zip(data[reference_column], data[column])):
                
                # Handle non-missing data using AI standardization
                if pd.notna(_existing_shortcodes):
                    standardized_value = openai_corrections.get(term, _existing_shortcodes)
                    ai_standardize_missing_data.append(standardized_value)
                    
                # Handle missing data by generating an AI solution
                else:
                    if isinstance(data[column].dtype, pd.api.types.CategoricalDtype) or isinstance(data[column].dtype, object):
                        # If it's categorical or text data
                        prompt = prompt_template.replace("{term}", "the context of the column and reference data")
                        ai_suggestion = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion)
                    
                    elif pd.api.types.is_numeric_dtype(data[column]):
                        # If it's numeric data, suggest the mean/median or AI-based imputation
                        mean_value = data[column].mean()
                        prompt_template = "Based on other data, suggest a numeric value."
                        ai_suggestion = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion if ai_suggestion else mean_value)
                    
                    elif pd.api.types.is_datetime64_any_dtype(data[column]):
                        # If it's datetime data, suggest a reasonable date or use AI-based imputation
                        most_common_date = data[column].mode()[0] if not data[column].mode().empty else pd.Timestamp.now()
                        prompt_template = "Based on other data, suggest a numeric value."
                        ai_suggestion = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion if ai_suggestion else most_common_date)
                    
                    else:
                        # For any other type, retain the original missing value
                        ai_standardize_missing_data.append(pd.NA)
                
                # Update the progress bar
                pbar.update(1)
            
            pbar.update(len(data) - pbar.n)  # Ensure progress is complete
    
        return pd.Series(ai_standardize_missing_data, index=data.index)




class HumanDataStandardizationAgent(ColumnStandardizationAgent):   
    original_data: None = None
    cleaned_data: None = None
    human_ai_data: None = None 
    standardize_human_ai_data: None = None 
    ai_standardize_column: None = None 
    ai_standardize_missing_data: None = None
    selected_column: None = None 
    reference_column: None = None 
    prompt_template: None = None 
    human_corrections: dict = {}
    accuracy_results: dict = {}

    def inflection_ai_standard_feature_name(self, df):
        """
        Create standard feature name in a dataframe using both inflection and Langchain LLM (AI).
        
        Parameters:
        -----------
        df (pd.DataFrame): The input dataframe with original feature names.
        
        Returns:
        --------
        pd.DataFrame: DataFrame with original, inflection-standardized, and LLM-standardized column names.
                                  original_feature,	inflection Standardization	AI Standardization
        """
        standardized_data = []

        # Prompt the user to input the inflection standardization method
        inflection_method = input("Enter the inflection method (e.g., 'underscore', 'camelize', 'dasherize', 'humanize', 'titleize', 'pluralize', 'singularize', 'parameterize')").strip()
        if not hasattr(inflection, inflection_method):
            raise ValueError(f"Invalid inflection method: {inflection_method}")
        inflection_standardized_feature = [getattr(inflection, inflection_method)(col) for col in df.columns]

        # Prompt the user to input the AI standardization prompt template
        prompt_template = input("Enter the AI prompt (use '{term}' as a placeholder for feature names): ")
        if not prompt_template:
            raise ValueError("Prompt template cannot be empty.")
        
        ai_standardized_features = self.get_openai_corrections(inflection_standardized_feature, set(), prompt_template)

        for original_feature_name, inflected_name in zip(df.columns, inflection_standardized_feature):
            ai_standardized_feature = ai_standardized_features.get(inflected_name, inflected_name)

            # Append to the list
            standardized_data.append({
                'original_feature_name': original_feature_name,
                'inflection_standardized_feature': inflected_name,
                'ai_standardized_feature': ai_standardized_feature
            })
            
        
        # Convert the list to a DataFrame
        rtn_df = pd.DataFrame(standardized_data)

        # Display the result before prompting for corrections
        print("\nStandardized Features:")
        display(HTML(rtn_df.to_html()))

        # Human correction of AI-standardized names
        self._human_inflxn_ai_correction(rtn_df)
        return rtn_df


    def _human_inflxn_ai_correction(self, df):
        """
        Allows the user to manually correct the AI-standardized feature names.

        Parameters:
        -----------
        df (pd.DataFrame): DataFrame with original, inflection-standardized, and AI-standardized columns.
        """

        
        
        while True:
            adjust = input("\nDo you want to adjust any AI standardized values? (y/n): ").strip().lower()
            if adjust == 'y':
                index = int(input("Enter the index of the row to adjust: "))
                new_value = input(f"Enter the new value for '{df.at[index, 'original_feature_name']}': ").strip()

                if 0 <= index < len(df):
                    df.at[index, 'ai_standardized_feature'] = new_value
                    print(f"Updated '{df.at[index, 'original_feature_name']}' to '{new_value}'")
                else:
                    print(f"Index {index} is out of range.")
            else:
                break
                

    def apply_human_inflxn_ai_standard_feature_name(self, orig_df, feature_name_df, export_path=None):
        # User to decide on the standard version to use
        standard = input("The standard feature to use ('inflection_standardized_feature' or 'ai_standardized_feature'): ").strip().lower()
        
        # Validate the user input
        if standard not in ['inflection_standardized_feature', 'ai_standardized_feature']:
            raise ValueError("Invalid standard. Choose 'inflection_standardized_feature' or 'ai_standardized_feature'.")
        
        # Map original feature names to the chosen standardization standard
        feature_map = feature_name_df.set_index('original_feature_name')[standard].to_dict()
        
        # Rename columns in the original DataFrame using the feature map
        df_with_standard_feature = orig_df.rename(columns=feature_map)
        
        # Export the updated DataFrame if a path is provided
        if export_path:
            df_with_standard_feature.to_csv(export_path, index=False)
            print(f"Updated DataFrame saved to {export_path}")
        
        return df_with_standard_feature

     
    def _plot_accuracy(self, column_name, is_human_corrected=False):
        """
        Generates and displays a bar chart showing the accuracy.
        """
        total_values = len(self.original_data[column_name])

        # Initial accuracies based on unchanged values
        orig_accuracy = (self.original_data[column_name] == self.human_ai_data[column_name]).sum() / total_values * 100
        ai_corrected_accuracy = (self.cleaned_data[column_name] == self.human_ai_data[column_name]).sum() / total_values * 100
        human_corrected_accuracy = 100  # Initially 100% as human accuracy is the baseline

         
        if is_human_corrected:
            # After human corrections
            #human_corrected_accuracy = (self.human_ai_data[column_name] == self.original_data[column_name]).sum() / total_values * 100
            ai_corrected_accuracy = (self.human_ai_data[column_name] == self.cleaned_data[column_name]).sum() / total_values * 100
            orig_accuracy = (self.human_ai_data[column_name] == self.original_data[column_name]).sum() / total_values * 100
        else:
            ai_corrected_accuracy = human_corrected_accuracy  # No change initially

        # Store results
        self.accuracy_results[column_name] = {
            'Original_Accuracy': orig_accuracy,
            'AI_Accuracy': ai_corrected_accuracy,
            'Human_Accuracy': human_corrected_accuracy
        }
        
        # Plot accuracies
        plt.figure(figsize=(5, 3))
        #bar_width = 0.25
        plt.bar(['Original\nAccuracy','AI\nAccuracy', 'Human\nAccuracy'], 
                [orig_accuracy, ai_corrected_accuracy, human_corrected_accuracy], 
                color=['#FF9999', '#99CCFF', '#99FF99'])
       
        plt.ylim(0, 100)
        plt.ylabel('Accuracy (%)')
        #plt.xticks(rotation=45)
        plt.title(f'Accuracy for {column_name}')
        
        plt.show()

        # Print final accuracies
        print("\nUpdated Accuracy")
        print(f"Updated Original for {column_name}: {orig_accuracy:.2f}%")
        print(f"Updated AI Accuracy for {column_name}: {ai_corrected_accuracy:.2f}%")
        print(f"Updated Human Accuracy for {column_name}: {human_corrected_accuracy:.2f}%")

    
    def _plot_final_accuracy(self):

        """
        Generates a bar chart showing the final accuracy for all columns, including both AI-standardized and human-corrected data.
        """
        orig_accuracies = []
        ai_accuracies = []
        human_accuracies = []
        columns = []

        for column, accuracies in self.accuracy_results.items():
            columns.append(column)
            orig_accuracies.append(accuracies['Original_Accuracy'])
            ai_accuracies.append(accuracies['AI_Accuracy'])
            human_accuracies.append(accuracies['Human_Accuracy'])

        # Plot final accuracies
        plt.figure(figsize=(5, 3))
        bar_width = 0.245
        spacing = 0.05 
        index = range(len(columns))

        plt.bar(index, orig_accuracies, bar_width, label='Original', color='#FF9999')
        plt.bar([i + bar_width+ spacing for i in index], ai_accuracies, bar_width, label='AI', color='#99CCFF')
        plt.bar([i + (bar_width + spacing) * 2 for i in index], human_accuracies, bar_width, label='Human', color='#99FF99')
       
        plt.ylim(0, 100)
        plt.xlabel('Columns')
        plt.ylabel('Accuracy (%)')
        plt.title('Final Accuracy Comparison')
        plt.xticks([i + bar_width for i in index], columns)
        plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), ncol=3)# upper right, upper left, lower left, lower right, center, best
        plt.show()
        
        # Calculate overall accuracy
        #overall_orig_accuracy = sum(orig_accuracies) / len(orig_accuracies)
        #overall_ai_accuracy = sum(ai_accuracies) / len(ai_accuracies)
        #overall_human_accuracy = sum(human_accuracies) / len(human_accuracies)

                # Check if there are any accuracies to calculate
        if len(orig_accuracies) > 0:
            overall_orig_accuracy = sum(orig_accuracies) / len(orig_accuracies)
        else:
            overall_orig_accuracy = 0  # or another default value, if necessary
    
        if len(ai_accuracies) > 0:
            overall_ai_accuracy = sum(ai_accuracies) / len(ai_accuracies)
        else:
            overall_ai_accuracy = 0  # or another default value
    
        if len(human_accuracies) > 0:
            overall_human_accuracy = sum(human_accuracies) / len(human_accuracies)
        else:
            overall_human_accuracy = 0  # or another default value

        print(f"\nOverall Original Accuracy: {overall_orig_accuracy:.2f}%")
        print(f"Overall AI Standardized Accuracy: {overall_ai_accuracy:.2f}%")
        print(f"Overall Human Corrected Accuracy: {overall_human_accuracy:.2f}%")    


        # Now you can plot the accuracies (or other logic)
        plt.bar(['Original', 'AI', 'Human'], 
                [overall_orig_accuracy, overall_ai_accuracy, overall_human_accuracy])
        plt.ylabel('Accuracy')
        plt.title('Final Accuracy Comparison')
        plt.show()

    
    def human_ai_standardized_data(self, data):
        """
        Manually correct data and standardize it further.
        """

        # Initialize dataframes, they are all at this stage a copy of the original data 
        self.original_data =data.copy()
        self.cleaned_data=data.copy()
        self.human_ai_data=self.cleaned_data.copy()
        
        '''
        missing_cols = self.missing_data_report[self.missing_data_report['Number of Missing Data'] > 0]['Column'].values
        if len(missing_cols) == 0:
            print("No columns with missing data.")
            break

        print("The columns with missing data\n")
        df_missing_cols = self.detect_missing_data_task(self.original_data)
        df_missing_cols
        '''

        
        while True:           
            data_cols = self.original_data.columns.tolist()
            # bold tags
            bst = "\033[1m"
            bld = "\033[0m"
            
            # Cols per line
            col_num = 5
            
            # Bold title
            print(f"{bst}List of Columns:{bld}")
            
            # Loop through the columns list and print 5 columns per line
            for i in range(0, len(data_cols), col_num):
                print(", ".join(data_cols[i:i + col_num]))

            # Select column o
            selected_column = input("Enter the column to adjust or 'exit' to skip").strip()
            
            # Check if user wants to exit
            if selected_column.lower() == 'exit':
                break
    
            # Validate the selected column
            if selected_column not in data.columns:
                print(f"'{selected_column}' is not part of the DataFrame columns. Please enter a correct column name.")
                continue  # Re-prompt the user to enter a valid column name

            
            reference_column = input("Enter the reference column for prompt (or if empty use the 'selected column'): ").strip()

            # If the reference column is empty, use the selected column as the reference column
            if not reference_column:
                reference_column = selected_column

            # Validate the reference column
            if reference_column not in data.columns:
                print(f"'{reference_column}' is not part of the DataFrame columns. Please enter a correct column name.")
                continue  # Re-prompt the user to enter a valid reference column
                

            prompt_template = input("Enter the prompt template (use '{term}' as placeholder for terms): ")
          
            '''
                Some example prompt_template: 
                
                Provide the standard abbreviation for the following term: '{term}'
                Provide the standard abbreviation for the following medical term: '{term}'
                Provide the standard abbreviation for the medical term: '{term}'
                Provide the standard abbreviation for the term: '{term}'
                Provide the standard abbreviation for '{term}'
                Provide the standard medical abbreviation for '{term}'
                Provide the standard medical abbreviation for the following term: '{term}'
                        
            '''

            # Assuming a wrong prompt was input, option to accept prompt results
            while True:            
                if selected_column and reference_column:
                    self.cleaned_data[selected_column] = self.standardize_column(
                        self.cleaned_data, selected_column, reference_column, prompt_template
                    )
                elif selected_column:
                    self.cleaned_data[selected_column] = self.standardize_column(
                        self.cleaned_data, selected_column, selected_column, prompt_template
                    )
    
                # Update human_ai_data as AI-corrected initially
                self.human_ai_data = copy.deepcopy(self.cleaned_data)
    
    
                # Create a DataFrame with original, AI-standardized, and human-corrected columns side by side
                comparison_df = pd.DataFrame({
                    reference_column: self.original_data[reference_column],
                    selected_column: self.original_data[selected_column],
                    f'AI_Standardized_{selected_column}': self.cleaned_data[selected_column],
                    f'Human_Corrected_{selected_column}': self.human_ai_data[selected_column]
                })
    
                # Display the side-by-side comparison
                print("\nComparison of Original, AI Standardized, and Human Corrected Data:")
                display(HTML(comparison_df.to_html()))


                # Ask if the results are acceptable
                is_acceptable = input("Looking at the AI-standardized results, is the prompt acceptable? (y/n): ").strip().lower()
                if is_acceptable == 'y':
                    break  # Exit loop if results are accepted
                else:
                    prompt_template = input("Enter a new prompt template (use '{term}' as placeholder for terms): ")
    
            
            # Calculate initial accuracies and plot
            initial_ai_accuracy = {col: 100 for col in self.cleaned_data.select_dtypes(include=['object']).columns}
            self._plot_accuracy(selected_column)

            # Human correction phase
            adjust = input("Do you want to adjust any values? (y/n): ").strip().lower()
            if adjust == 'y':
                while True:
                    idx = int(input("Enter the index to adjust: "))
                    new_value = input("Enter the new value: ").strip()

                    
                    if 0 <= idx < len(self.cleaned_data):
                        original_term = self.cleaned_data.at[idx, selected_column]
                        self.human_ai_data.at[idx, selected_column] = new_value

                        self.human_corrections[(selected_column, original_term)] = new_value
                        # Update the comparison DataFrame
                        comparison_df.at[idx, f'Human_Corrected_{selected_column}'] = new_value

                        print("\nUpdated Comparison After Human Correction:")
                        display(HTML(comparison_df.to_html()))

                        self._plot_accuracy(selected_column, is_human_corrected=True)

                    else:
                        print(f"Index {idx} is out of range or column name '{selected_column}' is invalid.")
                    
                        
                    more_adjustments = input("Do you want to adjust another value? (y/n): ").strip().lower()
                    if more_adjustments == 'n':
                        break

            more_columns = input("Do you want to standardize another column? (y/n): ").strip().lower()
            if more_columns == 'n':
                break

            # Update cleaned_data with the current human_ai_data for the loop
            self.cleaned_data = copy.deepcopy(self.human_ai_data) 

        
        
        # Plot final accuracies
        self._plot_final_accuracy()

        # Display the final human cleaned data
        self.human_ai_data.to_excel('human_ai_data.xlsx', index=False)
        print("\nFinal Human Data:")
        display(self.human_ai_data.head()) # sample of the dataframe
        
        return self.human_ai_data # return a clean dataframe    


    

    def calculate_accuracy(self, df1, df2):
        
        # Dealing with DataFrames
        if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
            raise TypeError("Both inputs must be pandas DataFrames.")
        
        # Obtain their common columns
        common_cols = df1.columns.intersection(df2.columns)
        
        # Return 0.00 accuracy where no common column exist
        if len(common_cols) == 0:
            return 0.00
        
        # Get the flatten values per column
        values_df1 = df1[common_cols].values.flatten()
        values_df2 = df2[common_cols].values.flatten()
        
        # Ensure that both arrays are of the same length
        if len(values_df1) != len(values_df2):
            raise ValueError("The DataFrame has different lengths in their flattened arrays.")
        
        # Resolve issues with NaN and NaT values
        nan_mask = pd.isna(values_df1) & pd.isna(values_df2)
        is_equal = (values_df1 == values_df2) | nan_mask
        correct_comparison = is_equal
    
        # Determin accuracy from the flatten values
        correct_count = np.sum(correct_comparison)
        total_count = len(values_df1)
        
        # Calculate accuracy percentage
        calc_accuracy = (correct_count / total_count) * 100
    
        return round(calc_accuracy, 2)


'''
• Missing values
• Duplication of the data
• Anomalies
• Primary keys for joining tables.
'''
#class MissingDataAgent:

################ Missing Data
class MissingDataAgent(HumanDataStandardizationAgent):
    original_missing_data: None = None
    cleaned_missing_data: None = None
    human_ai_missing_data: None = None 
    standardize_human_ai_data: None = None 
    ai_standardize_column: None = None 
    ai_standardize_missing_data: None = None 
    selected_column: None = None 
    reference_column: None = None 
    prompt_template: None = None 
    human_corrections: dict = {}
    missing_data_report: None = None 
    pbar: None = None 
    _get_ai_suggestion: None = None 
    orig_accuracies: None = None 
    final_human_ai_missing_data: None = None 

    def detect_missing_data(self, data):
        """Detects missing data and generates a report."""
        self.original_missing_data = data.copy()
        self.cleaned_missing_data = self.original_missing_data.copy()
        #self.missing_data_report = None

        # ANSI escape code for bold
        bst = "\033[1m"
        bld = "\033[0m"
        
        total_rows = len(self.original_missing_data)

        missing_count = self.original_missing_data.isnull().sum()[self.original_missing_data.isnull().sum() > 0]
        missing_percentage = ((missing_count / total_rows) * 100).round(2)

        self.missing_data_report = pd.DataFrame({
            'Column': missing_count.index,
            'Number of Missing Data': missing_count.values,
            '% of Missing Data': missing_percentage.values
        })

        print("Missing Data Report:")
        print(f"{bst}Columns with missing data{bld}")
        print(self.missing_data_report.to_string(index=False))


    def fix_missing_data(self, data):
        # Store the original DataFrame
        self.original_missing_data = data.copy()
        self.cleaned_missing_data = data.copy()

        # Get user input for the value to replace missing data
        process = input("Enter the value to populate all missing data, e.g., NoData: ").strip()
        print(f"Missing values in the dataframe replaced with {process}")

        # Iterate over each column to fill missing values
        for column in self.cleaned_missing_data.columns:
            # Check if the column is a datetime type
            if pd.api.types.is_datetime64_any_dtype(self.cleaned_missing_data[column]):
                # Fill NaT values with the user-defined process value as a string
                self.cleaned_missing_data[column] = self.cleaned_missing_data[column].fillna(process)
            else:
                # Fill NaN values with the user-defined process value
                self.cleaned_missing_data[column] = self.cleaned_missing_data[column].fillna(process)

 
        return self.cleaned_missing_data
    

    def fix_missing_data_with_stats_or_fill(self, data, column, process):
        """
        Apply the specified process to a column in the DataFrame.

        Parameters:
            data (pd.DataFrame): The DataFrame with missing values.
            column (str): The column name to process.
            process (str): The process to apply ('replace', 'mean', 'median', 'mode', 'ffill', 'bfill', 'interpolate').

        Returns:
            pd.DataFrame: DataFrame with missing values fixed according to the specified process.
        """
        if process == 'replace':
            if pd.api.types.is_numeric_dtype(data[column]):
                data[column] = data[column].fillna(9999)
            else:
                data[column] = data[column].fillna('NA')
        elif process == 'mean':
            if pd.api.types.is_numeric_dtype(data[column]):
                data[column] = data[column].fillna(data[column].mean())
        elif process == 'median':
            if pd.api.types.is_numeric_dtype(data[column]):
                data[column] = data[column].fillna(data[column].median())
        elif process == 'mode':
            data[column] = data[column].fillna(data[column].mode()[0])
        elif process == 'ffill':
            data[column] = data[column].ffill()
        elif process == 'bfill':
            data[column] = data[column].bfill()
        elif process == 'interpolate':
            data[column] = data[column].interpolate()
        else:
            print(f"Process '{process}' is not recognized for column '{column}'")
        
        return data




########-------------------------------------#################
################ Data Anomaly
class DataAnomalyAgent(ColumnStandardizationAgent):
    #def __init__(self):
    original_anomaly_data: None = None
    cleaned_anomaly_data: None = None
    cleaned_anomaly_data_scaled: None = None
    model_types: None = None
    model_type: None = None
    model_list: list = []
    chosen_models: list = []
    anomaly_dict: None = None
    _models_dict: dict = {}
    fixed_anomaly_dataframe: None = None
    dataframe_of_anomalies: None = None
    human_ai_anomaly_data: None = None 
    standardize_human_ai_data: None = None 
    ai_standardize_column: None = None 
    ai_standardize_anomaly_data: None = None 
    selected_column: None = None 
    reference_column: None = None 
    prompt_template: None = None 
    human_corrections: dict = {}
    anomaly_data_report: None = None 
    pbar: None = None 
    _get_ai_suggestion: None = None 
    orig_accuracies: None = None 
    final_human_ai_anomaly_data: None = None


    def crewai_preprocess_anomaly_data(self, data):
        """
        Creates a new DataFrame by converting the data to scaled numeric values between 0 and 1.

        Parameters:
        -----------
        data (pd.DataFrame): The name of the file to process.
        processed_numeric_data (pd.DataFrame): The name of the processed DataFrame.

        Returns:
        --------
        pd.DataFrame: Scaled (0 - 1) DataFrame.
        """
        self.original_anomaly_data = copy.deepcopy(data)
        self.cleaned_anomaly_data = self.original_anomaly_data 

        # Handle missing values in numeric columns with mode first
        '''
        for col in self.original_anomaly_data.select_dtypes(include=[np.number]).columns:
            if pd.api.types.is_numeric_dtype(self.original_anomaly_data[col]):
                mode_value = self.original_anomaly_data[col].mode()
                if not mode_value.empty:
                    self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(mode_value[0])
                else:
                    self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(self.original_anomaly_data[col].median())
        '''

        for col in self.original_anomaly_data.select_dtypes(include=[np.number]).columns:
            mode_value = self.original_anomaly_data[col].mode()
            if not mode_value.empty:
                self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(mode_value[0])
            self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(self.original_anomaly_data[col].median())


        # Fill completely null numeric columns with 0
        for col in self.original_anomaly_data.select_dtypes(include=[np.number]).columns:
            if self.original_anomaly_data[col].isnull().all():
                self.original_anomaly_data[col] = 0  # Fill with 0 for numeric columns

        for col in self.original_anomaly_data.select_dtypes(include=[np.number]).columns:
            if pd.api.types.is_numeric_dtype(self.original_anomaly_data[col]):
                self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(self.original_anomaly_data[col].median())

        for col in self.original_anomaly_data.select_dtypes(include=[np.number]).columns:
            if self.original_anomaly_data[col].isnull().all():
                if pd.api.types.is_numeric_dtype(self.original_anomaly_data[col]):
                    self.original_anomaly_data[col] = 0
                else:
                    self.original_anomaly_data[col] = '0'

        for col in self.original_anomaly_data.select_dtypes(include=[np.datetime64]):
            self.original_anomaly_data[col] = self.original_anomaly_data[col].fillna(self.original_anomaly_data[col].mode()[0])

        #or col in self.original_anomaly_data.select_dtypes(include=[object]).columns:
            #elf.original_anomaly_data[col] = self.original_anomaly_data[col].fillna('missing')
            #elf.original_anomaly_data[col] = LabelEncoder().fit_transform(self.original_anomaly_data[col])

        #numeric_columns = self.original_anomaly_data.select_dtypes(include=[np.number]).columns
        #numeric_columns = self.original_anomaly_data.apply(pd.to_numeric, errors='coerce').select_dtypes(include=['number']).columns

        # Force conversion to numbers, turning dates/strings into NaN, then drop them
        numeric_df = self.original_anomaly_data.apply(pd.to_numeric, errors='coerce')
        numeric_columns = numeric_df.select_dtypes(include=['number']).dropna(axis=1, how='all').columns

        # Now scale only the truly numeric data
        



        # numeric_columns = (
        #     self.original_anomaly_data
        #     .apply(pd.to_numeric, errors='coerce')
        #     .select_dtypes(include=['number'])
        #     .columns
        #     )

        scaler = StandardScaler()
        anomaly_data_scaled = scaler.fit_transform(numeric_df[numeric_columns].fillna(0))
        # anomaly_data_scaled = scaler.fit_transform(self.original_anomaly_data[numeric_columns])
        self.cleaned_anomaly_data_scaled = pd.DataFrame(anomaly_data_scaled, columns=numeric_columns, index=self.original_anomaly_data.index)

        return self.cleaned_anomaly_data_scaled


    def _crewai_calculate_support_fraction(self, data, low_variance_threshold=0.01, num_fractions=5, min_fraction=0.5):
        self.original_anomaly_data = data.copy()
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)

        std_devs = self.cleaned_anomaly_data_scaled.std()
        low_variance_count = (std_devs < low_variance_threshold).sum()
        num_features = len(std_devs)

        if num_features > 0:
            base_fraction = max(min_fraction, 1 - low_variance_count / num_features)
            base_fraction = round(base_fraction, 2)
        else:
            base_fraction = min_fraction

        # Produce a list of support fractions for MCD model
        mcd_fractions = np.linspace(base_fraction, min(0.99, base_fraction + 0.4), num_fractions)
        
        return list(mcd_fractions)

    '''

    #def crewai_detect_anomalies(self, data, model_types):
    def crewai_detect_anomalies(self, data):
        self.original_anomaly_data = data.copy()
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)
        support_fraction = self._crewai_calculate_support_fraction(self.cleaned_anomaly_data_scaled, low_variance_threshold=0.01)
       
        model_dict = {
            "ABOD": ABOD(),
            #"CBLOF": CBLOF(),
            "HPLOF": HPLOF(),
            "HBOS": HBOS(),
            "IForest": IForest(),
            "KNN": KNN(),
            "LOF": LOF(),
            "MCD": MCD(support_fraction=support_fraction),
            "OCSVM": OCSVM(),
            "PCA": PCA(),
            "INNE": INNE(),
            "GMM": GMM(),
            "KDE": KDE(),
            "LMDD": LMDD(),
            "DIF": DIF(),
            "COPOD": COPOD(),
            "ECOD": ECOD(),
            "SUOD": SUOD(),
            "QMCD": QMCD(),
            "Sampling": Sampling(subset_size=min(20, self.cleaned_anomaly_data_scaled.shape[0])),
            "KPCA": KPCA()
        }
    
        self.anomaly_dict = {column: np.zeros(len(self.cleaned_anomaly_data_scaled), dtype=int) for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns}

        model_list = [key for key in model_dict]

                    
        # Ask the user to input the model_types
        model_types = input(f"Choose a model types to detect anomalies choose from: {', '.join(model_list)}): ")

        # Ensure the input is valid
        if model_types not in model_list:
            print(f"Invalid choice, please choose from {', '.join(model_list)}):")
            model_types = input(f"input model type")
            if model_types not in model_list:
                print("Invalid choice defaults to: KNN.")
                model_types = "KNN" # Assign the default value


        if self.model_types is None:
            self.model_types = []         

        for self.model_type in model_types:
            model = model_dict.get(self.model_type, KNN())
            for column in self.cleaned_anomaly_data_scaled.columns:
                data_column = self.cleaned_anomaly_data_scaled[column].values.reshape(-1, 1)


            for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns:
                if self.model_type in ("FeatureBagging", "Sampling"):
                    data_column = self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).values
                    model.fit(data_column)
                    predictions = model.labels_  # 0: normal, 1: anomaly
                    self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)  # Combine results
                else:
                    # For other models, reshape the current column
                    data_column = self.cleaned_anomaly_data_scaled[column].values.reshape(-1, 1)


                if self.model_type == "LOF":
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        model.fit(data_column)
                        predictions = model.labels_
                        self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)
                else:
                    model.fit(data_column)
                    predictions = model.labels_
                    self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)

        for column in self.anomaly_dict:
            self.anomaly_dict[column] = (self.anomaly_dict[column] > 0).astype(int)
    
        return self.anomaly_dict

    '''
    '''
    def crewai_detect_anomalies(self, data, chosen_models=None):
        self.original_anomaly_data = data.copy()
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)
        support_fraction = self._crewai_calculate_support_fraction(self.cleaned_anomaly_data_scaled, low_variance_threshold=0.01)

        self.model_dict = {
            "ABOD": ABOD(),
            "HPLOF": HPLOF(),
            "HBOS": HBOS(),
            "IForest": IForest(),
            "KNN": KNN(),
            "LOF": LOF(),
            "MCD": MCD(support_fraction=support_fraction),
            "OCSVM": OCSVM(),
            "PCA": PCA(),
            "INNE": INNE(),
            "GMM": GMM(),
            "KDE": KDE(),
            "LMDD": LMDD(),
            "DIF": DIF(),
            "COPOD": COPOD(),
            "ECOD": ECOD(),
            "SUOD": SUOD(),
            "QMCD": QMCD(),
            "Sampling": Sampling(subset_size=min(20, self.cleaned_anomaly_data_scaled.shape[0])),
            "KPCA": KPCA()
        }

        self.anomaly_dict = {column: np.zeros(len(self.cleaned_anomaly_data_scaled), dtype=int) 
                             for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns}

        model_list = list(self.model_dict.keys())

        # If chosen_models is not provided, prompt for user input
        if chosen_models is None:
            print("Available models and numerical reference (e.g. ABOD = 1 and KNN = 5):")
            
            num_models = len(model_list)
            num_rows = math.ceil(num_models / 5)

            for row in range(num_rows):
                for col in range(5):
                    index = row * 5 + col
                    if index < num_models:
                        model = model_list[index]
                        print(f"{index + 1:2d}. {model:<15}", end="")
                print()  # New line after each row
            print()
            
            choice = input("Enter the numerical reference of models to apply (use commas to separate multiple choices): ")
            
            chosen_models = []
            
            if choice:
                try:
                    indices = [int(x.strip()) - 1 for x in choice.split(',')]
                    for index in indices:
                        if 0 <= index < len(model_list):
                            chosen_models.append(model_list[index])
                        else:
                            print(f"Invalid number: {index + 1}. Skipping.")
                except ValueError:
                    print("Please enter valid numbers separated by commas.")

            if not chosen_models:
                print("No valid models selected. Using default model: KNN")
                chosen_models = ["KNN"]

        print("Selected models:", ", ".join(chosen_models))
        self.chosen_models = chosen_models

        # Anomaly detection logic
        for model_type in chosen_models:
            model = self.model_dict[model_type]
            
            for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns:
                if model_type in ("FeatureBagging", "Sampling"):
                    data_column = self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).values
                else:
                    data_column = self.cleaned_anomaly_data_scaled[column].values.reshape(-1, 1)

                if model_type == "LOF":
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        model.fit(data_column)
                        predictions = model.labels_
                else:
                    model.fit(data_column)
                    predictions = model.labels_

                self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)

        # Convert predictions to binary (0 or 1)
        for column in self.anomaly_dict:
            self.anomaly_dict[column] = (self.anomaly_dict[column] > 0).astype(int)

        return self.anomaly_dict
    '''

    '''
    def crewai_detect_anomalies(self, data, chosen_models=None):
        self.original_anomaly_data = data.copy()
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)
        support_fraction = self._crewai_calculate_support_fraction(self.cleaned_anomaly_data_scaled, low_variance_threshold=0.01)

        self.model_dict = {
            "ABOD": ABOD(),
            "HPLOF": HPLOF(),
            "HBOS": HBOS(),
            "IForest": IForest(),
            "KNN": KNN(),
            "LOF": LOF(),
            "MCD": MCD(support_fraction=support_fraction),
            "OCSVM": OCSVM(),
            "PCA": PCA(),
            "INNE": INNE(),
            "GMM": GMM(),
            "KDE": KDE(),
            "LMDD": LMDD(),
            "DIF": DIF(),
            "COPOD": COPOD(),
            "ECOD": ECOD(),
            "SUOD": SUOD(),
            "QMCD": QMCD(),
            "Sampling": Sampling(subset_size=min(20, self.cleaned_anomaly_data_scaled.shape[0])),
            "KPCA": KPCA()
        }

        self.anomaly_dict = {column: np.zeros(len(self.cleaned_anomaly_data_scaled), dtype=int) 
                             for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns}

        # If chosen_models is not provided, prompt for user input
        if chosen_models is None:
            model_list = list(self.model_dict.keys())
            
            print("Available models and numerical reference (e.g. ABOD = 1 and KNN = 5):")
            
            num_models = len(model_list)
            num_rows = math.ceil(num_models / 5)

            for row in range(num_rows):
                for col in range(5):
                    index = row * 5 + col
                    if index < num_models:
                        model = model_list[index]
                        print(f"{index + 1:2d}. {model:<15}", end="")
                print()  # New line after each row
            print()
            
            choice = input("Enter the numerical reference of models to apply (use commas to separate multiple choices): ")
            
            chosen_models = []
            
            if choice:
                try:
                    indices = [int(x.strip()) - 1 for x in choice.split(',')]
                    for index in indices:
                        if 0 <= index < len(model_list):
                            chosen_models.append(model_list[index])
                        else:
                            print(f"Invalid number: {index + 1}. Skipping.")
                except ValueError:
                    print("Please enter valid numbers separated by commas.")

        if not chosen_models:
            print("No valid models selected. Returning empty anomaly dictionary.")
            return {}  # Return an empty dictionary instead of None

        print("Selected models:", ", ".join(chosen_models))
        self.chosen_models = chosen_models

        # Anomaly detection logic
        for model_type in chosen_models:
            model = self.model_dict[model_type]
            
            for column in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns:
                if model_type in ("FeatureBagging", "Sampling"):
                    data_column = self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).values
                else:
                    data_column = self.cleaned_anomaly_data_scaled[column].values.reshape(-1, 1)

                if model_type == "LOF":
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        model.fit(data_column)
                        predictions = model.labels_
                else:
                    model.fit(data_column)
                    predictions = model.labels_

                self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)

        # Convert predictions to binary (0 or 1)
        for column in self.anomaly_dict:
            self.anomaly_dict[column] = (self.anomaly_dict[column] > 0).astype(int)

        return self.anomaly_dict
    '''


    def _crewai_fit_mcd_iteratively(self, data_column, support_fractions):
        support_fractions = self._crewai_calculate_support_fraction(data_column)
        
        for mcd_fractions in support_fractions:
            try:
                model = MCD(support_fractions=mcd_fractions)
                model.fit(data_column.reshape(-1, 1))
                return model, model.labels_
            except ValueError as e:
                if "covariance matrix of the support data is equal to 0" in str(e):
                    print(f"Support fraction {support_fractions} failed: {str(e)}")
                    continue
                else:
                    raise

        print(f"MCD failed for all support fractions. Skipping this column.")
        return None, np.zeros(len(data_column))


    def model_dict(self):
        return {
            "ABOD": ABOD(),
            "LODA": LODA(),
            "HBOS": HBOS(),
            "IForest": IForest(),
            "KNN": KNN(),
            "LOF": LOF(),
            "MCD": MCD(),
            "OCSVM": OCSVM(),
            "PCA": PCA(),
            "INNE": INNE(),
            "GMM": GMM(),
            "KDE": KDE(),
            "DIF": DIF(),
            "COPOD": COPOD(),
            "ECOD": ECOD(),
            "SUOD": SUOD(),
            "QMCD": QMCD(),
            #"Sampling": Sampling(subset_size=min(20, self.cleaned_anomaly_data_scaled.shape[0])),
            "Sampling": Sampling(),
            "MAD": MAD(),
            "LOCI": LOCI(),
            "SOD": SOD(),
            "SOS": SOS(),
            "ROD": ROD(),
        }
    

    def crewai_detect_anomalies(self, data, chosen_models=None, feature_names=None):
        self.original_anomaly_data = data.copy()
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)
        support_fraction = self._crewai_calculate_support_fraction(self.cleaned_anomaly_data_scaled, low_variance_threshold=0.01)

        #model_dict = self._models_dict


        self._models_dict = {
            "ABOD": ABOD(),
            "LODA": LODA(),
            "HBOS": HBOS(),
            "IForest": IForest(),
            "KNN": KNN(),
            "LOF": LOF(),
            "MCD": MCD(),
            "OCSVM": OCSVM(),
            "PCA": PCA(),
            "INNE": INNE(),
            "GMM": GMM(),
            "KDE": KDE(),
            "DIF": DIF(),
            "COPOD": COPOD(),
            "ECOD": ECOD(),
            "SUOD": SUOD(),
            "QMCD": QMCD(),
            #"Sampling": Sampling(subset_size=min(20, self.cleaned_anomaly_data_scaled.shape[0])),
            "Sampling": Sampling(),
            "MAD": MAD(),
            "LOCI": LOCI(),
            "SOD": SOD(),
            "SOS": SOS(),
            "ROD": ROD(),
        }


        # Get all numeric columns
        #numeric_columns = self.cleaned_anomaly_data_scaled.select_dtypes(include=np.number).columns
        numeric_columns = self.cleaned_anomaly_data_scaled.apply(pd.to_numeric, errors='coerce').select_dtypes(include=['number']).columns

        # Filter user-specified feature names to include only numeric ones
        if feature_names is not None:
            numeric_feature_names = [col for col in feature_names if col in numeric_columns]
        else:
            numeric_feature_names = numeric_columns.tolist()

        self.anomaly_dict = {column: np.zeros(len(self.cleaned_anomaly_data_scaled), dtype=int) 
                             for column in numeric_feature_names}
        
        # If chosen_models is not provided, prompt for user input
        if chosen_models is None:
            #model_list = list(self.model_dict.keys())
            #model_dictionary = self.model_dict.model_dict()
            model_dictionary = self._models_dict()
            st.write("Note: anomaly detection is for numerical features: {model_dictionary}")

            model_list = list(self.model_dictionary.keys())
            
            print("Note: anomaly detection is for numerical features")
            print("Available models and numerical reference (e.g. ABOD = 1 and KNN = 5):")

            
            num_models = len(model_list)
            num_rows = math.ceil(num_models / 5)

            for row in range(num_rows):
                for col in range(5):
                    index = row * 5 + col
                    if index < num_models:
                        model = model_list[index]
                        print(f"{index + 1:2d}. {model:<15}", end="")
                print()  # New line after each row
            print()
            
            choice = input("Enter the numerical reference of models to apply (use commas to separate multiple choices): ")
            
            chosen_models = []
            
            if choice:
                try:
                    indices = [int(x.strip()) - 1 for x in choice.split(',')]
                    for index in indices:
                        if 0 <= index < len(model_list):
                            chosen_models.append(model_list[index])
                        else:
                            print(f"Invalid number: {index + 1}. Skipping.")
                except ValueError:
                    print("Please enter valid numbers separated by commas.")

        if not chosen_models:
            print("No valid models selected. Returning empty anomaly dictionary.")
            return {}  # Return an empty dictionary instead of None

        print("Selected models:", ", ".join(chosen_models))
        self.chosen_models = chosen_models

        with tqdm(total=1, desc=f"Anomaly detection with '{chosen_models}'", unit=" for column", ncols=135) as pbar:
            # Anomaly detection logic
            for model_type in chosen_models:
                model = self._models_dict[model_type]
                
                for column in numeric_feature_names:
                    #data_column = self.cleaned_anomaly_data_scaled[numeric_feature_names]
        
                    data_column = self.cleaned_anomaly_data_scaled[column].values.reshape(-1, 1)

                    predictions = np.zeros(len(data_column))

                    
                    # Case for Sampling
                    if model_type in ("Sampling"):
                        #data_column = self.cleaned_anomaly_data_scaled[numeric_feature_names]
                        model.fit(data_column)
                        predictions = model.labels_


                    # Case for LOF
                    elif model_type == "LOF":
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", UserWarning)
                            model.fit(data_column)
                            predictions = model.labels_

                    # Case for MCD

                    elif model_type == "MCD":
                        if data[column].var() == 0:
                            print(f"Ignoring column '{column}' due to zero variance.")
                            continue

                            support_fractions = self._crewai_calculate_support_fraction(data[column])

                            #for sf in support_fractions:
                                #support_fraction = sf

                            model, predictions = self._crewai_fit_mcd_iteratively(data[column].values, support_fraction)
                        
                        if model is not None:
                            self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)
                            print(f"MCD succeeded for column {column} with support_fraction {model.support_fraction}")
                        else:
                            print(f"MCD failed for column {column}. Skipping MCD for this column.")
                            continue
                        
                    else:  # Handle other models
                        try:
                            model.fit(data_column)
                            predictions = model.labels_
                            self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)
                        except Exception as e:
                            print(f"Error fitting {model_type} for column {column}: {str(e)}")
                            continue


                    self.anomaly_dict[column] = np.maximum(self.anomaly_dict[column], predictions)

                    #pbar.update(1)

            # Convert predictions to binary (0 or 1)
            for column in self.anomaly_dict:
                self.anomaly_dict[column] = (self.anomaly_dict[column] > 0).astype(int)

        # Update progress bar to indicate completion
        pbar.update(1)

        return self.anomaly_dict


    def detect_anomalies_across_models(self, data, model_types=None):
        """
        Detect anomalies using multiple models and return a DataFrame with results.

        Parameters:
        data (pd.DataFrame): The input data to analyze for anomalies.
        model_types (list, optional): List of model types to use for anomaly detection.
                                      If None, uses all available models in crewai_detect_anomalies.

        Returns:
        pd.DataFrame: A DataFrame with the count of anomalies detected by each model.
        """
        # Initialize a dictionary to store anomaly counts
        anomaly_counts = {}

        # If model_types is not provided, determine available models
        if model_types is None:
            try:
                # Call crewai_detect_anomalies once to get all available models
                all_anomalies = self.crewai_detect_anomalies(data, None)
                model_types = list(set([col.split('_')[0] for col in all_anomalies.keys()]))
            except Exception as e:
                print(f"Error getting available models: {str(e)}")
                return pd.DataFrame()

        print(f"Selected models: {', '.join(model_types)}")

        # Process each model type individually with progress tracking
        for model_type in tqdm(model_types, desc="Processing models", unit="model"):

            try:
                # Call crewai_detect_anomalies for this specific model
                anomaly_dict = self.crewai_detect_anomalies(data, [model_type])
                
                # Count total anomalies detected across all features for this model
                total_anomalies = sum(np.sum(values) for values in anomaly_dict.values())
                
                anomaly_counts[model_type] = total_anomalies
                
            except Exception as e:
                print(f"Error processing {model_type}: {str(e)}")
                anomaly_counts[model_type] = "-"

        # Create the result DataFrame
        result_df = pd.DataFrame.from_dict(anomaly_counts, orient='index', columns=['     Anomalies Detected'])
        result_df.index.name = 'Model'

        # Print the results
        print("\nModel Anomalies Detected")
        for model, count in result_df.itertuples():
            print(f"{model:<20} {count}")
            
            
        return result_df



    def combined_anomaly_detection(self, data, chosen_models=None, feature_names=None):
        # Get all numeric columns
        #numeric_columns = data.select_dtypes(include=np.number).columns
        numeric_columns = data.apply(pd.to_numeric, errors='coerce').select_dtypes(include=['number']).columns

        # Filter user-specified feature names to include only numeric ones
        if feature_names is not None:
            numeric_feature_names = [col for col in feature_names if col in numeric_columns]
        else:
            numeric_feature_names = numeric_columns.tolist()

        # If chosen_models is not provided, use all models
        if chosen_models is None:
            chosen_models = list(self._models_dict.keys())

        # Get anomalies per model using detect_anomalies_across_models
        model_anomalies = self.detect_anomalies_across_models(data[numeric_feature_names], chosen_models)

        # Get detailed anomalies per feature using crewai_detect_anomalies
        detailed_anomalies = self.crewai_detect_anomalies(data[numeric_feature_names], chosen_models)

        # Initialize results dictionary
        results = {feature: {model: 0 for model in chosen_models} for feature in numeric_feature_names}

        # Process the results
        if model_anomalies is not None and isinstance(model_anomalies, pd.DataFrame) and not model_anomalies.empty:
            for model in chosen_models:
                if model in model_anomalies.index:
                    print(model_anomalies)
                    print(f"Columns: {model_anomalies.columns}")
                    print(f"Index: {model_anomalies.index}")
                    print(f"Is DataFrame: {isinstance(model_anomalies, pd.DataFrame)}")
                    print(f"Is Series: {isinstance(model_anomalies, pd.Series)}")

                    total_anomalies = model_anomalies.loc[model]
                    #total_anomalies = model_anomalies.loc[model, 'Anomalies Detected']
                    if isinstance(total_anomalies, (int, float)) and total_anomalies > 0:
                        feature_anomalies = {feature: np.sum(detailed_anomalies[f"{model}_{feature}"]) 
                                             for feature in numeric_feature_names}
                        total_feature_anomalies = sum(feature_anomalies.values())
                        
                        if total_feature_anomalies > 0:
                            for feature in numeric_feature_names:
                                results[feature][model] = int(round((feature_anomalies[feature] / total_feature_anomalies) * total_anomalies))
                        else:
                            # If no anomalies detected in features, distribute evenly
                            anomalies_per_feature = total_anomalies // len(numeric_feature_names)
                            remaining_anomalies = total_anomalies % len(numeric_feature_names)
                            for i, feature in enumerate(numeric_feature_names):
                                results[feature][model] = anomalies_per_feature + (1 if i < remaining_anomalies else 0)
        else:
            print("No anomalies detected or error in detect_anomalies_across_models.")

        # Convert results to DataFrame
        results_df = pd.DataFrame(results).T  # Transpose to get features as rows and models as columns

        # Display results
        print("\nAnomaly Detection Results:")
        print(results_df)

        # Plot results
        plt.figure(figsize=(12, 8))
        plt.imshow(results_df.values, cmap='YlOrRd')
        plt.colorbar(label='Number of Anomalies')
        plt.xticks(range(len(results_df.columns)), results_df.columns, rotation=45, ha='right')
        plt.yticks(range(len(results_df.index)), results_df.index)
        plt.title('Anomalies Detected by Model and Feature')
        plt.tight_layout()
        plt.show()

        return results_df




    def _prompt_model_selection(self):
        model_list = list(self._models_dict.keys())
        print("Available models and numerical reference:")
        for i, model in enumerate(model_list, 1):
            print(f"{i:2d}. {model:<15}", end='' if i % 5 != 0 else '\n')
        print("\n")
        choice = input("Enter the numerical reference of models to apply (use commas to separate multiple choices): ")
        chosen_indices = [int(x.strip()) - 1 for x in choice.split(',')]
        return [model_list[i] for i in chosen_indices if 0 <= i < len(model_list)]




    def anomaly_dict_to_df(self, ano_dict):
        """
        Convert anomaly dictionary to a DataFrame and show only indices where values equal 1, that is anomalies are identified.

        Parameters:
        data_dict (dict): The input dictionary to be converted

        Returns:
        pandas.DataFrame: A DataFrame showing indices where values equal 1, with NaN for other values
        """
        # Convert anomaly dictionary to DataFrame
        dataframe = pd.DataFrame(ano_dict)
        
        # Filter for values equal to 1 and drop rows where all values are NaN
        anomaly_df_index = dataframe[dataframe == 1].dropna(how='all')
        
        anomaly_df_index = anomaly_df_index.rename_axis('Index').rename(columns={'Value': 'Anomaly_Indicator'})
        
        return anomaly_df_index


    def crewai_fix_anomalies(self, data, columns_to_fix, chosen_models=None, anomaly_dict=None):
        self.fixed_anomaly_dataframe = copy.deepcopy(data)

        if anomaly_dict is None:
            anomaly_dict = self.crewai_detect_anomalies(self.fixed_anomaly_dataframe, chosen_models=None)
        self.anomaly_dict = anomaly_dict

        fix_method_list = ['mean', 'median', 'mode', 'interpolation', 'knn', 'simple_imputer', 'moving_average', 'winsorization', 'zscore', 'quantile', 'random_forest']
        
        fix_method = input(f"Choose a fix method from: {', '.join(fix_method_list)}: ")
        if fix_method not in fix_method_list:
            print(f"Invalid choice, defaulting to: mean.")
            fix_method = "mean"

        anomaly_rows = []
        DEFAULT_DECIMAL_PLACES = 3
        
        #for column in columns_to_fix:
         #   if column in self.anomaly_dict:
        #        predictions = self.anomaly_dict[column]
          #      anomaly_indices = np.where(predictions == 1)[0]

        for column in columns_to_fix:
            if column in self.anomaly_dict.columns:
                anomaly_indices = self.anomaly_dict.index[self.anomaly_dict[column] == 1]

                if len(anomaly_indices) > 0:
                    print(f"Column: {column}, Number of anomalies: {len(anomaly_indices)}")

                    if self.fixed_anomaly_dataframe is None or self.fixed_anomaly_dataframe.empty:
                        self.fixed_anomaly_dataframe = copy.deepcopy(data)
                        
                    non_anomalous_data = self.fixed_anomaly_dataframe[column].drop(index=anomaly_indices)
                    
                    if fix_method == 'mean':
                        #dp = int(input("Enter the number of decimal places for the 'mean' value: "))
                        dp=DEFAULT_DECIMAL_PLACES
                        fix_value = round(non_anomalous_data.mean(), dp)
                    elif fix_method == 'median':
                        fix_value = non_anomalous_data.median()
                    elif fix_method == 'mode':
                        fix_value = non_anomalous_data.mode().iloc[0]
                        
                    
                    elif fix_method == 'knn':
                        # Step 1: Replace anomalies with NaN
                        self.fixed_anomaly_dataframe[column] = self.fixed_anomaly_dataframe[column].astype(float)
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = np.nan
                        
                        # Initialize KNNImputer
                        knn_imputer = KNNImputer(n_neighbors=5) # hard coded n_neighbors = 5
                        
                        # Step 2: Fit and transform the entire column (including NaNs)
                        imputed_values = knn_imputer.fit_transform(self.fixed_anomaly_dataframe[[column]])
                        
                        fix_value = imputed_values[anomaly_indices]  # Get only the imputed values for the specific anomaly indices
                        
                        # Step 4: Assign imputed values back to the DataFrame
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value.flatten()  # Ensure it is a 1D array
                        print(f"Without anomaly value(s), the Simple Imputer value for {column}: {fix_value}")


                    elif fix_method == 'simple_imputer':
                        si_strategies = ['mean', 'median', 'most_frequent', 'constant']

                        # Input the strategy
                        strategy = input(f"Choose a Simple Imputer strategy for {column} ({', '.join(si_strategies)}): ")

                        # Ensure the input is valid
                        if strategy not in si_strategies:
                            print(f"Invalid strategy. Please choose from {si_strategies}.")
                            continue  # Skip to the next column if the strategy is invalid

                        #fix_value = input(f"Enter the value to use as the constant fill for {column}: ")
                        if strategy == 'constant':
                            fix_value = input(f"Enter the value to use as the constant fill for {column}: ")
                            # Convert to numeric if needed
                            if pd.api.types.is_numeric_dtype(self.fixed_anomaly_dataframe[column]):
                                try:
                                    fix_value = float(fix_value)
                                except ValueError:
                                    print(f"Invalid numeric value: {fix_value}. Using default 9999.")
                                    fix_value = 9999
                            imputer = SimpleImputer(strategy='constant', fill_value=fix_value)
                            self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                            print(f"Without anomaly value(s), the Simple Imputer value for {column}: {fix_value}")

                        elif strategy == 'mean':
                            dp = int(input("Enter the number of decimal places for the 'mean' value: "))

                            # Use SimpleImputer with the selected strategy for 'mean', 'median', 'most_frequent'
                            imputer = SimpleImputer(strategy=strategy)                         
                            imputer.fit(non_anomalous_data.values.reshape(-1, 1))  # Fit the imputer
        
                            # Calculate the fill value using the fitted imputer
                            fix_value = imputer.transform(np.array([[np.nan]]))  # Transform a placeholder NaN

                            # If 'fix_value' is a numpy array, use np.round instead of round
                            if isinstance(fix_value, np.ndarray):
                                fix_value = np.round(fix_value, dp)  # Use np.round for arrays
                            else:
                                fix_value = round(fix_value, dp)  # Use round for scalar values
                            
                        else:
                            # Use SimpleImputer with the selected strategy for 'mean', 'median', 'most_frequent'
                            imputer = SimpleImputer(strategy=strategy)
                            
                            imputer.fit(non_anomalous_data.values.reshape(-1, 1))  # Fit the imputer
        
                            # Calculate the fill value using the fitted imputer
                            fix_value = imputer.transform(np.array([[np.nan]]))  # Transform a placeholder NaN
                            self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                            print(f"Without anomaly value(s), the Simple Imputer value for {column}: {fix_value}")

                    elif fix_method == 'moving_average':
                        # Step 1: Replace anomalies with NaN
                        self.fixed_anomaly_dataframe[column] = self.fixed_anomaly_dataframe[column].astype(float)  # Ensure the column is float type
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = np.nan  # Assign NaN to anomalies
                        window_size = input(f"Enter a positive integer rolling window size")
                        
                        try:
                            rolling_window_size = int(window_size)
                            if rolling_window_size <= 0:
                                raise ValueError("Window size must be a positive integer.")
                        except ValueError as ve:
                            print(f"Invalid input: {ve}")
                            rolling_window_size = 3  # Set a default value in case of invalid input

                        moving_avg_values = self.fixed_anomaly_dataframe[column].rolling(window=rolling_window_size, min_periods=1).mean()

                        fix_value = moving_avg_values[anomaly_indices]
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value                
                        print(f"Moving average applied on anomalies for {column}. Calculated values: {fix_value.values.flatten()}")


                    elif fix_method == 'winsorization':
                        # Get user input for the lower and upper limits of winsorization
                        try:
                            lower_limit = float(input("Enter the lower winsorization limit (as a percentage, e.g., 0.05 for 5%): "))
                            upper_limit = float(input("Enter the upper winsorization limit (as a percentage, e.g., 0.05 for 5%): "))
                            
                            # Ensure that limits are valid percentages between 0 and 1
                            if not (0 <= lower_limit <= 1 and 0 <= upper_limit <= 1):
                                raise ValueError("Limits must be between 0 and 1 (e.g., 0.05 for 5%).")
                    
                        except ValueError as ve:
                            print(f"Invalid input: {ve}. Applying default winsorization limits of 5%.")
                            lower_limit = 0.05  # default
                            upper_limit = 0.05  # default

                        anomaly_values = self.fixed_anomaly_dataframe.loc[anomaly_indices, column]
                        fix_value = winsorize(anomaly_values, limits=[lower_limit, upper_limit])
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                    
                        print(f"Winsorization applied to {column} with limits: lower={lower_limit}, upper={upper_limit}. Winsorized values: {fix_value}")

                    
                    elif fix_method == 'zscore':
                        common_zs_stats = ['mean', 'median', 'mode', 'trim_mean', 'winsorize', 'gmean', 'hmean', 'custom_percentile']
                        common_z_scores = [1.5, 2, 3, 4]
                    
                        # Ask the user to input the zs_stats
                        zs_stats = input(f"Choose a statistical method to impute anomalies for {column} ({', '.join(common_zs_stats)}): ").lower()

                        # Ensure the input is valid
                        if zs_stats not in common_zs_stats:
                            print(f"Invalid zs_stats. Please choose from {common_zs_stats}.")
                            zs_stats = input(f"Final choice of a statistical method for {column}, if invalid it defaults to mode (choose from {', '.join(common_zs_stats)}): ").lower()
                            if zs_stats not in common_zs_stats:
                                print("Invalid choice for statistical method again. Using default statistics: mode.")
                                zs_stats = 'mode'  # Assign the default value


                        z_scores_input = float(input(f"Enter Z-score threshold for {column} (choose from {', '.join(map(str, common_z_scores))}): "))          
                        if z_scores_input not in common_z_scores:
                            print(f"Invalid z_scores. Please choose from {common_z_scores}.")
                            z_scores_input = float(input(f"Final choice of Z-score threshold for {column}, if invalid it defauls to 3 (choose from {', '.join(map(str, common_z_scores))}): "))
                            if z_scores_input not in common_z_scores:
                                print("Invalid choice of Z-score threshold . Using default value: 3.")
                                z_scores_input = 3  # Assign the default value
                    
                        # Calculate Z-scores for non-anomalous data (excluding nulls)
                        non_anomalous_data_no_na = non_anomalous_data.dropna()  # Drop null values
                        z_scores = np.abs(stats.zscore(non_anomalous_data_no_na))

                    
                        # Filter data based on the Z-score threshold
                        filtered_data = non_anomalous_data_no_na[z_scores < z_scores_input]

                        if len(filtered_data) == 0:
                            print(f"No values in {column} to satisfy the Z-score threshold of {z_scores_input}. Using fallback method.")
                            filtered_data = non_anomalous_data  # Use the whole dataset (without filtering)
                            continue
                        
                        # Apply the selected statistical method (ignoring NaN)
                        if zs_stats == 'mean':
                            fix_value = filtered_data.mean()

                        # Apply the selected statistical method
                        if zs_stats == 'mean':
                            fix_value = filtered_data[z_scores < z_scores_input].mean()
                        elif zs_stats == 'median':
                            fix_value = filtered_data[z_scores < z_scores_input].median()
                        elif zs_stats == 'mode':
                            fix_value = mode(filtered_data[z_scores < z_scores_input])
                        elif zs_stats == 'trim_mean':
                            fix_value = stats.trim_mean(filtered_data[z_scores < z_scores_input], proportiontocut=0.05)
                        elif zs_stats == 'winsorize':
                            fix_value = stats.mstats.winsorize(filtered_data[z_scores < z_scores_input], limits=[0.05, 0.05]).mean()
                        elif zs_stats == 'gmean':
                            fix_value = gmean(filtered_data[z_scores < z_scores_input])
                        elif zs_stats == 'hmean':
                            fix_value = hmean(filtered_data[z_scores < z_scores_input])
                        elif zs_stats == 'custom_percentile':
                            percentile = float(input("Enter the percentile (e.g., 90 for 90th percentile): "))
                            fix_value = np.percentile(filtered_data[z_scores < z_scores_input], percentile)
                    
                        # Apply the fix value to the anomalies
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                    
                        print(f"{zs_stats} imputation applied to anomalies in {column} using Z-score threshold {z_scores_input}. Value imputed: {fix_value}")


                    elif fix_method == 'quantile':
                        self.fixed_anomaly_dataframe[column] = self.fixed_anomaly_dataframe[column].astype(float)
                        non_anomalous_data = self.fixed_anomaly_dataframe.loc[~self.fixed_anomaly_dataframe.index.isin(anomaly_indices), column]                

                        try:
                            lower_percentile = float(input("Enter the lower percentile limit (e.g., enter 5 for 5%): "))  
                            lower_percentile = round(lower_percentile/100, 2)
                            
                            upper_percentile = float(input("Enter the upper percentile limit (e.g., enter 95 for 95%): "))
                            upper_percentile = round(upper_percentile/100, 2)
                            
                            # Ensure that limits are valid percentages between 0 and 1
                            if not (0 <= lower_percentile <= 100 and 0 <= upper_percentile <= 100):
                                raise ValueError("Limits must be between 0 and 100.")
                    
                        except ValueError as ve:
                            print(f"Invalid input: {ve}. Applying default lower and upper percentile limits of 5%.")
                            lower_percentile = 0.05  # default
                            upper_percentile = 0.05  # default

                        # Calculate quantiles
                        lower_bound = non_anomalous_data.quantile(lower_percentile)
                        upper_bound = non_anomalous_data.quantile(upper_percentile)

                        fix_value = np.clip(self.fixed_anomaly_dataframe[column], lower_bound, upper_bound)
                        self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value

                        print(f"Quantile imputation applied to anomalies in {column}. Lower bound: {lower_bound}, Upper bound: {upper_bound}")


                    elif fix_method == 'random_forest':          
                        if self.fixed_anomaly_dataframe[column].dtype in [np.float64, np.int64]:
                            # Convert column to float to handle NaNs properly
                            self.fixed_anomaly_dataframe[column] = self.fixed_anomaly_dataframe[column].astype(float)
                           
                            # Create a shifted column to use as a feature
                            self.fixed_anomaly_dataframe[f'{column}_shift'] = self.fixed_anomaly_dataframe[column].shift(5)
                            
                            # Fill any NaNs in the shifted column with the mean of that column
                            self.fixed_anomaly_dataframe[f'{column}_shift'] = self.fixed_anomaly_dataframe[f'{column}_shift'].fillna(self.fixed_anomaly_dataframe[f'{column}_shift'].mean())

                            # Drop rows where the shifted column has NaN (since we can't train on those)
                            cleaned_fixed_anomaly_dataframe = self.fixed_anomaly_dataframe.dropna(subset=[f'{column}_shift'])
                   
                            # Drop rows where the target column (original column) has NaN
                            train_data_cfad = cleaned_fixed_anomaly_dataframe.dropna(subset=[column])
                                                    
                            if train_data_cfad.empty:
                                print("Limited data to train RF. Skip RF imputation.")
                            else:
                                # Prepare training data (non-anomalous data)
                                rfX_train = train_data_cfad[[f'{column}_shift']]  # Only one lag feature, reshaped as 2D
                                rfy_train = train_data_cfad[column]  # Column A as target
                            
                                # Train Random Forest model
                                rf_regressor = RandomForestRegressor()
                                rf_regressor.fit(rfX_train, rfy_train)
                            
                                # Prepare anomalous data for prediction
                                anomalous_data = self.fixed_anomaly_dataframe.loc[anomaly_indices, [f'{column}_shift']]
                                
                                if anomalous_data.empty:
                                    print("Invalid data for prediction. Skip RF imputation.")
                                else:
                                    # Predict values for anomalies using the trained Random Forest
                                    fix_value = rf_regressor.predict(anomalous_data)
                            
                                    # Replace the missing (anomalous) values in column A with the predictions
                                    self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                                    print(f"RF imputation applied to anomalies in {column}, imputed values are: {fix_value}")
                    
                        else:
                            print(f"Non-numeric data in column '{column}', no RF regression applied.")


                    else:
                        raise ValueError(f"Unsupported fix method: {fix_method}")
        
                    # Fill anomalies with the calculated fix value
 
                    self.fixed_anomaly_dataframe.loc[anomaly_indices, column] = fix_value
                    print(f"Applied {fix_method} method to fix anomalies in {column}. Fix value: {fix_value}")

                    for idx in anomaly_indices:
                        anomaly_rows.append({
                            "Index": idx,
                            "Column Name": column,
                            "Anomaly Value": data.iloc[idx][column],
                            "Fixed Anomaly Value": fix_value
                        })




        self.dataframe_of_anomalies = pd.DataFrame(anomaly_rows)
        if not self.dataframe_of_anomalies.empty:
            self.dataframe_of_anomalies = self.dataframe_of_anomalies.sort_values(by="Index").reset_index(drop=True)
            print(f"Total anomalies detected: {len(self.dataframe_of_anomalies)}")
            #print(f"Columns in dataframe_of_anomalies: {self.dataframe_of_anomalies.columns}")
            print(self.dataframe_of_anomalies)
        else:
            print("No anomalies detected. The dataframe_of_anomalies is empty.")

        return self.fixed_anomaly_dataframe


    def crewai_human_ai_anomaly_data(self, data, chosen_models=None):
        """
        Identify anomalies, apply code-based and AI-based corrections, and allow human corrections.
        """
        # Step 1: Detect anomalies (only once)
        self.fixed_anomaly_dataframe = data.copy()
        self.anomaly_dict = self.crewai_detect_anomalies(self.fixed_anomaly_dataframe, chosen_models=None)

        # Step 2: Prepare anomaly rows
        anomaly_rows = []
        for column in self.anomaly_dict:
            anomalies = self.anomaly_dict[column] > 0
            anomaly_indices = np.where(anomalies)[0]

            if len(anomaly_indices) > 0:
                # Step 3: Apply code-based corrections (only once per column)
                fixed_column = self.crewai_fix_anomalies(self.fixed_anomaly_dataframe, {column: self.anomaly_dict[column]})[column]

                for idx in anomaly_indices:
                    try:
                        original_value = data.iloc[idx][column]
                        code_corrected_value = fixed_column.iloc[idx]

                        anomaly_rows.append({
                            "Index": idx,
                            "Column Name": column,
                            "Anomaly Value": original_value,
                            "Code Corrected Value": code_corrected_value
                        })

                    except Exception as e:
                        print(f"Error accessing data at index {idx} for column {column}: {e}")

        # Step 4: Create a DataFrame from the collected anomaly data
        anomaly_data = pd.DataFrame(anomaly_rows)

        if not anomaly_data.empty:
            print(f"Total anomalies detected: {len(anomaly_data)}")
            print(anomaly_data)
        else:
            print("No anomalies detected.")

        return anomaly_data



    # # Plotting function
    # def crewai_plot_anomalies(self, data, chosen_models=None):
    #     self.original_anomaly_data = copy.deepcopy(data)
    #     self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)

    #     # Detect anomalies
    #     if chosen_models is None:
    #         chosen_models = self.chosen_models

    #     try:
    #         self.anomaly_dict = self.crewai_detect_anomalies(self.original_anomaly_data, chosen_models=chosen_models)


    #         n_cols = len(self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns)
    #         n_rows = 1
            
    #         fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 4))
    #         axes = np.atleast_1d(axes)  # Ensure axes is always an array
            
    #         for ax, column in zip(axes, self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns):
    #             anomaly_predictions = self.anomaly_dict[column]
    #             #chosen_models = self.chosen_models
    #             normal_indices = self.cleaned_anomaly_data_scaled[anomaly_predictions == 0].index
    #             anomaly_indices = self.cleaned_anomaly_data_scaled[anomaly_predictions == 1].index
                
    #             ax.scatter(normal_indices, self.cleaned_anomaly_data_scaled.loc[normal_indices, column], label='Normal', color='blue', marker='o', s=25)
    #             ax.scatter(anomaly_indices, self.cleaned_anomaly_data_scaled.loc[anomaly_indices, column], label='Anomaly', color='red', marker='x', s=25)
            
    #             #ax.set_title(f'Anomaly Detection: {column} \n Model: {chosen_models}')
    #             ax.set_title(f'Anomaly Detection: {column}\nModel: {", ".join(chosen_models)}')
    #             ax.set_xlabel('Index')
    #             ax.set_ylabel(column)
    #             #ax.grid()
    #             ax.grid(True)
    #             ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fancybox=True, shadow=True, ncol=2)
            
    #         plt.tight_layout()
    #         plt.show()

    #     except Exception as e:
    #         print(f"Error in anomaly detection: {e}")



    def crewai_plot_anomalies_plotly(self, data, chosen_models=None):
        self.original_anomaly_data = copy.deepcopy(data)
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(
            self.original_anomaly_data
        )

        if chosen_models is None:
            chosen_models = self.chosen_models

        try:
            self.anomaly_dict = self.crewai_detect_anomalies(
                self.original_anomaly_data,
                chosen_models=chosen_models
            )

            numeric_cols = self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns
            n_cols = len(numeric_cols)

            # Create subplots (1 row, multiple columns)
            fig = make_subplots(
                rows=1,
                cols=n_cols,
                subplot_titles=[
                    f"{col}<br>Model: {', '.join(chosen_models)}"
                    for col in numeric_cols
                ]
            )

            for i, column in enumerate(numeric_cols, start=1):
                anomaly_predictions = self.anomaly_dict[column]

                normal_mask = anomaly_predictions == 0
                anomaly_mask = anomaly_predictions == 1

                df = self.cleaned_anomaly_data_scaled

                # Normal points
                fig.add_trace(
                    go.Scatter(
                        x=df.index[normal_mask],
                        y=df.loc[normal_mask, column],
                        mode="markers",
                        name=f"{column} - Normal",
                        marker=dict(color="blue", size=6),
                        showlegend=(i == 1)  # show legend only once
                    ),
                    row=1,
                    col=i
                )

                # Anomaly points
                fig.add_trace(
                    go.Scatter(
                        x=df.index[anomaly_mask],
                        y=df.loc[anomaly_mask, column],
                        mode="markers",
                        name=f"{column} - Anomaly",
                        marker=dict(color="red", size=8, symbol="x"),
                        showlegend=(i == 1)
                    ),
                    row=1,
                    col=i
                )

            fig.update_layout(
                height=400,
                width=350 * n_cols,
                title="Anomaly Detection",
                template="plotly_white"
            )

            return fig

        except Exception as e:
            print(f"Error in anomaly detection: {e}")
            return None


#################


    def crewai_interactive_anomalies_plot(self, data):
        self.original_anomaly_data = copy.deepcopy(data)
        self.cleaned_anomaly_data_scaled = self.crewai_preprocess_anomaly_data(self.original_anomaly_data)

        # Create checkboxes for features and models
        feature_checkboxes = {col: widgets.Checkbox(value=False, description=col, layout=widgets.Layout(width='auto')) 
                              for col in self.cleaned_anomaly_data_scaled.select_dtypes(include=[np.number]).columns}
        
        model_checkboxes = {model: widgets.Checkbox(value=False, description=model, layout=widgets.Layout(width='auto')) 
                            for model in ["ABOD", "FeatureBagging", "HBOS", "IForest", "KNN", "LOF",
                                          "MCD", "OCSVM", "PCA", "INNE", "GMM", "KDE", "LMDD", "DIF", 
                                          "COPOD", "ECOD", "SUOD", "QMCD", "Sampling", "KPCA"]}

        output = widgets.Output()

        # Function to update plot based on selected checkbox values
        def update_plot(change):
            with output:
                output.clear_output()  # Clear previous output
                
                selected_features = [col for col, checkbox in feature_checkboxes.items() if checkbox.value]
                selected_models = [model for model, checkbox in model_checkboxes.items() if checkbox.value]

                if not selected_features:
                    print("Please select at least one feature.")
                    return
                
                # Handle model selection
                if not selected_models:
                    print("No models selected. Using default model: KNN")
                    chosen_models = ["KNN"]  # Default to KNN if no models are selected
                else:
                    chosen_models = selected_models

                print("Selected models:", ", ".join(chosen_models))

                # Detect anomalies using the original data and selected models
                anomaly_dict = self.crewai_detect_anomalies(self.original_anomaly_data[selected_features], chosen_models=chosen_models)

                # Check if anomaly_dict is empty or None
                if anomaly_dict is None or not anomaly_dict:
                    print("No anomalies detected or no valid models were used.")
                    return

                # Plot anomalies using the original data and the anomaly dictionary
                self.crewai_plot_anomalies(self.original_anomaly_data[selected_features], anomaly_dict)

        # Set up event handlers for checkboxes
        for checkbox in feature_checkboxes.values():
            checkbox.observe(update_plot, names='value')

        for checkbox in model_checkboxes.values():
            checkbox.observe(update_plot, names='value')

        # Function to chunk the feature and model checkboxes
        def chunked(iterable, chunk_size):
            return [iterable[i:i + chunk_size] for i in range(0, len(iterable), chunk_size)]

        flpc = math.ceil(len(data.columns) / 4)
        mlpc = math.ceil(len(model_checkboxes) / 5)

        # Create vertical layout for feature checkboxes (layout per column)
        chunked_feature_boxes = [VBox(chunk) for chunk in chunked(list(feature_checkboxes.values()), flpc)]

        # Create vertical layout for model checkboxes (layout per column)
        chunked_model_box = [VBox(chunk) for chunk in chunked(list(model_checkboxes.values()), mlpc)]

        layout = widgets.VBox([widgets.HTML('<b>Select Features</b>'), widgets.HBox(chunked_feature_boxes), \
                               widgets.HTML('<b>Select Models</b>'), widgets.HBox(chunked_model_box), \
                               output])

        # Display the layout
        display(layout)



###-------------------------------------###
# Normalization
# Still WIP

import pandas as pd
import ipywidgets as widgets
from IPython.display import HTML, display, clear_output

def identify_1NF_features(dataframes):
    """Identify columns in each DataFrame that qualify for 1NF normalization."""
    candidates_dict = {}
    for name, df in dataframes.items():  # Use dictionary items to get name and DataFrame
        candidates_dict[name] = [
            col for col in df.columns
            if df[col].apply(lambda x: isinstance(x, str) and ',' in x).any()
        ]
        
    candidates_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in candidates_dict.items()])).fillna('')
    candidates_df.reset_index(drop=True)
    #print("\nPotential features for 1NF normalization")
    print("\033[1m\nPotential features per DataFrame for 1NF normalization\n\033[0m")
    display(candidates_df)
    print("\n")
    return candidates_dict


def display_1NF_selection(dataframes):
    """Display checkboxes for user to choose columns for 1NF normalization."""
    candidates = identify_1NF_features(dataframes)
    selected_columns = {}
    normalized_dfs = {}
    output = widgets.Output()

    # Create checkboxes for each DataFrame's list-like columns
    for name, df in dataframes.items():
        list_like_columns = candidates.get(name, [])

        if list_like_columns:
            #checkboxes = [widgets.Checkbox(value=False, description=col) for col in list_like_columns]
            #selected_columns[name] = checkboxes

            # Display checkboxes for the current DataFrame
            #display(widgets.Label(f"Select columns for {name}:"), widgets.HBox(checkboxes))

            #display(widgets.Label('' * 15 + f"Select columns for DF:  {name}"), widgets.HBox(checkboxes))
            '''
            hbox_layout = widgets.VBox([
            widgets.HBox([
                #widgets.Label('' * 5 + f"Columns in DF: {name}"), widgets.HBox(checkboxes)
                widgets.HTML('' * 5 + f'<b>Features in DF:</b> {name}'), widgets.HBox(checkboxes)
                ])
            ])

            display(hbox_layout) 
            '''    

            n1nf_cols = math.ceil(len(df.columns) / 4)
            
            # Create checkboxes for each column
            checkboxes = [
                widgets.Checkbox(value=False, description=col, layout=widgets.Layout(width='auto'))
                for col in df.columns
            ]

            selected_columns[name] = checkboxes
        
            # Helper function to divide checkboxes into chunks
            def chunks(m, n):
                for i in range(0, len(m), n):
                    yield m[i:i + n]
        
            # Organize checkboxes into columns
            chunked_feature_boxes = [widgets.VBox(chunk) for chunk in chunks(checkboxes, n1nf_cols)]

        
            # Create layout with label and checkboxes in columns
            hbox_layout = widgets.VBox([
            widgets.HBox([
                #widgets.Label('' * 5 + f"Columns in DF: {name}"), widgets.HBox(checkboxes)
                widgets.HTML('' * 5 + f'<b>Features in DF:</b> {name}'), widgets.HBox(chunked_feature_boxes)
                ])
            ])

            display(hbox_layout) 


    # Single apply button outside the loop
    # apply_button = widgets.Button(description="Apply 1NF Normalization")
    apply_button = widgets.Button(
        description="Apply 1NF Normalization",
        style={'button_color': '#d5f5e3'}, # #4CAF50 #00FF00 #C0C0C0
        layout=widgets.Layout(margin='auto', width='auto', min_width='150px') 
    )
    
    apply_button.style.font_weight = 'bold'
  

    def _apply_normalization(button):
        with output:
            clear_output()  # Clear previous output
            normalized_dfs.clear()
            any_selected = False  # Flag to check if any columns are selected

            for df_name, checkboxes in selected_columns.items():
                selected_features = [cb.description for cb in checkboxes if cb.value]
                if selected_features:
                    any_selected = True
                    print(f"\nApplying 1NF normalization to {df_name} feature(s): {selected_features}")
                    new_df = dataframes[df_name].copy()

                    # Normalize each selected feature
                    for col in selected_features:
                        new_df[col] = new_df[col].astype(str).str.split(', ')
                        new_df = new_df.explode(col).reset_index(drop=True)

                    # Save the normalized DataFrame
                    normalized_dfs[f"n1NF_{df_name}"] = new_df
                    print(f"1NF normalization performed on {df_name}. Created new DataFrame: n1NF_{df_name}")

            # Only print once if no columns were selected across all DataFrames
            if not any_selected:
                print("\nNo selected feature for 1NF normalization.\n1NF Normalization was not applied.")

            # Display available normalized DataFrames if they exist
            if normalized_dfs:
                print("\n1NF normalization completed for selected feature(s) in DataFrame:")
                for key in normalized_dfs:
                    print(f"- {key}")


    # Attach the normalization function to the single Apply button
    apply_button.on_click(_apply_normalization)

    # Display the Apply button once, and the output area
    display(apply_button, output)

    return normalized_dfs  # Dictionary of normalized DataFrames

'''

# Example data
data1 = {
    "BookID": [1, 2, 3],
    "Title": ["Intro to DBMS", "Programming in Python", "C-sharp"],
    "Authors": ["John Dee, Jane Doe", "Alice Smith", "John Dee"],
    "Address": ["123 Main St, Apt 4", "456 Elm St, Apt 2B", "789 Oak St"],
    "Code": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "Code1": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "Codeeeeeee2": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "Codeqqqqqssssq3": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"]

}
data2 = {
    "StudentID": [101, 102, 103],
    "Courses": ["Math, Science", "Science, History", "Math"],
    "Name": ["Alice", "Bob", "Charlie"],
    "oTitle": ["Intro to DBMS", "Programming in Python", "C-sharp"],
    "oAuthors": ["John Dee, Jane Doe", "Alice Smith", "John Dee"],
    "oAddress": ["123 Main St, Apt 4", "456 Elm St, Apt 2B", "789 Oak St"],
    "oCode": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "oCode1": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "oCodeeeeeee2": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"],
    "oCodeqqqqqssssq3": ["12553 Go St, Apt 4", "T456 Elm St, Floor 5", "789 Oak St"]
}

data3 = {
    "StudentID": [101, 102, 103],
    "Courses": ["Math, Science", "Science, History", "Math"],
    "Name": ["Alice", "Bob", "Charlie"]
}
# Convert dictionaries to DataFrames
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)
df3 = pd.DataFrame(data3)
dataframes = {"df1": df1, "df2": df2, "df3": df3}  # Pass as a dictionary with names


abeeeeec=display_1NF_selection(dataframes)


for name, df in abeeeeec.items():
    print(f"\n{name}")
    display(df)

for name, df in abeeeeec.items():
    print(f"\n{name}")
    with pd.option_context('display.max_columns', None):
        display(df)


# Convert to list and access the dfs
the_dfs = list(abeeeeec.values())

# Access individual DataFrames:
n1NF_df1 = the_dfs[0]
n1NF_df2 = the_dfs[1]
n1NF_df3 = the_dfs[2]










import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
import math


class TransformTo1NF:
    def __init__(self, dataframe):
        """Initialize the transformation tool."""
        self.dataframe = dataframe
        self.original_df = dataframe.copy()
        self.transformed_df = None
        self.all_transformed_df = pd.DataFrame()
        self.transformed_dfs = []
        self.previous_states = []
        self.all_dropped_columns = []
        self.all_dropped_columns_list = []

        # Widgets
        self.columns = dataframe.columns.tolist()
        #self.melt_checkboxes = {col: widgets.Checkbox(value=False, description=col) for col in self.columns}
        self.melt_checkboxes = {
                col: widgets.Checkbox(value=False, description=col, layout=widgets.Layout(width='auto'))
                for col in self.columns
                }
        self.var_name_text = widgets.Text(value="End Var Name with _Variable", description="Var Name:")
        self.value_name_text = widgets.Text(value="Rename to common Value Name", description="Value Name:")
        self.apply_button = widgets.Button(description="  Apply Transformation", layout=widgets.Layout(min_width='150px'))
        self.save_button = widgets.Button(description="  Save Transformed DataFrame", layout=widgets.Layout(width='210px'))
        self.undo_button = widgets.Button(description="  Undo Last Saved Transformation", style={'button_color': '#d5f5e3'}, layout=widgets.Layout(width='230px'))
        self._undo_all_button = widgets.Button(description="  Undo All Transformations", style={'button_color': '#f57842'}, layout=widgets.Layout(width='205px'))
        self.output = widgets.Output()

        self._setup_callbacks()

    def _setup_callbacks(self):
        """Link button actions to their respective methods."""
        self.apply_button.on_click(self.apply_1NF_transformations)
        self.save_button.on_click(self._save_transformed_df)
        self.undo_button.on_click(self._undo_last_save)
        self._undo_all_button.on_click(self._undo_all)

    def apply_1NF_transformations(self, b):
        """Apply the melting transformation."""
        with self.output:
            clear_output()
            selected_columns = [
                col for col, cb in self.melt_checkboxes.items()
                if cb.value and col not in self.all_dropped_columns
            ]
            if selected_columns:
                non_id_vars = [col for col in self.original_df.columns if col not in selected_columns]
                var_name = self.var_name_text.value
                value_name = self.value_name_text.value

                self.transformed_df = pd.melt(
                    self.original_df,
                    id_vars=non_id_vars,
                    value_vars=selected_columns,
                    var_name=var_name,
                    value_name=value_name
                )
                display(self.transformed_df)
            else:
                print("Please select at least one column to melt or no columns left to select.")

    def _save_transformed_df(self, b):
        """Save the current transformed DataFrame."""
        with self.output:
            clear_output()
            if self.transformed_df is not None:
                self.previous_states.append(self.transformed_df.copy())
                print("Current state saved. Undo this state if you prefer.")
                print(f"Total saved states: {len(self.previous_states)}")

                # Deduplicate rows and manage dropped columns
                self.transformed_df = self.transformed_df.drop_duplicates()
                selected_columns = [
                    col for col, cb in self.melt_checkboxes.items()
                    if cb.value
                ]
                self.all_dropped_columns.append(selected_columns)
                self.all_dropped_columns_list = [i for sublist in self.all_dropped_columns for i in sublist]

                # Concatenate transformations side by side
                self.transformed_dfs.append(self.transformed_df)
                if isinstance(self.all_transformed_df, pd.DataFrame) and not self.all_transformed_df.empty:
                    self.transformed_df = pd.concat([self.transformed_df, self.all_transformed_df], axis=1)
                else:
                    self.all_transformed_df = self.transformed_df.copy()

                self.all_transformed_df = self.transformed_df.copy()

                print("\nSaved button actions:\nDuplicate features removed\nSelected columns dropped\nSaved the current transformed DataFrame\n")
                print(f"\nTotal saved transformations: {len(self.transformed_dfs)}\n")
                
                # Display the transformed DataFrame, list of dropped columns, and master DataFrame
                self.transformed_df = self.transformed_df.drop(columns=self.all_dropped_columns_list, errors='ignore') # action drops the features
                self.transformed_df = self.transformed_df.loc[:, ~self.transformed_df.columns.duplicated()] # dedup updated df
                
                _suffix = '_Variable'
                self.transformed_df = self.transformed_df.drop(columns=[col for col in self.transformed_df.columns if col.endswith(_suffix)], errors='ignore')
                
                print(f"Transformation saved. Total saved transformations: {len(self.transformed_dfs)}")
                
                display(self.transformed_df)

                #return self.transformed_df
                
            else:
                print("No transformation has been applied yet. Please apply a transformation first.")

    def _undo_last_save(self, b):
        """Undo the last saved transformation."""
        with self.output:
            clear_output()
            if self.previous_states:
                self.transformed_df = self.previous_states.pop()
                print("Reverted to the previous state:")
                display(self.transformed_df)
                print(f"Remaining undo states: {len(self.previous_states)}")
            else:
                print("No previous state to revert to. Undo action cannot be performed.")
                

    def _undo_all(self, b):
        """Undo all the saved transformation and restore back to the original dataframe."""
        with self.output:
            clear_output()

            # Restore the original DataFrame and reset state
            self.transformed_df = self.original_df.copy()
            self.previous_states.clear()
            self.selected_columns = []
            self.all_dropped_columns = []
            self.all_transformed_df = pd.DataFrame()

            # Reset checkboxes
            for checkbox in self.melt_checkboxes.values():
                checkbox.value = False

            print("\nAll transformations removed and,\nDataFrame restored to original state.\nThe head is displayed for reference.\n")
            display(self.original_df.head(5))

    
      
    def display_ui(self):
        """Display the interactive UI for transformations."""
        column_selector = widgets.VBox([widgets.Label("Select columns to melt:"), 
                                        widgets.VBox(list(self.melt_checkboxes.values()))])
        config_widgets = widgets.VBox([self.var_name_text, self.value_name_text])
        buttons = widgets.HBox([self.apply_button, self.save_button, self.undo_button, self._undo_all_button])

        display(widgets.VBox([column_selector, config_widgets, buttons, self.output]))

    

    def display_ui(self):
        """Display the interactive UI for transformations."""
        # Calculate the number of chunks for checkboxes based on columns in the DataFrame
        n1nf_cols = math.ceil(len(self.original_df.columns) / 4)
        
        # Helper function to divide checkboxes into chunks
        def chunks(m, n):
            for i in range(0, len(m), n):
                yield m[i:i + n]

        # Organize checkboxes into chunks
        chunked_feature_boxes = [widgets.VBox(chunk) for chunk in chunks(list(self.melt_checkboxes.values()), n1nf_cols)]

        # Column selector with checkboxes in chunks
        column_selector = widgets.VBox([
            widgets.Label("Select columns to melt:"),
            widgets.HBox(chunked_feature_boxes)  # Arrange the checkboxes in a horizontal box
        ])

        # Configuration widgets (Var Name and Value Name)
        config_widgets = widgets.VBox([self.var_name_text, self.value_name_text])

        # Buttons (Apply, Save, Undo, Undo All)
        buttons = widgets.HBox([self.apply_button, self.save_button, self.undo_button, self._undo_all_button])

        # Display the entire layout
        display(widgets.VBox([column_selector, config_widgets, buttons, self.output]))


transformer1NF = TransformTo1NF(df)
transformer1NF.display_ui()

OneNF = transformer1NF.transformed_df
OneNF

#pd.set_option('display.max_columns', 100)
#pd.set_option('display.max_rows', 100)
'''




































###-------------------------------------###



########-------------------------------------#################


class Crew:
    def __init__(self, agents=None, tasks=None, verbose=False, planning=True):
        self.agents = agents or []
        self.tasks = tasks or []
        self.verbose = verbose
        self.planning = planning

    def run_task(self, task, *args, **kwargs):
        if self.verbose:
            print(f"Running task: {task.description}")
        
        agent = task.agent
        method = getattr(agent, task.method_name)
        
        if callable(method):
            output = method(*args, **kwargs)
            return output
        else:
            raise AttributeError(f"The method {task.method_name} is not callable on {agent}")


class Task:
    def __init__(self, description, expected_output, agent, output_pydantic, method_name):
        self.description = description
        self.expected_output = expected_output
        self.agent = agent
        self.output_pydantic = output_pydantic
        self.method_name = method_name  # Add method_name to the task attributes


# Create a sequence of tasks that the crew will execute.

data_read_task = Task(
    description="Read data from a file path, use .data for DataFrame",
    expected_output="Original data",
    agent=DataReadAgent(),
    output_pydantic=DataFrameOutput,
    method_name='read_dir_file'
)

load_openai_api_key_task = Task(
    description="Load the api key and return openai from Langstane LLM",
    expected_output="openai string",
    agent=APIAgent(),
    output_pydantic=APIOutput,
    method_name='load_openai_api_key'
)

get_openai_corrections_task = Task(
    description="Get corrected API response for the term using relevant prompt",
    expected_output="openai string",
    agent=CorrectCleanAPIAgent(),
    output_pydantic=APIOutput,
    method_name='get_openai_corrections'
)

ai_standardize_column_task = Task(
    description="Standardize feature names using OpenAI",
    expected_output="Standardized DataFrame",
    agent=ColumnStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='ai_standardize_column'
)

human_inflxn_ai_correction_task = Task(
    description="Use inflexn to correct AI-standardized feature names",
    expected_output="Inflxn Corrected DataFrame",
    agent=HumanDataStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='inflection_ai_standard_feature_name'
)

apply_human_inflxn_ai_standard_feature_name_task = Task(
    description="Apply the output of the human-inflection-AI-standardized feature names",
    expected_output="Human-Inflection-AI-corrected DataFrame",
    agent=HumanDataStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='apply_human_inflxn_ai_standard_feature_name'
)

human_ai_standardized_data_task = Task(
    description="Manually correct AI-standardized feature names",
    expected_output="Corrected feature names of DataFrame",
    agent=HumanDataStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='human_ai_standardized_data'
)


calculate_accuracy_task = Task(
    description="Calculate accuracy change between dataframes",
    expected_output="Accuracy readings",
    agent=HumanDataStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='calculate_accuracy'
)

show_directory_files_in_tabs_task = Task(
    description="Files in directory displayed in tabs as DataFrame",
    expected_output="Individual DataFrame in tabs",
    agent=DataReadAgent(),
    output_pydantic=DataFrameOutput,
    method_name='show_directory_files_in_tabs'
)

detect_missing_data_task = Task(
    description="Display detected missing data and stats per column.\n",
    expected_output="Missing data df",
    agent=MissingDataAgent(),
    output_pydantic=DataFrameOutput,
    method_name='detect_missing_data'
)


fix_missing_data_task = Task(
    description="Fix detected missing data.\n",
    expected_output="Fixed missing data df",
    agent=MissingDataAgent(),
    output_pydantic=DataFrameOutput,
    method_name='fix_missing_data'
)

fix_human_ai_missing_data_task = Task(
    description="Fix detected missing data.\n",
    expected_output="Fixed missing data df",
    agent=MissingDataAgent(),
    output_pydantic=DataFrameOutput,
    method_name='fix_human_ai_missing_data'
)

preprocess_anomaly_data_task = Task(
    description="Preprocess data with anomaly values.\n",
    expected_output="Preprocessed anomaly data",
    agent=DataAnomalyAgent(),
    output_pydantic=DataFrameOutput,
    method_name='crewai_preprocess_anomaly_data'
)

detect_anomaly_data_task = Task(
    description="Detect the anomalies in a data.\n",
    expected_output="Data frame with detected anomalies",
    agent=DataAnomalyAgent(),
    output_pydantic=DataFrameOutput,
    method_name='crewai_detect_anomalies'
)

fix_anomalies_task = Task(
    description="Detect the anomalies in a data.\n",
    expected_output="Data frame with detected anomalies",
    agent=DataAnomalyAgent(),
    output_pydantic=DataFrameOutput,
    method_name='crewai_fix_anomalies'
)

crewai_plot_anomalies_task = Task(
    description="Plot the anomalies in a data.\n",
    expected_output="Plot of anomalies",
    agent=DataAnomalyAgent(),
    output_pydantic=DataFrameOutput,
    method_name='crewai_plot_anomalies'
)

crewai_interactive_anomalies_plot_task = Task(
    description="Plot interactive anomalies in a data.\n",
    expected_output="Plot of anomalies",
    agent=DataAnomalyAgent(),
    output_pydantic=DataFrameOutput,
    method_name='crewai_interactive_anomalies_plot'
)


# Create the crew and plan for task execution
crew = Crew(
    agents=[DataReadAgent(), APIAgent(), ColumnStandardizationAgent(), HumanDataStandardizationAgent()],
    tasks=[data_read_task, load_openai_api_key_task, get_openai_corrections_task, ai_standardize_column_task, 
           human_inflxn_ai_correction_task, apply_human_inflxn_ai_standard_feature_name_task, human_ai_standardized_data_task, 
           calculate_accuracy_task, show_directory_files_in_tabs_task,
           detect_missing_data_task, fix_missing_data_task, fix_human_ai_missing_data_task, preprocess_anomaly_data_task,
           detect_anomaly_data_task, fix_anomalies_task, crewai_plot_anomalies_task, crewai_interactive_anomalies_plot_task],
    verbose=True,
    planning=False
)


#• Standardization of the feature names
#• Standardization of the data
#• Missing values
#• Duplication of the data
#• Anomalies
#• Primary keys for joining tables.
# Use this one