
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
import inspect
from sklearn.metrics import accuracy_score

#####
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, PrivateAttr
from crewai import Agent, Crew
from crewai.task import Task
from crewai.tasks.task_output import TaskOutput
from typing import ClassVar, Optional
#####

#from .crew_ai.correct_clean_api_agent import CorrectCleanAPIAgent as ccapi
from correct_clean_api_agent import CleanAPIAgent as ccapi
#ccapi = CorrectCleanAPIAgent()

#from .api_agent import APIAgent as apia
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
from pyod.models.feature_bagging import FeatureBagging #'please install combo first for combination by `pip install combo`'
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
from pyod.models.lmdd import LMDD
from pyod.models.dif import DIF
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from pyod.models.suod import SUOD       # Scalable Unsupervised Outlier Detection #'pip install suod'
from pyod.models.qmcd import QMCD
from pyod.models.sampling import Sampling
from pyod.models.kpca import KPCA
from pyod.models.lunar import LUNAR


from ipywidgets import Checkbox, Output, VBox, HBox, Label
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


from spellchecker import SpellChecker #case sensitive, lower() best results
from fuzzywuzzy import fuzz, process
# pip install phunspell
import phunspell
import traceback
import streamlit as st



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


#############################  ColumnStandardizationAgent



class ColumnStandardizationAgent(ccapi):  
    ai_standardize_column: dict = Field(default_factory=dict)
    ai_standardize_missing_data: dict = Field(default_factory=dict)
    
    def __init__(self, **data):
        super().__init__(**data)
        self._spell = SpellChecker(language='en', distance=1)  #SpellChecker(language='en_GB')
        try:
            self._pspell = phunspell.Phunspell("en_GB")
        except Exception as e:
            self._pspell = None
        
          
   
    def correct_spelling(self, text: str) -> str:
        """
        Correct spelling - GUARANTEED to return a string, never None.
        """
        # Step 1: Handle None and non-string inputs
        if text is None:
            return ""
        
        if not isinstance(text, str):
            try:
                text = str(text)
            except:
                return ""
        
        # Step 2: Clean the text
        original_text = text
        text = text.strip()
        
        if text == "":
            return ""
        
        # Step 3: Process the text
        words = text.split()
        corrected_words = []
        
        for word in words:
            corrected_word = word  # Start with original
            
            # Try SpellChecker first
            if hasattr(self, '_spell') and self._spell is not None:
                try:
                    # Store original case pattern
                    is_all_upper = word.isupper()
                    is_title_case = word.istitle()
                    is_capitalized = word[0].isupper() if word else False
                    
                    # Check spelling using lowercase version
                    word_lower = word.lower()
                    
                    # Check if word is misspelled
                    # We need to check if it's in the dictionary
                    known_words = self._spell.known([word_lower])
                    
                    if not known_words:  # Word is misspelled
                        correction = self._spell.correction(word_lower)
                        
                        if correction and correction != word_lower:
                            # Apply original case pattern
                            if is_all_upper:
                                corrected_word = correction.upper()
                            elif is_title_case:
                                corrected_word = correction.title()
                            elif is_capitalized:
                                corrected_word = correction.capitalize()
                            else:
                                corrected_word = correction
                except Exception:
                    pass
            
            corrected_words.append(corrected_word)
        
        # Join words back
        result = " ".join(corrected_words)
        
        return result
    
    
    def standardize_column(self, data: pd.DataFrame, column: str, reference_column: str, prompt_template: str = "") -> pd.Series:
        
        self.ai_standardize_column = {}
        
        # Retain word cases
        original_column = data[column].astype(str).str.strip()
        
        # Get unique terms
        unique_terms = list(original_column.dropna().unique())
        
        # Create correction map for ALL terms
        correction_map = {}
        corrections_made = 0
        
        for term in unique_terms:
            corrected = self.correct_spelling(term) 
            
            if corrected != term:
                correction_map[term] = corrected
                corrections_made += 1
        
        # Apply corrections
        standardized_column = original_column.map(correction_map).fillna(original_column)

        # Display corrections table
        if correction_map:
            with st.expander(f"Spelling Corrections in '{column}'", expanded=False):          
                # Create table from correction_map
                corrections_df = pd.DataFrame(
                    list(correction_map.items()),
                    columns=['Original', 'Corrected']
                )
                st.dataframe(corrections_df, hide_index=True, width='stretch')
                st.info(f"✅ Applied {len(correction_map)} term spelling correction(s)")


        return standardized_column


    
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
            openai_corrections = self.get_openai_corrections(terms, _existing_shortcodes, prompt_template, progress_bar=pbar)

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
                        ai_suggestion = ccapi.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion)
                    
                    elif pd.api.types.is_numeric_dtype(data[column]):
                        # If it's numeric data, suggest the mean/median or AI-based imputation
                        mean_value = data[column].mean()
                        prompt_template = "Based on other data, suggest a numeric value."
                        ai_suggestion = ccapi.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion if ai_suggestion else mean_value)
                    
                    elif pd.api.types.is_datetime64_any_dtype(data[column]):
                        # If it's datetime data, suggest a reasonable date or use AI-based imputation
                        most_common_date = data[column].mode()[0] if not data[column].mode().empty else pd.Timestamp.now()
                        prompt_template = "Based on other data, suggest a numeric value."
                        ai_suggestion = ccapi.get_openai_corrections(terms, _existing_shortcodes, prompt_template)
                        ai_standardize_missing_data.append(ai_suggestion if ai_suggestion else most_common_date)
                    
                    else:
                        # For any other type, retain the original missing value
                        ai_standardize_missing_data.append(pd.NA)
                
                # Update the progress bar
                pbar.update(1)
            
            pbar.update(len(data) - pbar.n)  # Ensure progress is complete
    
        return pd.Series(ai_standardize_missing_data, index=data.index)


########-------------------------------------#################


class Crew:
    def __init__(self, agents=None, tasks=None, verbose=False, planning=True):
        self.agents = agents or []
        self.tasks = tasks or []
        self.verbose = verbose
        self.planning = planning

    def run_task(self, task, *args, **kwargs):
        if self.verbose:
            st.write(f"Running task: {task.description}")
        
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

ai_standardize_column_task = Task(
    description="Standardize feature names using OpenAI",
    expected_output="Standardized DataFrame",
    agent=ColumnStandardizationAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='ai_standardize_column'
)

# Create the crew and plan for task execution
crew = Crew(
    agents=[ColumnStandardizationAgent()],
    tasks=[ai_standardize_column_task],
    verbose=True,
    planning=False
)