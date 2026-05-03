
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


    def auto_flatten_json(self, df):
        """
        Universal recursive flattener: Digs through layers of lists and dicts
        until every single cell is an atomic value.
        """
        # Create a copy to avoid modifying the original during processing
        df = df.copy()

        while True:
            # 1. Identify all columns that are still "dirty" (contain lists or dicts)
            nested_cols = [
                col for col in df.columns 
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any()
            ]
            
            # If no columns contain nested structures, we are finished
            if not nested_cols:
                break
                
            for col in nested_cols:
                # Check the actual content of the column
                sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                
                # CASE A: It's a list (Needs to become new rows)
                if isinstance(sample_val, list):
                    df = df.explode(col).reset_index(drop=True)
                    
                # CASE B: It's a dictionary (Needs to become new columns)
                elif isinstance(sample_val, dict):
                    # Normalize and add prefix to keep track of where data came from
                    norm = pd.json_normalize(df[col]).add_prefix(f"{col}_")
                    # Drop original column and join the new flattened ones
                    df = pd.concat([df.drop(columns=[col]).reset_index(drop=True), 
                                    norm.reset_index(drop=True)], axis=1)
        
        # Final cleanup: convert strings that look like numbers/dates into proper types
        return df.apply(pd.to_numeric, errors='ignore')



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
            original_data_json = pd.read_json(file_path)
            original_data = self.auto_flatten_json(original_data_json)
        else:
            raise ValueError(f"Unsupported file type: {file_type}") #return original_data
        
        return  DataFrameOutput(data=original_data)



    def add_headers_to_df(self, dataframe, headers):
        """
        Add headers to a DataFrame while optionally resetting the index.

        Parameters:
        dataframe (pandas.DataFrame): The DataFrame to which headers will be added.
        headers (list): List of header names. Must match the number of columns in the DataFrame.

        Returns:
        pandas.DataFrame: DataFrame with added headers and all original rows preserved.
        """
        
        # Validate that headers length matches the number of columns
        if len(headers) != len(dataframe.columns):
            raise ValueError(f"Number of headers ({len(headers)}) does not match number of columns ({len(dataframe.columns)})")
        
        # Create a new DataFrame with the current headers as the first row
        # Use the same column structure as the original DataFrame
        header_row = pd.DataFrame([dataframe.columns.tolist()], columns=dataframe.columns)
        
        # Concatenate the header row with the original DataFrame
        new_dataframe = pd.concat([header_row, dataframe], ignore_index=True)
        
        # Set the new headers
        new_dataframe.columns = headers

        return new_dataframe


    def rename_feature(self, dataframe, columns):
        """
        Rename columns in a DataFrame.

        Parameters:
        dataframe (pandas.DataFrame): The DataFrame to which columns are renamed.

        Returns:
        pandas.DataFrame: DataFrame with renamed feature names.
        """

        columns_row = pd.DataFrame([dataframe.columns.tolist()], columns=dataframe.columns)

        # Create a new DataFrame with the current headers as the first row
        dataframe = pd.DataFrame([dataframe.columns.tolist()], columns=dataframe.columns)
        
        # Concatenate the header row with the original DataFrame
        new_dataframe = pd.concat([columns_row, dataframe], ignore_index=True)
        
        # Set the new headers
        new_dataframe.columns = columns


        return new_dataframe


        #df = df.rename(columns={'old_name1': 'new_name1', 'old_name2': 'new_name2'})








    def assign_features_dtypes(self, dataframe, dict_of_dtype):
        """
        Assign datatypes to specified columns in a DataFrame.

        Parameters:
        dataframe (pandas.DataFrame): The DataFrame to modify
        dict_of_dtype (dict): A dictionary mapping column names to desired datatypes

        Returns:
        pandas.DataFrame: The DataFrame with updated column datatypes
        
        Example:
        
            dict_of_dtype = {
                'timestamp': 'datetime64[ns]',
                'value': 'float64'
            }
        
        """
        return dataframe.astype(dict_of_dtype)


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


############

    def compare_two_df_by_columns(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict[str, Any]:

        frame = inspect.currentframe()
        try:
            context = inspect.getframeinfo(frame.f_back).code_context
            caller_lines = ''.join(context)
            name_df1 = self._get_variable_name(caller_lines, 0)
            name_df2 = self._get_variable_name(caller_lines, 1)
        finally:
            del frame  

        # Sort columns to benefit when column names are relatively similar
        sorted_columns_df1 = sorted(df1.columns)
        sorted_columns_df2 = sorted(df2.columns)
        
        # Dropdown widgets to select individual columns from each DataFrame
        select_df1_column = widgets.Dropdown(
            options=sorted_columns_df1,
            #description=f'Columns in {name_df1}:',
            disabled=False
        )
        
        select_df2_column = widgets.Dropdown(
            options=sorted_columns_df2,
            #description=f'Columns in {name_df2}:',
            disabled=False
        )
        
        # Create dropdown for selecting comparison mode
        comparison_mode_selector = widgets.Dropdown(
            options=['Column-wise', 'Row-wise'],
            #description='Comparison Mode:',
            disabled=False
        )
        
        # Create button to trigger comparison
        compare_button = widgets.Button(
            description='Compare DF',
            disabled=False,
            button_style='success',
            tooltip='Click to compare selected columns'
        )
        
        output = widgets.Output() # Display results
        
        # Comparison control by button on click
        def _click_button_to_compare(b):
            mode = comparison_mode_selector.value
            col1 = select_df1_column.value
            col2 = select_df2_column.value
            
            # Check if the columns are present in the dataframes
            if col1 not in df1.columns or col2 not in df2.columns:
                with output:
                    output.clear_output()
                    print(f"Error: One or both columns '{col1}' or '{col2}' not found.")
                return
            
            result = {}
            result_str = ""
            
            with output:
                # Clear previous output
                output.clear_output()
                
                if mode == 'Column-wise':
                    # Column-wise display of results
                    df1_col_values = df1[col1].values
                    df2_col_values = df2[col2].values
                    
                    # Compare lengths first
                    if len(df1_col_values) != len(df2_col_values):
                        result['error'] = f"Error: Columns '{col1}' and '{col2}' have different lengths."
                        result_str = result['error']
                        with output:
                            output.clear_output()
                            print(result_str)
                        return
                    
                    # Compare values element-wise, handle behaviours of NaN and NaT
                    df_nan_mask = pd.isna(df1_col_values) & pd.isna(df2_col_values)
                    comparison = (df1_col_values == df2_col_values) | df_nan_mask
                    num_matches = comparison.sum()
                    num_total = len(comparison)
                    
                   
                    result['comparison_type'] = 'Column-wise'
                    result['total_comparisons'] = num_total
                    result['matches'] = num_matches
                    result['accuracy'] = round((num_matches / num_total) * 100, 2)
                    
                    # Collect mismatches
                    mismatches = [(i, df1_col_values[i], df2_col_values[i]) for i in range(len(comparison)) if not comparison[i]]
                    
                    result_str += f"Column '{col1}' in DataFrame '{name_df1}' vs Column '{col2}' in DataFrame '{name_df2}'\n"
                    result_str += f"Total comparisons: {num_total}\n"
                    result_str += f"Matches: {num_matches}\n"
                    result_str += f"Accuracy: {result['accuracy']}%\n"
                    
                    if mismatches:
                        result_str += "Differences found between DataFrames:\n"
                        for idx, val1, val2 in mismatches:
                            result_str += f"Index {idx}: {name_df1}' ='{val1}', {name_df2}' ='{val2}'\n"
                    else:
                        result_str += "No differences found."
                
                elif mode == 'Row-wise':
                    # Check if the number of rows are numerically alike
                    if len(df1) != len(df2):
                        result['error'] = "Error: DataFrames have different numbers of rows."
                        result_str = result['error']
                        with output:
                            output.clear_output()
                            print(result_str)
                        return
                    
                    # Display row-wise comparison results
                    mismatches = []
                    for idx in range(len(df1)):
                        row1 = df1.loc[idx, col1]
                        row2 = df2.loc[idx, col2]
                        if row1 != row2:
                            mismatches.append((idx, row1, row2))
                    
                    num_matches = len(df1) - len(mismatches)
                    num_total = len(df1)
                    
                    result['comparison_type'] = 'Row-wise'
                    result['total_rows_compared'] = num_total
                    result['rows_matching'] = num_matches
                    result['accuracy'] = round((num_matches / num_total) * 100, 2)
                    
                    # Collect mismatches
                    result_str += f"Row-wise comparison between '{col1}' in {name_df1} and '{col2}' in {name_df2}\n"
                    result_str += f"Total rows compared: {num_total}\n"
                    result_str += f"Rows matching: {num_matches}\n"
                    result_str += f"Accuracy: {result['accuracy']}%\n"
                    
                    if mismatches:
                        result_str += "Differences found:\n"
                        for idx, val1, val2 in mismatches:
                            result_str += f"Row {idx}: {name_df1}='{val1}', {name_df2}='{val2}'\n"
                    else:
                        result_str += "No differences found."
            
            # Display the result string
            with output:
                output.clear_output()
                print(result_str)
            
        # Click event linked to compare function
        compare_button.on_click(_click_button_to_compare)
        
        # Horizontal layout
        hbox_layout = widgets.HBox([
            widgets.VBox([
                widgets.Label(' ' * 15 + f"Columns in DF:  {name_df1}", layout=widgets.Layout(width='200px')),
                select_df1_column
            ]),
            widgets.VBox([
                widgets.Label(' ' * 15 + f"Columns in DF:  {name_df2}", layout=widgets.Layout(width='200px')),
                select_df2_column
            ]),
            widgets.VBox([
                widgets.Label(' ' * 15 + "Comparison Mode", layout=widgets.Layout(width='200px')),
                comparison_mode_selector
            ])
    
        ])
        
        display(hbox_layout, compare_button, output) # final result displayed


    def _get_variable_name(self, caller_lines, arg_index):
        import re
        method_call = re.search(r'compare_two_df_by_columns\((.*?)\)', caller_lines)
        if method_call:
            args = [arg.strip() for arg in method_call.group(1).split(',')]
            if arg_index < len(args):
                return args[arg_index]
        return f"df{arg_index + 1}"



###############
    '''
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
        '''



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

compare_two_df_by_columns_task = Task(
    description="Calculate accuracy change between dataframes",
    expected_output="Accuracy readings",
    agent=DataReadAgent(),
    output_pydantic=StandardizedDataFrameOutput,
    method_name='compare_two_df_by_columns'
)
# Create the crew and plan for task execution
crew = Crew(
    agents=[DataReadAgent()],
    tasks=[data_read_task, compare_two_df_by_columns_task],
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