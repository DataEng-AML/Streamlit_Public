import streamlit as st
import pandas as pd
import numpy as np
import re
import uuid
import io
import os
import tempfile



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


    def fix_missing_data(self, df):
        # Store original_df in session state
        if 'original_df' not in st.session_state or st.session_state.original_df is None:
            st.session_state.original_df = df.copy(deep=True)

        # Use the current df in session if exists
        df = st.session_state.get('df', df)
        
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
                    #st.dataframe(cleaned_missing_data.head())

                    # Update the session state with the cleaned dataframe
                    st.session_state.df = cleaned_missing_data
                    return cleaned_missing_data
                else:
                    st.warning("Please enter a value to replace missing data.")
        
        with col2:
            if st.button("Revert Changes", key="revert_changes_button"):
                if 'original_df' in st.session_state and st.session_state.original_df is not None:
                    st.session_state.df = st.session_state.original_df.copy(deep=True)
                    st.success("Changes reverted. Original DataFrame restored.")
                    st.dataframe(st.session_state.df.head())
                    return st.session_state.df
                else:
                    st.warning("No original data to revert to.")

        return None
