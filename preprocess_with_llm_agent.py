import streamlit as st
import pandas as pd
import numpy as np
import re
import uuid
import sys
import io
import os
import tempfile


from streamlit_app_class_agent import CrewAIPyDEAML
from langchain_experimental.agents.agent_toolkits import create_csv_agent
#from langchain.agents.agent_types import AgentType
#from langchain.agents import AgentType
#from langchain_community.agent_toolkits.base import AgentType
#from langchain.agents import AgentType
#from langchain.agents import AgentExecutor

#from langchain_community.llms import OpenAI
from langchain_openai import OpenAI
from statistics_agent import FeaturesStatisticsAgent




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
    

output_capture = OutputCapture()

    
class PreprocessWithLLMAgent():

    def __init__(self, session_key='df', version_key='df_versions', cleaned_key='df_cleaned'):
        self.session_key = session_key
        self.version_key = version_key
        self.cleaned_key = cleaned_key

        if 'df' in st.session_state:
            df = st.session_state.df
        
        if self.version_key not in st.session_state:
            st.session_state[self.version_key] = []
        if self.session_key not in st.session_state:
            st.session_state[self.session_key] = None
        if self.cleaned_key not in st.session_state:
            st.session_state[self.cleaned_key] = None


    # Function to create a pandas DataFrame agent
    def generate_create_csv(self,uploaded_file):
        # Ensure that 'uploaded_file' is the file-like object, not just the file name string
        if hasattr(uploaded_file, 'getvalue'):
            file_content = uploaded_file.getvalue()  # This works correctly for file-like objects
            with open('wip.csv', 'wb') as f:  # Assuming you want to write as binary
                f.write(file_content)
            #st.write("File has been saved as 'wip.csv'")
        else:
            st.error("The file is not in the expected format.")
            return None

        custom_prefix = """
        You are a Professional Data Quality Agent. 
        Your goal is to find hidden issues that standard tools miss.
        
        When a user asks a question, follow this internal logic:
        - Never trust .isnull() alone. Search for text-based placeholders (unknown, na, none, 0, ?).
        - Look for 'rare' values in categorical columns that might be typos (like 'showerUnsure').
        - If you find issues, always list the Row Indices so the user can find them.
        - Keep your final response concise and data-driven.
        """

        create_csv = create_csv_agent(OpenAI(temperature=0), 
                                      "wip.csv", 
                                      verbose=True, 
                                      allow_dangerous_code=True,
                                      agent_type="zero-shot-react-description",
                                      #agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                                      prefix=custom_prefix
                                      ) 

        return create_csv    


    def df_csv_io(self,df):
        # Use BytesIO to create a file-like object in memory
        csv_in_memory = io.BytesIO()  # Use BytesIO for binary data (use StringIO for text-based CSVs)
        df.to_csv(csv_in_memory, index=False)  # df to the buffer

        csv_in_memory.seek(0) # reset the cursor
        #csv_in_memory.getvalue()
        return csv_in_memory   
    

    def save_version(self):
        if st.session_state[self.session_key] is not None:
            st.session_state[self.version_key].append(st.session_state[self.session_key].copy())

    def undo(self):
        if st.session_state[self.version_key]:
            last_version = st.session_state[self.version_key].pop()
            st.session_state[self.session_key] = last_version
            st.success("Undo successful: reverted to previous version.")
        else:
            st.warning("No previous versions to undo.")



    def run(self, output_capture):

        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) 
        st.markdown("##### Preprocessed DataFrame")


        # Initialize session state for tracking changes
        if 'df_history' not in st.session_state:
            st.session_state.df_history = []

        if 'df' not in st.session_state:
            st.session_state.df = None

        # STORE ORIGINAL DF FOR RESET - ADD THIS
        if 'original_df' not in st.session_state:
            st.session_state.original_df = st.session_state.df.copy()  # Store the raw original

        if 'stats_agent' not in st.session_state:
            st.session_state.stats_agent = FeaturesStatisticsAgent()


        if "current_df" not in st.session_state and st.session_state.df is not None:
            st.session_state.current_df = st.session_state.df.copy() 
        else:
            ""


        if self.session_key in st.session_state and st.session_state[self.session_key] is not None:
            df = st.session_state[self.session_key]
            st.dataframe(df.head())

        
            preprocessed_csv = self.df_csv_io(df)
            
            wip_preprocessed_csv = self.generate_create_csv(preprocessed_csv)
            

            output_capture = OutputCapture()

            # Check if the agent already exists to avoid recreating it on every click
            if 'wip_agent' not in st.session_state:
                preprocessed_csv = self.df_csv_io(df)
                st.session_state.wip_agent = self.generate_create_csv(preprocessed_csv)

            # Use the persistent agent from session state
            wip_preprocessed_csv = st.session_state.wip_agent


            if wip_preprocessed_csv is not None:
                #uq_k = f"editable_code_box_1_{str(uuid.uuid4())}"
                uq_k = f"editable_code_box_1_stable"
                
                # Unique key for text_area as well
                user_question = st.text_area(
                    "Interact with the LLM about the DataFrame, request for a Python code to resolve issues",
                    height=100,
                    key=uq_k
                )

                if st.button("Ask the LLM"):
                    if user_question is not None and user_question != "":
                        #st.write(user_question)
                        with st.spinner(text="In progress..."):
                            try:
                                #action_input = user_question
                                response = wip_preprocessed_csv.run(user_question)

                                st.write("**:green[User Request:]**")
                                #st.text(action_input)
                                
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
                            except Exception as e:
                                st.error(f"LLM Error: {str(e)}")
                                st.info("Make sure you have OPENAI_API_KEY set in your environment and have valid API credits.")


            st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

            cleaning_code = st.text_area(
                "Enter the suggested Python preprocessing code here. Use 'df' variable as input and assign result back to `df`.",
                height=200,
                value="# For example df = df.dropna()"  # Sample default cleaning code without #
            )


            # Apply Preprocessing Code button
            if st.button("Apply Preprocessing Code", 
                        help="Apply your code to create a new df. Confirm Change if new df is accepted."):
                st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True)
                try:
                                   
                    # Store the current state in history before applying new changes
                    st.session_state.df_history.append(st.session_state.current_df.copy())
                    
                    # Execute user code with the current dataframe
                    local_vars = {'df': st.session_state.current_df}
                    exec(cleaning_code, {}, local_vars)
                    

                    if 'df' in local_vars:
                        st.session_state.current_df = local_vars['df']
                    st.session_state.show_preview = True
                    st.success("Cleaning code applied successfully. Preview the changes below.")

                    
                    st.markdown("##### Transformed Dataframe")
                    st.dataframe(st.session_state.current_df)


                except Exception as e:
                    st.error(f"Error applying cleaning code: {e}")

            #     st.markdown("##### Statistics After Transformation 2")
            #     st.session_state.stats_agent.single_dataframe_summary(st.session_state.current_df)                    

            # st.markdown("##### Statistics After Transformation 3")
            # st.session_state.stats_agent.single_dataframe_summary(st.session_state.current_df)    

        return st.session_state.current_df, st.session_state.original_df
    

