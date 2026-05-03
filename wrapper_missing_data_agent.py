import os
import pandas as pd
from crewai import LLM, Agent, Task, Crew, Process
from correct_clean_api_agent import APIAgent as apiagent
import streamlit as st
from crewai import LLM


from dotenv import load_dotenv
from langchain_community.tools.google_jobs import GoogleJobsQueryRun
from langchain_community.utilities.google_jobs import GoogleJobsAPIWrapper
from crewai import Agent, Task, Crew

from smart_missing_data_filler_agent import SmartMissingDataFiller # this is for missing data, specially for AI filling multiple columns with context

# load dotenv:
load_dotenv()

# Initial start

# class MissingDataAgentCrew:
#     def __init__(self, llm_instance):
#         if llm_instance is None:
#             raise ValueError("An llm_instance must be provided")

#         self.agent = Agent(
#             role="Data Imputation Specialist",
#             goal="Analyze dataset and decide the best way to fix missing data in a given column with explanation.",
#             backstory="You are an expert data scientist specializing in cleaning and imputing missing data with sound reasoning.",
#             allow_delegation=False,
#             verbose=True,
#             llm=llm_instance
#         )

#         #self.task = Task(
#         #    description=(
#         #        "Given the dataset summary and a column name, select the best missing data imputation method "
#         #        "and provide a justification in JSON with keys: 'process' (str), 'custom_value' (optional), 'explanation' (str)."
#         #    ),
#         #    expected_output="JSON with keys: 'process' (str), 'custom_value' (optional), 'explanation' (str)",
#         #    agent=self.agent
#         #)
        

#         user_description = st.text_area("Enter your custom imputation task description:", height=200)

#         if user_description.strip() == "":
#             user_description = (
#                 "Given the dataset summary, a column of interest, and brief statistics "
#                 "about adjacent or related columns that might inform imputation, analyze "
#                 "the column with missing data and select the best missing data imputation method. "
#                 "Provide a detailed justification explaining:\n\n"
#                 "- Which imputation method you chose (e.g., mean, median, mode, custom value, forward fill, etc.)\n"
#                 "- Why this method is optimal given the data characteristics (data type, missing ratio, distribution, etc.)\n"
#                 "- How information from adjacent or related columns influenced your decision\n\n"
#                 "You may consider value correlations, value similarity, or patterns across these columns "
#                 "to inform imputation.\n\n"
#                 "Respond in a JSON with keys:\n"
#                 "  'process' (str) — the chosen imputation method\n"
#                 "  'custom_value' (optional) — any special value used\n"
#                 "  'explanation' (str) — detailed reasoning including how adjacent columns influenced your choice."
#             )

#         # Use user_description in your Task
#         self.task = Task(
#             description=user_description,
#             expected_output="JSON with keys: 'process', 'custom_value' (optional), and 'explanation'",
#             agent=self.agent
#         )


#         self.crew = Crew(
#             agents=[self.agent],
#             tasks=[self.task],
#             process=Process.sequential,
#             verbose=True
#         )

#     def decide_imputation(self, data: pd.DataFrame, column: str) -> dict:
#         data_summary = data.describe(include='all').to_dict()
#         inputs = {
#             "data_summary": data_summary,
#             "column": column,
#             "missing_ratio": float(data[column].isnull().mean()),
#             "column_dtype": str(data[column].dtype),
#         }
#         result = self.crew.kickoff(inputs=inputs)
#         # CrewOutput may not support `.get()`, so convert to dict or access .tasks
#         try:
#             result_dict = dict(result)
#         except Exception:
#             result_dict = getattr(result, 'tasks', {})
#         return result_dict.get(self.task.name, {})

# class WrapperMissingDataAgent:
#     def __init__(self):
#         # # Optional: Use your existing APIAgent to ensure key is present
#         # agent_checker = apiagent()
#         # agent_checker.load_openai_api_key()  # just to verify key exists

#         # # Read API key
#         # api_key = os.getenv("OPENAI_API_KEY")
#         # if not api_key:
#         #     raise ValueError("OPENAI_API_KEY environment variable is required")

#         # # Create CrewAI LLM instance internally referencing OpenAI provider
#         # llm_instance = LLM(
#         #     model="openai/gpt-4",
#         #     api_key=api_key,
#         #     temperature=0.7
#         # )

#         # Create CrewAI LLM instance internally referencing Ollama provider

#         #sudo apt update
#         #sudo apt install snapd
#         #Install Ollama via Snap:
#         #sudo snap install ollama
#         #Run Ollama server:
#         #ollama serve
                
#         llm_instance = LLM(
#             model="ollama/llama2:latest",           # Exact model and tag name
#             base_url="http://localhost:11434",     # Ollama local server URL
#             api_key=None,                          # Local server usually no API key required
#             temperature=0.7
#         )

#         # Pass to your crew agent
#         self.crew_agent = MissingDataAgentCrew(llm_instance=llm_instance)


#     def fix_missing_data_with_stats_or_fill(self, data, column, process, custom_value=None):
#         explanation = ""

#         if process == 'ai':
#             ai_decision = self.crew_agent.decide_imputation(data, column)
#             process = ai_decision.get("process", "mode")
#             custom_value = ai_decision.get("custom_value")
#             explanation = ai_decision.get("explanation", "")
#         else:
#             explanation = f"Manual process chosen: {process}"

#         processed_data = self._apply_imputation(data.copy(), column, process, custom_value)
#         return processed_data, explanation

#     def _apply_imputation(self, data, column, process, custom_value=None):
#         col_dtype = data[column].dtype
#         if process == 'replace':
#             fill_val = 9999 if pd.api.types.is_numeric_dtype(col_dtype) else 'NA'
#             data[column] = data[column].fillna(fill_val)
#         elif process == 'custom_text' and custom_value is not None:
#             if pd.api.types.is_string_dtype(col_dtype) or pd.api.types.is_object_dtype(col_dtype):
#                 data[column] = data[column].fillna(str(custom_value))
#         elif process == 'custom_number' and custom_value is not None:
#             if pd.api.types.is_numeric_dtype(col_dtype):
#                 val = int(custom_value) if pd.api.types.is_integer_dtype(col_dtype) else float(custom_value)
#                 data[column] = data[column].fillna(val)
#         elif process == 'custom_date' and custom_value is not None:
#             if pd.api.types.is_datetime64_any_dtype(col_dtype):
#                 data[column] = data[column].fillna(pd.to_datetime(custom_value))
#         elif process == 'mean':
#             if pd.api.types.is_numeric_dtype(col_dtype):
#                 val = data[column].mean()
#                 data[column] = data[column].fillna(val)
#         elif process == 'median':
#             if pd.api.types.is_numeric_dtype(col_dtype):
#                 val = data[column].median()
#                 data[column] = data[column].fillna(val)
#         elif process == 'mode':
#             mode_vals = data[column].mode()
#             if not mode_vals.empty:
#                 data[column] = data[column].fillna(mode_vals[0])
#         elif process == 'ffill':
#             data[column] = data[column].ffill()
#         elif process == 'bfill':
#             data[column] = data[column].bfill()
#         elif process == 'interpolate':
#             if pd.api.types.is_numeric_dtype(col_dtype):
#                 data[column] = data[column].interpolate()
#         else:
#             raise ValueError(f"Unknown process '{process}' or missing custom value.")
#         return data

# Initial end

class MissingDataAgentCrew:
    def __init__(self, llm_instance, user_description=None):
        if llm_instance is None:
            raise ValueError("An llm_instance must be provided")

        self.agent = Agent(
            role="Data Imputation Specialist",
            goal="Analyze dataset and decide the best way to fix missing data in a given column with explanation.",
            backstory="You are an expert data scientist specializing in cleaning and imputing missing data with sound reasoning.",
            allow_delegation=False,
            verbose=True,
            llm=llm_instance
        )

        # Default description
        if not user_description or user_description.strip() == "":
            user_description = (
                "Given the dataset summary, a column of interest, and brief statistics "
                "about adjacent or related columns that might inform imputation, analyze "
                "the column with missing data and select the best missing data imputation method. "
                "Provide a detailed justification explaining:\n\n"
                "- Which imputation method you chose (e.g., mean, median, mode, custom value, forward fill, etc.)\n"
                "- Why this method is optimal given the data characteristics (data type, missing ratio, distribution, etc.)\n"
                "- How information from adjacent or related columns influenced your decision\n\n"
                "You may consider value correlations, value similarity, or patterns across these columns "
                "to inform imputation.\n\n"
                "Respond in a JSON with keys:\n"
                "  'process' (str) — the chosen imputation method\n"
                "  'custom_value' (optional) — any special value used\n"
                "  'explanation' (str) — detailed reasoning including how adjacent columns influenced your choice."
            )

        self.task = Task(
            description=user_description,
            expected_output="JSON with keys: 'process', 'custom_value' (optional), and 'explanation'",
            agent=self.agent
        )

        self.crew = Crew(
            agents=[self.agent],
            tasks=[self.task],
            process=Process.sequential,
            verbose=True
        )

    def decide_imputation(self, data: pd.DataFrame, column: str) -> dict:
        data_summary = data.describe(include='all').to_dict()
        inputs = {
            "data_summary": data_summary,
            "column": column,
            "missing_ratio": float(data[column].isnull().mean()),
            "column_dtype": str(data[column].dtype),
        }
        result = self.crew.kickoff(inputs=inputs)
        
        # Parse the result - CrewAI returns raw text
        try:
            # Try to parse JSON from the result
            import json
            result_text = str(result)
            # Extract JSON if embedded in text
            if "```json" in result_text:
                json_str = result_text.split("```json")[1].split("```")[0]
            elif "```" in result_text:
                json_str = result_text.split("```")[1].split("```")[0]
            else:
                json_str = result_text
                
            result_dict = json.loads(json_str.strip())
            return result_dict
        except Exception as e:
            # Fallback: return a default response
            return {
                "process": "mode",
                "explanation": f"AI analysis failed. Error: {str(e)}. Using mode as fallback."
            }
        


class WrapperMissingDataAgent:
    def __init__(self):
        self.crew_agent = None
        self.initialized = False
    
    def initialize_agent(self, user_description=None):
        """Initialize the agent with optional user description"""
        try:
            llm_instance = LLM(
                model="ollama/llama2:latest",
                base_url="http://localhost:11434",
                api_key=None,
                temperature=0.7
            )
            
            self.crew_agent = MissingDataAgentCrew(
                llm_instance=llm_instance, 
                user_description=user_description
            )
            self.initialized = True
            return self
        except Exception as e:
            print(f"Error initializing agent: {e}")
            return self
    
    def fix_missing_data_with_stats_or_fill(self, data, column, process, custom_value=None):
        """Main method to fix missing data - FIXED VERSION"""
        if data is None or data.empty:
            return data, "Error: No data provided"
        
        if column not in data.columns:
            return data, f"Error: Column '{column}' not found in data"
        
        # Check if column has missing values
        missing_count = data[column].isnull().sum()
        if missing_count == 0:
            return data, f"No missing values in column '{column}'"
        
        explanation = ""

        if process == 'ai':
            with st.spinner("AI is analyzing patterns across multiple columns..."):
                try:
                    model="ollama/llama2:latest"
                    filler = SmartMissingDataFiller(model=model)
                    result_df = filler.fill_missing_with_context(st.session_statedf, column)
                    
                    # Update session state
                    st.session_state.df = result_df
                    
                    # Show results
                    st.success("✅ AI has filled missing values across selected columns!")
                    
                    # Download
                    csv = result_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download AI-Filled Data",
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

                # if process == 'ai':
                #     if not self.initialized:
                #         self.initialize_agent()
                #
                #     try:
        #         ai_decision = self.crew_agent.decide_imputation(data, column)
        #         process = ai_decision.get("process", "mode")
        #         custom_value = ai_decision.get("custom_value")
        #         explanation = ai_decision.get("explanation", "AI decision applied")
        #     except Exception as e:
        #         process = 'mode'
        #         explanation = f"AI failed: {str(e)}. Using mode as fallback."
        
        # else:
        #     explanation = f"Manual process chosen: {process}"



        # Apply the imputation
        processed_data = self._apply_imputation(data.copy(), column, process, custom_value)
        
        # Verify the fix worked
        new_missing_count = processed_data[column].isnull().sum()
        if new_missing_count < missing_count:
            explanation += f"\nFixed {missing_count - new_missing_count} missing values"
        else:
            explanation += f"\nNo values were fixed. Check data type compatibility."
        
        return processed_data, explanation
    
    def _apply_imputation(self, data, column, process, custom_value=None):
        """Apply imputation method - COMPLETELY REWRITTEN FOR RELIABILITY"""
        # Make a copy to avoid modifying original
        df_copy = data.copy()
        
        # Get column info
        col_dtype = df_copy[column].dtype
        missing_before = df_copy[column].isnull().sum()
        
        print(f"DEBUG: Process='{process}', Column='{column}', Type={col_dtype}, Custom={custom_value}")
        
        try:
            if process == 'custom_text':
                # Force conversion to string for text replacement
                df_copy[column] = df_copy[column].astype(str)
                df_copy.loc[df_copy[column] == 'nan', column] = str(custom_value)
                df_copy.loc[df_copy[column] == 'NaN', column] = str(custom_value)
                df_copy.loc[df_copy[column] == 'None', column] = str(custom_value)
                # Also handle actual NaN
                df_copy[column] = df_copy[column].replace({'nan': str(custom_value), 
                                                          'NaN': str(custom_value), 
                                                          'None': str(custom_value)})
                df_copy[column] = df_copy[column].fillna(str(custom_value))
            
            elif process == 'custom_number':
                if pd.api.types.is_numeric_dtype(col_dtype):
                    # Convert custom_value to appropriate numeric type
                    if pd.api.types.is_integer_dtype(col_dtype):
                        fill_val = int(float(custom_value))
                    else:
                        fill_val = float(custom_value)
                    df_copy[column] = df_copy[column].fillna(fill_val)
            
            elif process == 'custom_date':
                if pd.api.types.is_datetime64_any_dtype(col_dtype):
                    df_copy[column] = df_copy[column].fillna(pd.to_datetime(custom_value))
            
            elif process == 'mean':
                if pd.api.types.is_numeric_dtype(col_dtype):
                    mean_val = df_copy[column].mean()
                    df_copy[column] = df_copy[column].fillna(mean_val)
            
            elif process == 'median':
                if pd.api.types.is_numeric_dtype(col_dtype):
                    median_val = df_copy[column].median()
                    df_copy[column] = df_copy[column].fillna(median_val)
            
            elif process == 'mode':
                mode_vals = df_copy[column].dropna().mode()
                if not mode_vals.empty:
                    df_copy[column] = df_copy[column].fillna(mode_vals.iloc[0])
            
            elif process == 'ffill':
                df_copy[column] = df_copy[column].ffill()
            
            elif process == 'bfill':
                df_copy[column] = df_copy[column].bfill()
            
            elif process == 'interpolate':
                if pd.api.types.is_numeric_dtype(col_dtype):
                    df_copy[column] = df_copy[column].interpolate()
            
            elif process == 'replace':
                if pd.api.types.is_numeric_dtype(col_dtype):
                    df_copy[column] = df_copy[column].fillna(9999)
                else:
                    df_copy[column] = df_copy[column].fillna('NA')
            
            else:
                print(f"Warning: Unknown process '{process}'")
        
        except Exception as e:
            print(f"Error applying imputation: {e}")
        
        # Debug: Show what changed
        missing_after = df_copy[column].isnull().sum()
        print(f"DEBUG: Missing before={missing_before}, after={missing_after}")
        
        return df_copy