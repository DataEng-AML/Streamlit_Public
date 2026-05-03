import pandas as pd
import numpy as np
import streamlit as st
from crewai import Agent, Task, Crew, Process
from typing import Dict, Any, List, Optional
import json
from datetime import datetime

from pydantic import BaseModel, Field
from typing import List
from langchain_openai import ChatOpenAI

import logging
import os
from litellm import completion

# Ensure folder exists
if not os.path.exists("logs"):
    os.makedirs("logs")

# Configure where the data goes
logging.basicConfig(
    filename='logs/app_activity.log', 
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# Create the logger object
logger = logging.getLogger(__name__)

# Single row result with minified keys
class ImpItem(BaseModel):
    id: int = Field(description="Row index")
    v: str = Field(description="Suggested value")
    r: str = Field(description="Reasoning")
    c: str = Field(description="Confidence H/M/L")

# The wrapper for the entire batch
class ImpBatch(BaseModel):
    s: List[ImpItem] = Field(description="List of suggestions")

# Initialize the LLM once outside the loop
# gpt-4o-mini is highly recommended for speed and cost on this task
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
structured_llm = llm.with_structured_output(ImpBatch)



class NumpyJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy and pandas types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict()
        elif pd.isna(obj):
            return None
        return super().default(obj)
    


#class SmartMissingDataFiller:
#    def __init__(self, model="llama2"):
#        self.model = model
#        self.llm_config = {
#            "model": f"ollama/{model}",
#            "temperature": 0.1,
#            "base_url": "http://localhost:11434"
#        }

class SmartMissingDataFiller:
    def __init__(self, model="gpt-3.5-turbo"):
        self.model = model
    
    def fill(self, data):
        response = completion(
            model=self.model,
            messages=[{"role": "user", "content": "Your prompt here"}],
            api_key=st.secrets.get("OPENAI_API_KEY")
        )
        return response.choices[0].message.content
        


    def create_context_agent(self):
        from crewai import Agent
        return Agent(
            role="Data Pattern Analyst",
            goal="Use context from related columns to intelligently fill missing values",
            backstory="Expert data analyst who identifies patterns and relationships between columns "
                     "to suggest the most appropriate values for missing data.",
            verbose=True,
            allow_delegation=False,
            llm_config=self.llm_config
        )
    
    def analyze_column_with_context(self, df: pd.DataFrame, target_column: str, context_columns: List[str]) -> Dict[str, Any]:
        """
        Analyze target column and its relationship with context columns.
        """
        def convert_to_serializable(val):
            """Convert any value to JSON-serializable format."""
            if pd.isna(val):
                return None
            elif isinstance(val, (np.integer, np.int64)):
                return int(val)
            elif isinstance(val, (np.floating, np.float64)):
                return float(val)
            elif isinstance(val, np.bool_):
                return bool(val)
            elif isinstance(val, pd.Timestamp):
                return str(val)
            return str(val)
        
        analysis = {
            "target_column": target_column,
            "context_columns": context_columns,
            "total_rows": len(df),
            "target_column_analysis": {},
            "context_analysis": {},
            "relationships": {}
        }
        
        # Analyze target column
        if target_column in df.columns:
            analysis["target_column_analysis"] = {
                "data_type": str(df[target_column].dtype),
                "missing_count": int(df[target_column].isnull().sum()),
                "missing_percentage": float(df[target_column].isnull().sum() / len(df) * 100),
                "unique_values": int(df[target_column].dropna().nunique()),
                "sample_values": [convert_to_serializable(val) for val in df[target_column].dropna().head(10).tolist()]
            }
            
            if np.issubdtype(df[target_column].dtype, np.number):
                analysis["target_column_analysis"]["statistics"] = {
                    "mean": convert_to_serializable(df[target_column].mean()),
                    "median": convert_to_serializable(df[target_column].median()),
                    "std": convert_to_serializable(df[target_column].std()),
                    "min": convert_to_serializable(df[target_column].min()),
                    "max": convert_to_serializable(df[target_column].max())
                }
        
        # Analyze context columns
        for col in context_columns:
            if col in df.columns and col != target_column:
                analysis["context_analysis"][col] = {
                    "data_type": str(df[col].dtype),
                    "missing_count": int(df[col].isnull().sum()),
                    "unique_values": int(df[col].nunique()),
                    "sample_values": [convert_to_serializable(val) for val in df[col].dropna().head(5).tolist()]
                }
        
        # Analyze relationships between target column and context columns
        if target_column in df.columns:
            for col in context_columns:
                if col in df.columns and col != target_column:
                    # Check for correlation if both are numeric
                    if (np.issubdtype(df[target_column].dtype, np.number) and 
                        np.issubdtype(df[col].dtype, np.number)):
                        try:
                            corr = df[[target_column, col]].corr().iloc[0, 1]
                            if not pd.isna(corr):
                                analysis["relationships"][f"{target_column}_{col}"] = {
                                    "correlation": float(corr),
                                    "interpretation": "strong" if abs(corr) > 0.7 else 
                                                   "moderate" if abs(corr) > 0.3 else "weak"
                                }
                        except:
                            pass
                    
                    # For categorical relationships, check value co-occurrence patterns
                    if df[target_column].nunique() < 20 and df[col].nunique() < 20:
                        try:
                            crosstab = pd.crosstab(df[target_column], df[col])
                            analysis["relationships"][f"{target_column}_{col}"] = {
                                "relationship_type": "categorical_crosstab",
                                "most_common_pairs": crosstab.stack().nlargest(5).to_dict()
                            }
                        except:
                            pass
        
        return analysis
    
    def get_row_context(self, df: pd.DataFrame, idx: int, target_column: str, context_columns: List[str]) -> Dict[str, Any]:
        """
        Get context for a specific row with missing value in target column.
        """
        def convert_value(val):
            if pd.isna(val):
                return None
            elif isinstance(val, (np.integer, np.int64)):
                return int(val)
            elif isinstance(val, (np.floating, np.float64)):
                return float(val)
            elif isinstance(val, np.bool_):
                return bool(val)
            elif isinstance(val, pd.Timestamp):
                return str(val)
            return str(val)
        
        # Get values from context columns in the current row
        row_context = {}
        for col in context_columns:
            if col in df.columns and col != target_column:
                val = df.iloc[idx][col]
                if pd.notna(val):
                    row_context[col] = convert_value(val)
        
        # Get adjacent rows (before and after)
        window_size = 2
        start = max(0, idx - window_size)
        end = min(len(df), idx + window_size + 1)
        
        adjacent_rows = []
        for i in range(start, end):
            if i == idx:
                continue
            
            row_data = {}
            for col in [target_column] + context_columns:
                if col in df.columns:
                    val = df.iloc[i][col]
                    row_data[col] = convert_value(val)
            
            adjacent_rows.append({
                "index": int(i),
                "distance": int(abs(i - idx)),
                "is_before": i < idx,
                "data": row_data
            })
        
        return {
            "row_index": int(idx),
            "target_column_missing": True,
            "row_context": row_context,
            "adjacent_rows": adjacent_rows,
            "window_size": window_size
        }
    
    def fill_target_with_context(self, df: pd.DataFrame, target_column: str, context_columns: List[str]) -> pd.DataFrame:
        """
        Fill missing values in target column using context from other columns.
        """
        from crewai import Task, Crew, Process


        
        # Check if target column exists
        if target_column not in df.columns:
            st.error(f"Target column '{target_column}' not found!")
            return df
        
        # Filter context columns to only those that exist
        valid_context = [col for col in context_columns if col in df.columns and col != target_column]
        
        if not valid_context:
            st.warning("No valid context columns provided. Using all other columns as context.")
            valid_context = [col for col in df.columns if col != target_column]


        #st.write("Fill missing values in a target column using patterns from related columns.")
        self.target_column = target_column
        self.valid_context = valid_context
        #self.missing_indices = missing_indices
        #self.filled_count = filled_count


        # Get indices of missing values in target column
        missing_indices = df[df[target_column].isnull()].index.tolist()

        self.missing_indices = missing_indices

        if not missing_indices:
            st.success(f"No missing values in '{target_column}'")
            return df
        
        # Analyze data
        analysis = self.analyze_column_with_context(df, target_column, valid_context)
        
        # Create agent
        agent = self.create_context_agent()
        
        # Prepare results
        results_df = df.copy()
        
        st.write(f"**Target Column:** {target_column}")
        st.write(f"**Context Columns:** {', '.join(valid_context)}")
        st.write(f"**Missing values to fill:** {len(missing_indices)}")
        
        # Process in batches
        batch_size = 50
        filled_count = 0
        
        for i in range(0, len(missing_indices), batch_size):
            batch_indices = missing_indices[i:i + batch_size]
            
            # Prepare batch data
            batch_data = []
            for idx in batch_indices:
                context = self.get_row_context(df, idx, target_column, valid_context)
                batch_data.append(context)
            
            # Create analysis JSON
            try:
                analysis_json = json.dumps(analysis, default=str, indent=2)
                batch_json = json.dumps(batch_data, default=str, indent=2)
            except Exception as e:
                ""#st.error(f"JSON error: {e}")
                analysis_json = str(analysis)
                batch_json = str(batch_data)
            


                batch_json = json.dumps(batch_data, separators=(',', ':'))

                # 2. Simple prompt
                prompt = f"Target: {target_column}. Context: {valid_context}. Data: {batch_json}"

                # 3. Direct structured call (Replaces Crew/Task/Regex)
                result = structured_llm.invoke(prompt)
                
                # 4. Apply suggestions using the Pydantic object
                for item in result.s:
                    idx = item.id
                    value = item.v
                    reasoning = item.r
                    confidence = item.c
                    
                    if idx is not None and value is not None and idx in results_df.index:
                        # MAINTAIN ORIGINAL DATA TYPE
                        original_dtype = df[target_column].dtype
                        try:
                            if pd.api.types.is_numeric_dtype(original_dtype):
                                value = float(value)
                            elif pd.api.types.is_datetime64_any_dtype(original_dtype):
                                value = pd.to_datetime(value)
                        except Exception:
                            pass # Fallback to string if conversion fails
                        
                        results_df.at[idx, target_column] = value
                        filled_count += 1
                        
                        # LOG TO FILE (Replaces the slow UI Expanders)
                        logger.info(f"Row {idx} | Val: {value} | Conf: {confidence} | Reason: {reasoning}")
                        
                        self.filled_count = filled_count  
                
                st.write(f"Processed batch {i//batch_size + 1}: {len(result.s)} suggestions")

        return results_df

        


    def smart_contextual_fill_ui(self, df: pd.DataFrame) -> pd.DataFrame:
        """UI for filling missing data using context from other columns."""
        
        
        if 'df' not in st.session_state or st.session_state.df is None:
            st.warning("Please load data first!")
            return
        
        #df = st.session_state.df
        
        # Step 1: Select target column (with missing data)

        #st.markdown("##### Select Target Column")

        df_missign_clean = df.replace([r'^\s*$', 'nan', 'NaN', 'None', 'NULL'], np.nan, regex=True)

        # 2. Detect: Now run your original logic on the cleaned data
        missing_cols = [col for col in df_missign_clean.columns if df_missign_clean[col].isnull().any()]


        if not missing_cols:
            st.success("No columns have missing data!")
            return


        target_column = st.selectbox(
        "Select Target Column (has missing data):",
        options=missing_cols,
        help="Select the column that contains missing values you want to fill",
        key="context_target_column"  # Add a unique key
        )
        

    
        st.write("Select Context Columns")
        context_columns = st.multiselect(
            "Choose:",
            options=[c for c in df.columns if c != target_column],
            default=[c for c in df.columns if c != target_column][:3],
            label_visibility="collapsed"  # Hides the label since we already have "Select Context Columns"
        )


        
        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

        # Step 3: AI Configuration
        st.write("AI Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            model = st.selectbox(
                "Ollama Model:",
                options=["llama2", "mistral", "gemma:2b", "codellama", "neural-chat"],
                index=0
            )
        
        with col2:
            confidence_threshold = st.select_slider(
                "Minimum Confidence:",
                options=["Low", "Medium", "High"],
                value="Medium",
                help="Only accept suggestions with at least this confidence level"
            )
        

        st.markdown("<div style='margin-top:25px'></div>", unsafe_allow_html=True) # space

        # Step 4: Fill with AI
        st.write("Fill Missing Values")
        
        if st.button("Fill Using Contextual AI", type="primary", width='stretch'):
            if not context_columns:
                st.error("Please select at least one context column!")
                return
            

            with st.spinner(f"AI is analyzing patterns between '{target_column}' "
                            f"and {len(context_columns)} context columns: {', '.join(context_columns)}"):


                try:
                    #filler = SmartMissingDataFiller(model=model)
                    result_df = self.fill_target_with_context(st.session_state.df, 
                                                                target_column, 
                                                                context_columns)

                    #st.write("Fill missing values in a target column using patterns from related columns.")
                    target_column = self.target_column
                    valid_context = self.valid_context
                    #missing_indices = self.missing_indices
                    filled_count = self.filled_count

                    # Show before/after comparison
                    st.success("Contextual imputation complete!")
                    
                    # Comparison
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Before - '{target_column}':**")
                        before_sample = st.session_state.df[[target_column] + context_columns[:3]].head(10)
                        st.dataframe(before_sample, width='stretch')
                        #width='stretch'

                    st.write(result_df)
                    st.session_state.df = result_df.copy(deep=True)
                    
                    with col2:
                        st.markdown(f"**After - '{target_column}':**")
                        after_sample = result_df[[target_column] + context_columns[:3]].head(10)
                        st.dataframe(after_sample, width='stretch')
                        #width='stretch'


                        
                    st.markdown("### Current within block DataFrame and before undo")
                    st.dataframe(st.session_state.df.head(20), width='stretch')


                    if st.session_state.df.equals(result_df):
                        undo_clicked = st.button("Undo Result", key="undo_context_fill")
                        if undo_clicked:
                            st.session_state.df = df.copy()
                            st.success("Main dataframe reverted to original state!")


                    st.markdown("### Current within block DataFrame")
                    st.dataframe(st.session_state.df.head(20), width='stretch')

                except Exception as e:
                    st.error(f"Error: {str(e)}")
                    st.info("Ensure Ollama (or your AI model) is running.")

                st.markdown("### Current DataFrame output after context fill (will update if you undo):")
                st.dataframe(st.session_state.df.head(20), width='stretch')

                csv = st.session_state.df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Context-Filled Data",
                    csv,
                    f"context_filled_{target_column}.csv",
                    "text/csv"
                )

        return st.session_state.df


    
        # Final report
        # st.success(f"""
        # # **Imputation Complete**
        # - Target column: {self.target_column}
        # - Context columns used: {len(self.valid_context)}
        # - Rows processed: {len(self.missing_indices)}
        # - Values filled: {self.filled_count}
        # - Success rate: {(self.filled_count/len(self.missing_indices)*100):.1f}%
        # """)


            #return st.session_state.df, result_df

            # # Create task for AI
            # task_description = f"""
            # You are a data imputation expert. Your task is to fill missing values in column '{target_column}' 
            # using context from related columns: {', '.join(valid_context)}.
            
            # DATA ANALYSIS:
            # {analysis_json}
            
            # BATCH OF ROWS WITH MISSING VALUES (Rows {i+1}-{min(i+batch_size, len(missing_indices))}):
            # {batch_json}
            
            # INSTRUCTIONS:
            # 1. For EACH row in the batch, analyze the context from the provided columns
            # 2. Look for patterns between '{target_column}' and the context columns
            # 3. Consider values from adjacent rows (before/after)
            # 4. Suggest the MOST APPROPRIATE value for '{target_column}' in each row
            # 5. Base your suggestions on observed patterns and relationships
            # 6. For categorical data, suggest the most likely category
            # 7. For numerical data, suggest a value that fits the pattern
            
            # Return a JSON array where each object has:
            # - "row_index": the row number
            # - "suggested_value": your suggested value for '{target_column}'
            # - "reasoning": brief explanation of why this value was chosen
            # - "confidence": your confidence level (High/Medium/Low)
            
            # EXAMPLE:
            # [
            #   {{
            #     "row_index": 5,
            #     "suggested_value": "CategoryA",
            #     "reasoning": "Based on pattern: when ColumnX='Value1', '{target_column}' is usually 'CategoryA'",
            #     "confidence": "High"
            #   }}
            # ]
            
            # Return ONLY valid JSON.
            # """
            
            # task = Task(
            #     description=task_description,
            #     agent=agent,
            #     expected_output="JSON array with suggested values and reasoning"
            # )
            
            # crew = Crew(
            #     agents=[agent],
            #     tasks=[task],
            #     verbose=False,
            #     process=Process.sequential
            # )
            
            # try:
            #     # Execute AI task
            #     result = crew.kickoff()
                
            #     # Parse results
            #     try:
            #         suggestions = json.loads(str(result))
            #     except json.JSONDecodeError:
            #         # Try to extract JSON from text
            #         import re
            #         json_match = re.search(r'\[.*\]', str(result), re.DOTALL)
            #         if json_match:
            #             suggestions = json.loads(json_match.group())
            #         else:
            #             st.error(f"Could not parse AI response: {result}")
            #             continue
                
            #     # Apply suggestions
            #     for suggestion in suggestions:
            #         idx = suggestion.get('row_index')
            #         value = suggestion.get('suggested_value')
            #         reasoning = suggestion.get('reasoning', '')
            #         confidence = suggestion.get('confidence', 'Medium')
                    
            #         if idx is not None and value is not None and idx in results_df.index:
            #             # Try to maintain original data type
            #             original_dtype = df[target_column].dtype
            #             try:
            #                 if pd.api.types.is_numeric_dtype(original_dtype):
            #                     value = float(value)
            #                 elif pd.api.types.is_datetime64_any_dtype(original_dtype):
            #                     value = pd.to_datetime(value)
            #             except:
            #                 pass  # Keep as string
                        
            #             results_df.at[idx, target_column] = value
            #             filled_count += 1
                        
            #             # Show suggestion details
            #             with st.expander(f"Row {idx}: {value} ({confidence} confidence)", expanded=False):
            #                 st.write(f"**Reasoning:** {reasoning}")
            #                 st.write(f"**Context used:**")
            #                 context_vals = {col: df.at[idx, col] for col in valid_context if pd.notna(df.at[idx, col])}
            #                 st.json(context_vals)
                
            #     st.write(f"Processed batch {i//batch_size + 1}: {len(suggestions)} suggestions")
                
            # except Exception as e:
            #     st.error(f"Error in batch {i//batch_size + 1}: {e}")
            #     # Fallback: use simple imputation for this batch
            #     for idx in batch_indices:
            #         if np.issubdtype(df[target_column].dtype, np.number):
            #             results_df.at[idx, target_column] = df[target_column].median()
            #         else:
            #             mode_val = df[target_column].mode()
            #             results_df.at[idx, target_column] = mode_val[0] if len(mode_val) > 0 else None
        
