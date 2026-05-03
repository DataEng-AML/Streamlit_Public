from crewai.project import agent
from sklearn import pipeline
import streamlit as st
import pandas as pd
import numpy as np
import uuid
import builtins
import io
from contextlib import contextmanager
import time

# For input mocking context manager convenience
from data_anomaly_agent import DataAnomalyAgent

################ Wrapper Data Anomaly ################

class WrapperDataAnomalyAgent():


    def __init__(self):
        agent = st.session_state.get("anomaly_agent")

        if not isinstance(agent, DataAnomalyAgent):
            agent = DataAnomalyAgent()

            if "anomaly_agent" in st.session_state:
                del st.session_state["anomaly_agent"]
    
            st.session_state["anomaly_agent"] = agent


        self.agent = agent


    def choose_features_models(self, df, key_prefix="default"):
        numeric_cols = df.select_dtypes(include="number").columns.tolist()

        if not isinstance(self.agent, DataAnomalyAgent):
            st.error("Anomaly agent not properly initialized")
            return [], []
        
        try:
            model_dict = self.agent.model_dict()
        except TypeError:
            model_dict = self.agent.model_dict

        if not isinstance(model_dict, dict):
            st.error(f"Expected dict, got {type(model_dict)}")
            return [], []
        
        model_keys = list(model_dict.keys())
        
        #timestamp = int(time.time() * 1000)  # Milliseconds
        #feature_key = f"{key_prefix}_features_{timestamp}"
        #model_key = f"{key_prefix}_models_{timestamp}"

        feature_key = f"{key_prefix}_features"
        model_key = f"{key_prefix}_models"


        st.markdown(
        "<h3 style='font-size:20px; color: white;'>Detection Parameters </h3>",
        unsafe_allow_html=True
        )
        

        # # Initialize session state with defaults (only if not already set for this specific key)
        # if feature_key not in st.session_state:
        #     st.session_state[feature_key] = [numeric_cols[0]] if numeric_cols else []
        
        # if model_key not in st.session_state:
        #     st.session_state[model_key] = [model_keys[0]] if model_keys else []
        
        # st.markdown(
        #     "<h3 style='font-size:20px; color: white;'>Detection Parameters </h3>",
        #     unsafe_allow_html=True
        # )
        
    
        # Create widgets WITHOUT default parameter (session state handles it)
        features = st.multiselect(
            "Select a numeric feature (add/remove)",
            numeric_cols,
            default=[numeric_cols[0]] if numeric_cols else [],
            key=feature_key
        )
        
        models = st.multiselect(
            "Select a model (add/remove)",
            model_keys,
            default=[model_keys[0]] if model_keys else [],
            key=model_key
        )
        
        # ✅ Store selections in a predictable location for later retrieval
        # Use the original prefix (without timestamp) for consistent access
        original_feature_key = f"{key_prefix}_features"
        original_model_key = f"{key_prefix}_models"
        
        # Only set if not already set (first run)
        if original_feature_key not in st.session_state:
            st.session_state[original_feature_key] = features
        if original_model_key not in st.session_state:
            st.session_state[original_model_key] = models
        
        return features, models



        # # Initialize session state
        # if feature_key not in st.session_state:
        #     st.session_state[feature_key] = [numeric_cols[0]] if numeric_cols else []
        
        # if model_key not in st.session_state:
        #     st.session_state[model_key] = [model_keys[0]] if model_keys else []
        
        # st.markdown(
        #     "<h3 style='font-size:20px; color: white;'>Detection Parameters </h3>",
        #     unsafe_allow_html=True
        # )
        
        # features = st.multiselect(
        #     "Select a numeric feature (add/remove)",
        #     numeric_cols,
        #     default=st.session_state[feature_key]
        # )
        
        # models = st.multiselect(
        #     "Select a model (add/remove)",
        #     model_keys,
        #     default=st.session_state[model_key]
        # )
        
        # return features, models


    def wrapper_model_dict(self):
        model_dict = self.agent.model_dict()
        return model_dict


    def anomaly_dictionary(self, df, features, models):
        """Run and display full anomaly detection."""
        st.info("Producing anomaly, please wait...")
        with st.spinner("Anomaly Dictionary..."):

            working_df = df.copy()

            # ✅ If features are provided, use only them
            if features:
                working_df = working_df[features]

            # ✅ Keep only numeric columns for anomaly detection
            numeric_df = working_df.select_dtypes(include=[np.number])

            # Safety check: make sure we have numeric data
            if numeric_df.empty:
                st.error("No numeric columns available for anomaly detection.")
                return None

            # Optional debug info
            st.write("Using numeric columns:", numeric_df.columns.tolist())

            # Run anomaly detection on numeric data only
            anomaly_dict = self.agent.crewai_detect_anomalies(
                numeric_df,
                chosen_models=models,
                feature_names=numeric_df.columns.tolist()
            )

            st.success("Anomaly Dictionary complete!")
            st.write("Features with anomalies per index:")

            # Convert results to DataFrame
            if isinstance(anomaly_dict, dict):
                anomaly_dict = pd.DataFrame.from_dict(anomaly_dict)
            
            if not isinstance(anomaly_dict, pd.DataFrame) or anomaly_dict.empty:
                st.warning("No anomalies detected or empty results.")
                return pd.DataFrame() 



            # Keep only rows and columns with anomalies
            anomaly_dict_df = anomaly_dict.loc[:, (anomaly_dict == 1).any(axis=0)]
            anomaly_dict_df = anomaly_dict_df[(anomaly_dict_df == 1).any(axis=1)]

            st.dataframe(anomaly_dict_df)

        return anomaly_dict_df



        
    def run_detection_across_models(self, df, models):
        """Run and display full anomaly detection."""
        st.info("Detecting anomalies by different models, please wait...")
        with st.spinner("Detecting across models..."):
            try:
                diff_model_results_df = self.agent.detect_anomalies_across_models(df, model_types=models)
                if diff_model_results_df is None:
                    st.error("No results returned (None)")
                    return None
                if diff_model_results_df.empty:
                    st.warning("Returned DataFrame is empty")
                    return diff_model_results_df
  
                return diff_model_results_df
            
            except Exception as e:
                st.error(f"Exception during detection: {e}")
                return None

  
    def run_detection(self, df, features, models):
        """Run and display full anomaly detection."""
        st.info("Running anomaly detection, please wait...")
        with st.spinner("Detecting combined anomaly..."):
            results_df = self.agent.combined_anomaly_detection(df, chosen_models=models, feature_names=features)
            #results_df = self.agent.detect_anomalies_across_models(df, model_types=models)
            st.success("Detection complete!")
            st.subheader("Anomaly Matrix:")
            st.dataframe(results_df)
        return results_df

    def create_anomaly_summary(self, df, features, models):
        """
        Run anomaly detection across all selected models and create a summary table.
        
        Returns:
            pd.DataFrame: Summary table with Model and Detected Anomalies count
        """
        if not features or not models:
            return None
        
        # Work with numeric columns only
        working_df = df[features].copy()
        numeric_df = working_df.select_dtypes(include=[np.number])
        
        if numeric_df.empty:
            st.error("No numeric columns available for anomaly detection.")
            return None
        
        summary_data = []
        
        # Run detection for each model
        for model in models:
            try:
                # Run anomaly detection for this single model
                anomaly_result = self.agent.crewai_detect_anomalies(
                    numeric_df,
                    chosen_models=[model],
                    feature_names=numeric_df.columns.tolist()
                )
                
                # Convert to DataFrame if needed
                if isinstance(anomaly_result, dict):
                    anomaly_df = pd.DataFrame.from_dict(anomaly_result)
                else:
                    anomaly_df = anomaly_result
                
                # Count anomalies (assuming 1 = anomaly)
                if anomaly_df is not None and not anomaly_df.empty:
                    anomaly_count = (anomaly_df == 1).sum().sum()
                else:
                    anomaly_count = 0
                
                summary_data.append({
                    'Model': model,
                    'Detected Anomalies': anomaly_count
                })
                
            except Exception as e:
                st.error(f"Error running {model}: {e}")
                summary_data.append({
                    'Model': model,
                    'Detected Anomalies': 0
                })
        
        return pd.DataFrame(summary_data)

    def download_area(self, df, filename, label_text):
        csv = df.to_csv(index=True).encode()
        st.download_button(label=label_text, data=csv, file_name=filename, mime="text/csv")


    def show_anomaly_plot(self, df, features, models):
        st.info("Generating anomaly plot...")

        fig = self.agent.crewai_plot_anomalies_plotly(
            df[features],
            chosen_models=models
        )

        if fig:
            st.success("Anomaly plot complete!")
            st.subheader("Anomaly Plot:")
            st.plotly_chart(fig, width='stretch')




    def fix_anomaly_flow(self, df: pd.DataFrame, features: list[str], models: list[str]):
        
        """
        Streamlit wrapper for crewai_fix_anomalies from DataAnomalyAgent
        without modifying the original method (uses input() monkeypatch).
        """

        agent = DataAnomalyAgent()

        # Parameters dependent on fix method
        decimal_places = st.number_input(
            "Decimal places (for mean)", value=2, min_value=0, max_value=10, key="mean_dp"
        ) #if fix_method == 'mean' else None


        # Select Fix Method and parameters
        fix_method_list = [
            'mean', 'median', 'mode', 'interpolation', 'knn', 'simple_imputer',
            'moving_average', 'winsorization', 'zscore', 'quantile', 'random_forest'
        ]
        fix_method = st.selectbox("Select a fix method", fix_method_list, key='fix_ano_ft')
        #columns_to_fix = st.multiselect("Columns to fix", features, default=features, key="columns_to_fix")

        # Persist dataframe in session state
        if "df" not in st.session_state:
            st.session_state.df = df
         
        # Persist dataframe in session state
        if "fixed_df" not in st.session_state:
            st.session_state.fixed_df = None

        # Persist selected features in session state
        if "selected_features" not in st.session_state:
            st.session_state.selected_features = features

        columns_to_fix = st.multiselect(
            "Columns to fix",
            options=features,
            default=features,#st.session_state.selected_features,
            key="selected_features"  # keys allow session_state to auto-store the selection
        )


        si_strategy_list = ['mean', 'median', 'most_frequent', 'constant']
        simple_strategy = st.selectbox("SimpleImputer strategy", si_strategy_list, key="si_strategy") if fix_method == 'simple_imputer' else None
        simple_constant = st.text_input(
            "SimpleImputer constant value", value="9999", key="si_constant"
        ) if fix_method == 'simple_imputer' and simple_strategy == 'constant' else None

        window_size = st.number_input(
            "Rolling window size", value=3, min_value=1, step=1, key="ma_window"
        ) if fix_method == 'moving_average' else None


        # Step 2: Run fixing with inputs simulated via monkeypatch on input()
        if st.button("Batch Impute / Fix Anomalies", key="fix_anomalies_button"):
            # Must have anomaly_dict from detection step
            anomaly_dict = st.session_state.get('anomaly_dict', None)
            if anomaly_dict is None:
                st.error("Please run anomaly detection first!")
                return None
            if len(columns_to_fix) == 0:
                st.warning("Please select at least one column to fix.")
                return None
            
            DataAnomalyAgent.DEFAULT_DECIMAL_PLACES = decimal_places

            # Build simulated inputs list according to your backend input() order:
            simulated_inputs = [fix_method]
                
            if fix_method == 'mean':
                if not decimal_places:
                    st.error("Please specify decimal places for 'mean' fix method.")
                    return None
                simulated_inputs.append(str(decimal_places))
            

            if fix_method == 'simple_imputer':
                if simple_strategy == 'constant' and not simple_constant:
                    st.error("Please specify constant value for SimpleImputer strategy 'constant'.")
                    return None
    
                for _ in columns_to_fix:
                    simulated_inputs.append(simple_strategy)
                    if simple_strategy == 'constant':
                        simulated_inputs.append(str(simple_constant))
                    elif simple_strategy == 'mean':
                        simulated_inputs.append(str(decimal_places))
                        
                        
            #st.write("simulated_inputs after building:", simulated_inputs)


            if fix_method == 'moving_average':
                simulated_inputs.append(str(window_size))  # e.g. '3'

            # Monkeypatch input() to return these answers in order
            input_iter = iter(simulated_inputs)
            original_input = builtins.input

            
            #show_warnings = st.checkbox("Show unexpected input prompts warnings", value=False)
            warning_logs = []


            def mock_input(prompt=None):
                try:
                    val = next(input_iter)
                    #st.write(f"INPUT PROMPT: {prompt} -> returning: {val}")
                    return val
                except StopIteration:
                    msg = f"Unexpected input prompt: {prompt} - returning default valid value to avoid error."
                    warning_logs.append(msg)
                    st.warning(msg)

                    if 'decimal places' in (prompt or '').lower():
                        return '2'
                    if 'window size' in (prompt or '').lower():
                        return '3'
                    if 'strategy' in (prompt or '').lower():
                        return 'mean'
                    return '0'



            builtins.input = mock_input


            # In fix_anomaly_flow method, before calling crewai_fix_anomalies:
            if df is None:
                st.error("DataFrame is None! Cannot fix anomalies.")
                return None

            if not isinstance(df, pd.DataFrame):
                st.error(f"Invalid data type: {type(df)}. Expected DataFrame.")
                return None

            # Check if dataframe is empty
            if df.empty:
                st.warning("DataFrame is empty. Nothing to fix.")
                return df

            try:
                fixed_df = agent.crewai_fix_anomalies(
                    data=df,
                    columns_to_fix=columns_to_fix,
                    chosen_models=models,
                    anomaly_dict=anomaly_dict
                )
                st.session_state.fixed_df = fixed_df
            finally:
                builtins.input = original_input  # Always restore

            



            # Post-fix display and download
            if fixed_df is None or fixed_df.empty:
                st.error("No fixed DataFrame returned.")
                return None

            if fixed_df.equals(df):
                st.warning("Fixed DataFrame is identical to input (no changes applied). Please check anomaly detection and inputs.")
            else:
                st.success("Anomaly fixing completed.")

            #st.dataframe(fixed_df)

            try:
                styled_fixed_df = fixed_df.style.format(precision=decimal_places)
                st.dataframe(styled_fixed_df.head(5))
            except Exception:
                st.dataframe(fixed_df.head(5))


            csv = fixed_df.to_csv(index=False).encode()
            st.download_button(label="⬇️ Download Fixed Data", data=csv, file_name="anomaly_fixed.csv", mime="text/csv")

            # Log and download warnings
            if warning_logs:
                #if show_warnings:
                st.text_area("Warnings Log", "\n".join(warning_logs), height=150)

                # Download warnings
                warnings_bytes = "\n".join(warning_logs).encode("utf-8")
                st.download_button(
                    label="⬇️ Download warnings log",
                    data=warnings_bytes,
                    file_name="fixing_warnings.log",
                    mime="text/plain"
                )

            fixed_df = st.session_state["fixed_df"]

            if "fixed_df" in st.session_state and st.session_state["fixed_df"] and "df" not in st.session_state:
                df = st.session_state["fixed_df"]

            return fixed_df

        return None
    

    def human_ai_anomaly_workflow(self, df: pd.DataFrame, features: list[str], default_models: list[str]):
        st.subheader("Human-AI Anomaly Correction Workflow")

        # Model name to numerical ref mapping your backend expects:
        model_name_to_ref = {
            "ABOD": "1", "LODA": "2", "HBOS": "3", "IForest": "4", "KNN": "5",
            "LOF": "6", "MCD": "7", "OCSVM": "8", "PCA": "9", "INNE": "10",
            "GMM": "11", "KDE": "12", "DIF": "13", "COPOD": "14", "ECOD": "15",
            "SUOD": "16", "QMCD": "17", "Sampling": "18", "MAD": "19", "LOCI": "20",
            "SOD": "21", "SOS": "22", "ROD": "23"
        }

        # Step 1: User selects models and features
        all_models = list(model_name_to_ref.keys())
        chosen_models = st.multiselect(
            "Select anomaly detection models",
            all_models,
            default=["KNN"],#default_models if isinstance(default_models, list) else [default_models],
            key="hai_chosen_models"
        )
        if not chosen_models:
            st.warning("Please select at least one anomaly detection model.")
            return

        columns_to_check = st.multiselect(
            "Select feature columns for anomaly detection",
            features,
            default=features,
            key="hai_selected_features"
        )
        if not columns_to_check:
            st.warning("Please select at least one feature column to check.")
            return

        input_df = df[columns_to_check].copy()

        if "input_df" not in st.session_state:
            st.session_state["input_df"] = input_df.copy()

        # Step 2: Choose fix method and parameters (simplified example)
        fix_method = st.selectbox(
            "Select a fix method",
            ['mean', 'median', 'mode', 'simple_imputer', 'interpolation', 'knn'],
            index=0,
            key="hai_fix_method"
        )

        decimal_places = None
        simple_strategy = None
        simple_constant = None

        if fix_method == 'mean':
            decimal_places = st.number_input(
                "Decimal places for mean fix",
                min_value=0,
                max_value=10,
                value=2,
                key="hai_decimal_places"
            )
        if fix_method == 'simple_imputer':
            simple_strategy = st.selectbox(
                "SimpleImputer strategy",
                ['mean', 'median', 'most_frequent', 'constant'],
                index=0,
                key="hai_simple_strategy"
            )
            if simple_strategy == 'constant':
                simple_constant = st.text_input(
                    "Constant fill value",
                    value="9999",
                    key="hai_simple_constant"
                )

        # Session state for storing anomaly table and human corrections
        if "anomaly_table" not in st.session_state:
            st.session_state["anomaly_table"] = None
        if "human_corrections" not in st.session_state:
            st.session_state["human_corrections"] = None


        # Helper: mock input to feed CLI prompts expected by backend
        def run_with_mocked_input():
            # Compose mocked inputs in exact backend expected order

            # 1) Model numerical refs comma separated
            model_refs = ",".join(model_name_to_ref[m] for m in chosen_models)

            mocked_inputs = [model_refs, fix_method]

            # 2) Fix method params depending on fix_method
            if fix_method == 'mean':
                mocked_inputs.append(str(decimal_places))
            elif fix_method == 'simple_imputer':
                # For each selected column, backend expects:
                # a) strategy, b) constant value or dummy, c) decimal places if needed
                for _ in columns_to_check:
                    mocked_inputs.append(simple_strategy)
                    mocked_inputs.append(simple_constant if simple_strategy == 'constant' else "0")
                    if simple_strategy == 'mean':
                        mocked_inputs.append(str(decimal_places))

            # You can extend this for other fix methods if your backend requests inputs

            input_iter = iter(mocked_inputs)
            original_input = builtins.input

            def mocked_input(prompt=None):
                try:
                    val = next(input_iter)
                    st.write(f"INPUT PROMPT: {prompt} → returning: {val}")
                    return val
                except StopIteration:
                    st.warning(f"Unexpected input prompt: '{prompt}' -> returning '0'")
                    return "0"

            builtins.input = mocked_input
            try:
                # 1. Detect anomalies
                anomaly_dict = self.agent.crewai_detect_anomalies(input_df, chosen_models)

                # Defensive: convert to DataFrame if dict
                if isinstance(anomaly_dict, dict):
                    anomaly_dict = pd.DataFrame.from_dict(anomaly_dict)

                columns_to_fix = list(anomaly_dict.columns)

                # 2. Fix anomalies passing full anomaly DataFrame

                fixed_df = self.agent.crewai_fix_anomalies(
                    data=df,
                    columns_to_fix=columns_to_fix,
                    chosen_models=chosen_models,
                    anomaly_dict=anomaly_dict
                )

                # 3. Build anomaly table (combine anomaly flags and fixes to display or allow fix)
                records = []
                for col in anomaly_dict.columns:
                    anomalies = anomaly_dict[col] == 1
                    for idx in anomaly_dict.index[anomalies]:
                        records.append({
                            "Index": idx,
                            "Column Name": col,
                            "Anomaly Value": input_df.at[idx, col],
                            "Code Corrected Value": fixed_df.at[idx, col],
                            "Final Human Value": fixed_df.at[idx, col]
                        })
                anomaly_table = pd.DataFrame(records)
            finally:
                builtins.input = original_input

            return anomaly_table


        if "human_corrections" not in st.session_state:
            st.session_state["human_corrections"] = anomaly_table.copy() 

        # This entire detection & fixing ONLY happens on button click:
        if st.button("Find and Fix Anomalies"):
            with st.spinner("Detecting and fixing anomalies..."):
                anomaly_table = run_with_mocked_input()
                if anomaly_table is not None:
                    st.session_state["anomaly_table"] = anomaly_table
                    st.session_state["human_corrections"] = None
                    st.write("Dataframe after detection and fixing anomaly.")
                    st.dataframe(st.session_state["fixed_df"].head())
                else:
                    st.info("No anomalies detected or empty output.")


        # Display anomaly table and allow human corrections if available
        anomaly_table = st.session_state.get("anomaly_table")
        if anomaly_table is not None:
            if anomaly_table.empty:
                st.success("No anomalies detected!")
            else:
                st.write("Anomalies Detected and AI Fixes")
                st.dataframe(anomaly_table)

                st.write("Human Review and Correction (Optional)")

                # Initialize human_corrections if not present or None
                if "human_corrections" not in st.session_state or st.session_state["human_corrections"] is None:
                    st.session_state["human_corrections"] = anomaly_table.copy()

                # Show input boxes for user to correct values
                corrected_values = []
                human_corr_df = st.session_state["human_corrections"]
                for i, row in human_corr_df.iterrows():
                    user_input = st.text_input(
                        label=f"Row {row['Index']} | Col {row['Column Name']}",
                        value=str(row.get('Final Human Value', row['Code Corrected Value'])),
                        key=f"human_corr_{i}_{row['Column Name']}"
                    )
                    corrected_values.append(user_input)

                if st.button("Apply Human Corrections", key="apply_human_corrections"):
                    # Update human_corrections DataFrame with user inputs
                    edited_table = human_corr_df.copy()
                    for i, val in enumerate(corrected_values):
                        edited_table.at[i, 'Final Human Value'] = val

                    st.session_state["human_corrections"] = edited_table
                    st.success("Human corrections saved.")


            if "fixed_df" in st.session_state and st.session_state["fixed_df"] is not None and "human_corrections" in st.session_state:
                mod_fixed_data = st.session_state["fixed_df"].copy()



                st.dataframe(mod_fixed_data.head())


                human_corr_df = st.session_state["human_corrections"]

                st.dataframe(human_corr_df.head())

                for _, row in human_corr_df.iterrows():
                    idx = row["Index"]
                    col = row["Column Name"]
                    mod_fixed_data.at[idx, col] = row["Final Human Value"]


                st.dataframe(mod_fixed_data.head())

                st.session_state["df"] = mod_fixed_data


                mod_updated_human_corr = human_corr_df.copy()
                for i, row in mod_updated_human_corr.iterrows():
                    idx = row["Index"]
                    col = row["Column Name"]
                    # Get the updated value from fixed_df
                    mod_updated_human_corr.at[i, "Final Human Value"] = mod_fixed_data.at[idx, col]

                st.session_state["human_corrections"] = mod_updated_human_corr

                st.write("Dataframe after ai correction.")
                st.dataframe(st.session_state["fixed_df"].head())

                st.write("Dataframe after human correction.")
                st.dataframe(st.session_state["df"].head())

                # Download button for the fully corrected DataFrame
                csv_bytes = st.session_state["df"].to_csv(index=False).encode()
                st.download_button(
                    label="Download Corrected Full Data CSV",
                    data=csv_bytes,
                    file_name="corrected_full_data.csv",
                    mime="text/csv"
                )

            else:
                st.warning("Original input data or human corrections missing.")
                st.dataframe(st.session_state["fixed_df"])


        # Optional: Reset workflow
        if st.button("If required. reset Anomaly Correction Workflow"):
            st.session_state["anomaly_table"] = None
            st.session_state["human_corrections"] = None
            st.session_state["fixed_df"] = None
            st.rerun()

    def anomaly_dict_to_df(self, ano_dict):
        """Dictionary to sparse anomaly DataFrame."""
        dataframe = pd.DataFrame(ano_dict)
        anomaly_df = dataframe.where(dataframe == 1).dropna(how='all')
        anomaly_df.index.name = 'Index'
        return anomaly_df