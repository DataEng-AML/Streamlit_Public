import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from IPython import display
import numpy as np
# streamlit extras
from streamlit_extras.dataframe_explorer import dataframe_explorer


# Class to handle resolution of missing data operations
class qresolve_missing_data:
    def qaccuracy_missing_data(self, df):
        print("Missing Data Accuracy:")
        value_count = df.count().sum()
        element_count = df.size
        ratio_missing_data = (element_count - value_count) / element_count
        accuracy_missing_data = 1 - ratio_missing_data
        print(accuracy_missing_data)
        return accuracy_missing_data

    def qpredict_missing_data(self, df, column):
        print("Predict Missing Data:")
        df_column = df[[column]]
        known_data = df_column.dropna()
        unknown_data = df_column[df_column.isnull()]

        if known_data.empty or unknown_data.empty:
            return pd.Series([], index=unknown_data.index)

        X_known = known_data.index.values.reshape(-1, 1)
        y_known = known_data.values.flatten()

        model = RandomForestRegressor()
        model.fit(X_known, y_known)

        X_unknown = unknown_data.index.values.reshape(-1, 1)
        predicted_values = model.predict(X_unknown)

        print(predicted_values)
        return pd.Series(predicted_values, index=unknown_data.index)

    def qfix_missing_data(self, df, process='replace'):
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
                elif process == 'kino':
                    predict_lr = self.predict_missing_data(fixed_missing_data, column)
                    fixed_missing_data[column] = fixed_missing_data[column].combine_first(predict_lr)
        
        print(fixed_missing_data)
        return fixed_missing_data



#dedup_operations
#    def initialize_processor(df):
#        if 'processor' not in st.session_state:
#            st.session_state.processor = PyLade(df)

#    def initialize_state(df):
#        if "confirming" not in st.session_state:
#            st.session_state.confirming = False
#            st.session_state.operation = None
#            st.session_state.last_operation_data = df  # Initial data state
#            st.session_state.cleaned_data_dedup = df  # Current data state
class dedup_data_operations:
    @staticmethod
    def dedup_apply(processor, df):
        # Save the current state before applying the new operation
        st.session_state.last_operation_data = st.session_state.cleaned_data_dedup
        # Simulate applying the operation
        st.session_state.cleaned_data_dedup = processor.fix_duplicate_data(df)
        st.write(st.session_state.cleaned_data_dedup)

    def dedup_undo(df):
        # Save the current state before undoing
        st.session_state.last_operation_data = st.session_state.cleaned_data_dedup
        # Simulate undoing the operation
        st.session_state.cleaned_data_dedup = df
        st.write(st.session_state.cleaned_data_dedup)

    def dedup_confirm():
        st.session_state.confirming = True
        st.session_state.operation = "Fix Duplicate Data"

    def dedup_operations(df):
        # Initialize processor and state
        initialize_processor(df)
        processor = st.session_state.processor
        initialize_state(df)

        # Select operations
        selected_operations = st.multiselect(
            "Select Operations: start with :red[Fix Duplicate Data]",
            ["Fix Duplicate Data", "Fix Missing Data", "Fix Unstandardized Data", "Show Final Data"]
        )

        for operation in selected_operations:
            if operation == "Fix Duplicate Data":
                selected_operation = st.radio(
                    "Select Operation:",
                    ["Apply", "Undo", "Confirm"],
                    horizontal=True
                )

                # Perform action based on selected operation
                if selected_operation == "Apply":
                    apply_operation(processor, df)
                elif selected_operation == "Undo":
                    undo_operation(df)
                    st.write("Choose **:green[Apply]** to restart the **:red[Fix Duplicate Data]** process")
                elif selected_operation == "Confirm":
                    confirm_operation()

                # Handle confirmation step
                if st.session_state.confirming and st.session_state.operation == "Fix Duplicate Data":
                    confirm_button = st.radio(
                        "Are you sure?",
                        ["Select", "Yes", "No"],
                        horizontal=True
                    )
                    if confirm_button == "Select":
                        st.write("Select **:green[Yes]** to **Confirm** or **:green[No]** to revert.")
                        st.session_state.confirming = False
                        st.session_state.operation = None
                    elif confirm_button == "Yes":
                        # Perform the confirmation action
                        cleaned_data = st.session_state.cleaned_data_dedup
                        st.write(cleaned_data)
                        processor.cleaned_data = cleaned_data
                        # Reset confirmation state
                        st.session_state.confirming = False
                        st.session_state.operation = None
                        return cleaned_data
                    elif confirm_button == "No":
                        # Cancel the confirmation
                        st.write("**Operation cancelled.** Choose **:green[Apply]** to restart the **:red[Fix Duplicate Data]** process")
                        cleaned_data = undo_operation(df)
                        st.write(cleaned_data)
                        st.session_state.confirming = False
                        st.session_state.operation = None

    # Example usage:
    # df = your_dataframe_here
    # handle_operations(df)
