import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objs as go
from streamlit_extras.add_vertical_space import add_vertical_space
from data_read_agent import DataReadAgent, DataFrameOutput
import ast
import math
import re
from datetime import datetime
import operator
from html import escape


# Get the utility module for feature engineering
import feature_engineering_utils as feu
#from missing_data_agent import MissingDataAgent


class FeaturesEngineeringAgent():
    def __init__(self):
        self.df = None

    def preprocess_data(self,df):
        #df = crew_ai.llm_explore()  # this provide a dataframe with named features, proper datatypes and other references required for LLM exploration
        if 'df' in st.session_state:
            df = st.session_state.df
                    
            try:
                data_agent = DataReadAgent()
                
                # Radio button to select operation
                selected_preprocess = st.radio(
                    "Select Preprocessing:",
                    #["Initial Statistics", "Remove Duplicates", "Add Headers", "Assign Data Types", "Create New Feature", "Rename Features", "View Interval Records", "Delete Records", "Delete Columns", "Edit Values"],#, "Reset Last Operation"
                    ["Add Headers", "Remove Duplicates", "Assign Data Types", "Create New Feature", "Rename Features", "View Interval Records", "Subset Dataframe", "Delete Records", "Delete Columns", "Edit Values"],#, "Reset Last Operation"
                    horizontal = True

                )

                # Perform action based on selected operation
                if selected_preprocess == "Initial Statistics":
                    # Option to check initial general statistics of the data / DataFrame
                    num_df_col = len(st.session_state.df.columns)
                    num_df_row = len(st.session_state.df)

                    st.markdown(f"Head of the dataframe with **:green[{num_df_col} columns]** and **:green[{num_df_row} rows]**")

                    st.dataframe(st.session_state.df.head())  # Display the updated DataFrame

                    # Generate the statistics of the dataframe
                    self.df_statistics()

                # Option to assign data types to columns
                if selected_preprocess == "Remove Duplicates":
                    raw_data = df.copy(deep=True)
                    st.dataframe(df.head())  # Display the updated DataFrame]

                    # Check if duplicates exist
                    if df.duplicated().any():
                        st.warning(f":red[Duplicates detected!] There are **:red[{df.duplicated().sum()}]** duplicate rows in the dataset.")
                    else:
                        st.success("✅ No duplicate rows detected in the dataset.")
                        # Disable the Remove Duplicates button if no duplicates
                        #with st.container():
                        st.stop()
                        

                    # Create columns just for buttons
                    col1, col2, col3, col4 = st.columns(4)

                    # Variables to store results
                    df_to_show = None
                    message_to_show = ""

                    with col1:
                        if st.button("Remove Duplicates", width='stretch'):
                            # Always remove duplicates from the ORIGINAL dataframe
                            df_dedup = raw_data.drop_duplicates()
                            df_to_show = df_dedup.head()
                            # Update the current dataframe
                            current_df = df_dedup
                            st.session_state.df = df_dedup
                            message_to_show = f"There were **:red[{len(raw_data)}]** records before duplicate removal, and **:green[{len(df_dedup)}]** records after duplicate removal. Removed {len(raw_data) - len(df_dedup)} duplicate rows."
                    
                    with col2:
                        if st.button("Revert Duplicate Removal", key="revert_duplicate_button", width='stretch'):
                            # Always revert to the ORIGINAL dataframe
                            current_df = raw_data
                            st.session_state.df = raw_data
                            df_to_show = st.session_state.df.head()
                            message_to_show = f"Reverted duplicate removal, the complete **:green[{len(st.session_state.df)}]** records have been restored."

                    # Display results outside columns
                    if df_to_show is not None:
                        st.write("DataFrame Preview:")
                        st.dataframe(df_to_show)
                        
                    if message_to_show:
                        st.markdown(message_to_show)
                        if "Removed" in message_to_show:
                            st.success("✅ Operation completed!")


                         
                # Add headers to the DataFrame
                if selected_preprocess == "Add Headers":
                    st.markdown("<hr style='margin: 1px 0;'>", unsafe_allow_html=True)                    
                   
                    #st.markdown(":red[This is only for dataframes without feature names (first row actual record is the header!)]<br>This solution pushed the row down into index 0, and creates headers with user entries:", unsafe_allow_html=True)

                    st.markdown('<p style="color: #FF0000; font-weight: bold; background-color: #FFEEEE; padding: 10px; border-radius: 5px;">💡 Note:  "Add Headers" is only for dataframes without feature names (first row actual record is the header! )</p>', unsafe_allow_html=True)
                    st.markdown(":red[&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; This solution pushed the row down into index 0, and creates headers with user entries!]", unsafe_allow_html=True)


                    #st.markdown(""" <p style='color: red; text-shadow: 1px 1px 1px white;'>This is only for dataframes without headers, else use Rename Features:</p>""", unsafe_allow_html=True)
                    st.markdown("<hr style='margin: 1px 0;'>", unsafe_allow_html=True)

                    new_headers = st.text_input("Enter new headers, separate with comma with no space:")
                    # if new_headers:
                    #     headers = new_headers.split(",")  # Split input into list of headers
                    #     df = data_agent.add_headers_to_df(df, headers)  # Replace df with updated version
                    #     df.columns = headers
                    #     st.session_state.df = df
                    #     st.write("Updated DataFrame with new headers:")
                    #     df = st.session_state.df
                    #     st.dataframe(df.head())  # Display the updated DataFrame]
                        
                    # st.dataframe(st.session_state.df.head())                       

                    if new_headers:
                        headers = [h.strip() for h in new_headers.split(",")]
                        
                        if 'df' in st.session_state and st.session_state.df is not None:
                            df = st.session_state.df.copy()
                            
                            if len(headers) == df.shape[1]:
                                # Push the first row down as data and set new headers
                                # First row becomes part of the data
                                # Create a new dataframe with the first row as data
                                first_row_as_data = pd.DataFrame([df.iloc[0].values], columns=headers)
                                # Then append the rest of the rows (from index 1 onwards)
                                rest_of_data = pd.DataFrame(df.iloc[1:].values, columns=headers)
                                # Combine them
                                df_new = pd.concat([first_row_as_data, rest_of_data], ignore_index=True)
                                
                                st.session_state.df = df_new
                                st.success(f"Successfully added headers and pushed first row to index 0!")
                                st.dataframe(st.session_state.df.head())
                            else:
                                st.error(f"Header count mismatch...")

                    st.dataframe(st.session_state.df.head())


                    #crew_ai.df_statistics()
                
                # Option to assign data types to columns
                if selected_preprocess == "Assign Data Types":
                    st.dataframe(df.head()) 
                    column_names = df.columns.tolist()
                    column_dtypes = {}
                    data_types = ['float64', 'int64', 'object', 'bool', 'datetime64[ns]']
                    #column_names = df.columns.tolist()
                    #column_dtypes = {}

                    # Initialize or get from session state
                    if "max_rows_per_col" not in st.session_state:
                        st.session_state["max_rows_per_col"] = 5  # default value

                    # Create two columns for label and dropdown, with fixed widths to keep dropdown small
                    col_label, col_select = st.columns([3, 1], gap="large")

                    with col_label:
                        st.markdown("**Maximum rows per column**")

                    with col_select:
                        max_rows_options = list(range(1, 11))
                        max_rows_per_col = st.selectbox(
                            label="",  # no label, as label is in separate column
                            options=max_rows_options,
                            index=max_rows_options.index(st.session_state["max_rows_per_col"]),
                            key="max_rows_per_col_selectbox"
                        )
                        st.session_state["max_rows_per_col"] = max_rows_per_col

                    # Inject CSS for compact layout (optional, to keep dropdown narrow and compact)
                    st.markdown("""
                    <style>
                        div[data-testid="column"] {
                            min-width: 0 !important;
                            max-width: 220px !important;
                            flex: unset !important;
                        }
                        .stSelectbox label {
                            display: none !important;
                        }
                        .stSelectbox, .stTextInput {
                            margin-bottom: 0 !important;
                            padding-top: 0 !important;
                            padding-bottom: 0 !important;
                        }
                    </style>
                    """, unsafe_allow_html=True)

                    # Your existing chunk/grid logic (example)
                    def chunks(lst, n):
                        for i in range(0, len(lst), n):
                            yield lst[i:i + n]

                    num_cols = math.ceil(len(column_names) / max_rows_per_col)
                    column_chunks = list(chunks(column_names, max_rows_per_col))
                    cols = st.columns(num_cols, gap="large")

                    for col_idx, chunk in enumerate(column_chunks):
                        with cols[col_idx]:
                            for col in chunk:
                                st.markdown(f"<b>{col}</b>", unsafe_allow_html=True)
                                curr_dtype = str(df[col].dtype)
                                try:
                                    idx = data_types.index(curr_dtype)
                                except ValueError:
                                    idx = 0
                                column_dtypes[col] = st.selectbox(
                                    label="",
                                    options=data_types,
                                    index=idx,
                                    key=f"dtype_select_{col}"
                                )

                    st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space

                    if st.button("Apply Data Types"):
                        df = data_agent.assign_features_dtypes(df, column_dtypes)  # Replace df with updated version
                        st.write("Data types applied successfully:")
                        st.session_state.df = df
                        df = st.session_state.df
                        st.dataframe(df.head())  # Display the updated DataFrame]

                    st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True) # space

                # Option to create a new feature


                if selected_preprocess == "Create New Feature":
                    df = st.session_state.df.copy()
                    st.dataframe(df.head())

                    # Improved UI layout
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        new_feature_name = st.text_input(
                            "New feature name:",
                            placeholder="e.g., total_score",
                            help="Name for the new feature. After success keep it blank"
                        )

                        # Before creating new feature, check for duplicates
                        if new_feature_name in df.columns:
                            st.error(f"Column '{new_feature_name}' already exists!")
                            return

                    with col2:
                        # Add import library section
                        import_text = st.text_area(
                            "Import Libraries (Optional):",
                            value="import numpy as np\nimport pandas as pd",
                            height=100,
                            help="Add Python imports if required"
                        )


                    with col3:
                        feature_expression = st.text_input(
                            "Python expression for new feature:",
                            placeholder="e.g., math_score * 0.7 + english_score * 0.3",
                            help="Use column names with Python/math operations"
                        )

                    # Column position control
                    st.markdown("---")
                    st.write("**Column Position Control**")

                    pos_col1, pos_col2, pos_col3 = st.columns([2, 2, 1])

                    with pos_col1:
                        position_option = st.radio(
                            "Insert position:",
                            options=["At end (default)", "At specific index", "Before/After column"],
                            index=0,
                            horizontal=True
                        )

                    with pos_col2:
                        if position_option == "At specific index":
                            max_index = len(df.columns)
                            position_index = st.number_input(
                                "Index position:",
                                min_value=0,
                                max_value=max_index,
                                value=max_index,
                                help=f"0 = first column, {max_index} = after last column"
                            )
                        elif position_option == "Before/After column":
                            reference_col = st.selectbox(
                                "Reference column:",
                                options=df.columns.tolist(),
                                index=len(df.columns)-1 if len(df.columns) > 0 else 0
                            )
                            relative_position = st.radio(
                                "Position:",
                                options=["Before", "After"],
                                index=1,
                                horizontal=True
                            )
                    
                    with pos_col3:
                    
                        # CREATE NEW FEATURE BUTTON
                        #st.markdown("---")
                        if st.button("Create New Feature", type="primary", width='stretch'):
                            if not new_feature_name:
                                st.error("Please enter a new feature name!")
                                # Before creating new feature, check for duplicates
                                if new_feature_name in df.columns:
                                    st.error(f"Column '{new_feature_name}' already exists!")
                                    return

                            elif not feature_expression:
                                st.error("Please enter an expression for the new feature!")
                            else:
                                try:
                                    # Execute import statements
                                    exec_globals = {}
                                    exec(import_text, exec_globals)


                                    # Use the utility function
                                    safe_env = feu.create_safe_environment(df, exec_globals)

                                    
                                    # # Create safe evaluation environment
                                    # safe_env = {
                                    #     'np': exec_globals.get('np', np),
                                    #     'pd': exec_globals.get('pd', pd),
                                    #     'math': exec_globals.get('math', math),
                                    #     'abs': abs, 'round': round, 'min': min, 'max': max,
                                    #     'sum': sum, 'len': len,
                                    #     'pi': math.pi, 'e': math.e,
                                    #     'inf': float('inf'), 'nan': float('nan'),
                                    #     'None': None, 'True': True, 'False': False,
                                    #     'df': df,
                                    # }
                                    
                                    # Add all column data to the environment
                                    for col in df.columns:
                                        safe_env[col] = df[col]
                                    
                                    # Evaluate the expression
                                    result = eval(feature_expression, {"__builtins__": {}}, safe_env)
                                    
                                    # Handle the result
                                    if isinstance(result, pd.Series):
                                        new_series = result
                                    else:
                                        # Try to apply expression row by row
                                        new_series = df.apply(
                                            lambda row: eval(feature_expression, {"__builtins__": {}}, {**safe_env, **row.to_dict()}),
                                            axis=1
                                        )
                                    
                                    # Determine insert position and create new columns list
                                    if position_option == "At specific index":
                                        insert_index = min(position_index, len(df.columns))
                                        new_columns = list(df.columns)
                                        new_columns.insert(insert_index, new_feature_name)
                                    elif position_option == "Before/After column" and reference_col in df.columns:
                                        ref_idx = list(df.columns).index(reference_col)
                                        insert_index = ref_idx if relative_position == "Before" else ref_idx + 1
                                        new_columns = list(df.columns)
                                        new_columns.insert(insert_index, new_feature_name)
                                    else:
                                        # At end
                                        insert_index = len(df.columns)
                                        new_columns = list(df.columns) + [new_feature_name]
                                    
                                    # Create new dataframe with column in correct position
                                    df[new_feature_name] = new_series
                                    
                                    # Reorder columns
                                    df = df[new_columns]
                                    
                                    # Update session state
                                    st.session_state.df = df
                                    
                                    # Show success with position info
                                    st.success(f"✅ New feature '{new_feature_name}' is created successfully! Preview dataframe below")
                                    
                                except Exception as e:
                                    st.error(f"❌ Error creating feature: {str(e)}")
                                    st.info("Check your expression syntax and make sure column names are correct.")

                    st.markdown('---')
                    st.markdown("#### Current Dataframe")
                    st.dataframe(st.session_state.df.head())


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
                    st.markdown(":green[The purpose is to view intervals in a DataFrame,] for instance viewing rows between index 5 and 15, as well as between index 45 and 60.", unsafe_allow_html=True)
                    st.dataframe(st.session_state.df.head())
                    
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


                    st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True) # space
                    
                    st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)     
                            

                if selected_preprocess == "Subset Dataframe":

                    if 'df_subset' not in st.session_state:
                        st.session_state.df_subset = None

                    # Option 1: Select specific columns by name
                    with st.expander("Select Columns by Name"):
                        selected_columns = st.multiselect(
                            "Choose columns to include:",
                            options=df.columns.tolist(),
                            default=df.columns.tolist()[:3] if len(df.columns) > 3 else df.columns.tolist()
                        )
                        
                        if selected_columns:
                            df_subset_name = df[selected_columns]
                            st.write(f"Selected {len(selected_columns)} columns")
                            st.dataframe(df_subset_name.head())
                            
                            if st.button("Save This Subset (by name)", key="sdf_1"):
                                st.session_state.df_subset = df_subset_name
                                st.session_state.df = df_subset_name
                                st.success(f"Subset saved! Shape: {df_subset_name.shape}")

                    # Option 2: Select by column indexes
                    with st.expander("Select by Column Indexes"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            start_idx = st.number_input(
                                "Start column index:",
                                min_value=0,
                                max_value=len(df.columns)-1,
                                value=0,
                                step=1
                            )
                        
                        with col2:
                            end_idx = st.number_input(
                                "End column index:",
                                min_value=0,
                                max_value=len(df.columns)-1,
                                value=min(5, len(df.columns)-1),
                                step=1
                            )
                        
                        if start_idx <= end_idx:
                            df_subset_idx = df.iloc[:, start_idx:end_idx+1]
                            st.write(f"Columns {start_idx} to {end_idx} (inclusive)")
                            st.write(f"Selected columns: {df_subset_idx.columns.tolist()}")
                            st.dataframe(df_subset_idx.head())
                            
                            if st.button("Save This Subset (by index)", key="sidf_1"):
                                st.session_state.df_subset = df_subset_idx
                                st.session_state.df = df_subset_idx
                                st.success(f"Subset saved! Shape: {df_subset_idx.shape}")

                    # Option 3: Select columns to the right/left of a specific column
                    with st.expander("Select Relative to Column"):
                        reference_col = st.selectbox(
                            "Choose reference column:",
                            options=df.columns.tolist()
                        )
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            direction = st.radio(
                                "Direction:",
                                ["Left of column", "Right of column", "Both sides"]
                            )
                        
                        with col2:
                            include_ref = st.checkbox("Include reference column", value=True)
                        
                        ref_idx = df.columns.get_loc(reference_col)
                        
                        if direction == "Left of column":
                            relative_selected = df.iloc[:, :ref_idx+1] if include_ref else df.iloc[:, :ref_idx]
                        elif direction == "Right of column":
                            relative_selected = df.iloc[:, ref_idx:] if include_ref else df.iloc[:, ref_idx+1:]
                        else:  # Both sides
                            relative_selected = df
                        
                        st.write(f"Selected columns: {relative_selected.columns.tolist()}")
                        st.dataframe(relative_selected.head())

                        if st.button("Save This Subset (by relative column)", key="srcdf_1"):
                            st.session_state.df_subset = relative_selected
                            st.session_state.df = relative_selected
                            st.success(f"Subset saved! Shape: {relative_selected.shape}")


                    # Option 4: Simple range selection
                    with st.expander("Quick Range Selection"):
                        st.write(f"Total columns: {len(df.columns)}")
                        
                        # Use multiselect with checkboxes style
                        selected_by_checkbox = st.multiselect(
                            "Select columns:",
                            options=df.columns.tolist(),
                            default=df.columns.tolist(),
                            format_func=lambda x: f"{df.columns.get_loc(x)}: {x}"
                        )
                        
                        if selected_by_checkbox:
                            df_checkbox = df[selected_by_checkbox]
                            st.write(f"Selected {len(selected_by_checkbox)} columns")
                            st.dataframe(df_checkbox.head())

                            if st.button("Save This Subset (by range)", key="srdf_1"):
                                st.session_state.df_subset = df_checkbox
                                st.session_state.df = df_checkbox
                                st.success(f"Subset saved! Shape: {df_checkbox.shape}")



                    # Display final subset if saved
                    if 'df_subset' in st.session_state:
                        st.divider()
                        st.subheader("Saved Subset")
                        st.write(f"Shape: {st.session_state.df_subset.shape}")
                        st.dataframe(st.session_state.df_subset)
                        
                        # Download option
                        csv = st.session_state.df_subset.to_csv(index=False)
                        st.download_button(
                            label="Download Subset as CSV",
                            data=csv,
                            file_name="Final Data Subset.csv",
                            mime="text/csv"
                        )



                if selected_preprocess == "Delete Records":
                    st.dataframe(st.session_state.df.head())

                    # Allow user to input indices to delete
                    indices_to_delete = st.text_input("Enter indices to delete (comma-separated):")


                    if indices_to_delete:
                        try:
                            # Convert input string to a list of integers
                            indices = [int(idx.strip()) for idx in indices_to_delete.split(',')]

                            if st.button("Confirm Delete", key="confirm_delete_button"):

 
                            
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




                if selected_preprocess == "Delete Columns":
                    st.dataframe(st.session_state.df.head())
                    
                    #df = st.session_state.df
                    current_columns = list(df.columns)
                    st.write("Current Columns:")
                    st.write(list(df.columns))
                    
                    # Input for columns to delete
                    delete_input = st.text_input(
                        "Enter column names or indices (comma-separated):",
                        placeholder="e.g., 'Column1, Column2' or '0, 2'"
                    )
                    
                    
                    # Store original for undo
                    if 'original_df_columns' not in st.session_state:
                        st.session_state.original_df_columns = df.copy()
                    
                    if delete_input:
                        try:
                            to_delete = []
                            invalid_entries = []
                            
                            for entry in delete_input.split(','):
                                entry = entry.strip()
                                
                                if entry.isdigit():  # Index
                                    idx = int(entry)
                                    if 0 <= idx < len(current_columns):
                                        to_delete.append(current_columns[idx])
                                    else:
                                        invalid_entries.append(f"Index {idx}")
                                else:  # Column name
                                    if entry in current_columns:
                                        to_delete.append(entry)
                                    else:
                                        invalid_entries.append(f"'{entry}'")
                            
                            if invalid_entries:
                                st.warning(f"Invalid entries: {', '.join(invalid_entries)}")
                            
                            if to_delete:
                                st.write(f"Will delete: {', '.join(to_delete)}")
                                
                                # Buttons in columns
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    if st.button("Delete Columns"):
                                        # Save for undo
                                        st.session_state.df_before_delete = df.copy()
                                        st.session_state.deleted_cols = to_delete
                                        
                                        # Delete columns
                                        df = df.drop(columns=to_delete)
                                        st.session_state.df = df
                                        
                                        st.success(f"Deleted {len(to_delete)} column(s): {', '.join(to_delete)}")
                                st.dataframe(df.head())
                                
                                with col2:
                                    # Undo button
                                    if 'df_before_delete' in st.session_state:
                                        if st.button("Undo Delete"):
                                            st.session_state.df = st.session_state.df_before_delete.copy()
                                            st.success("Undid column deletion")
                                            st.dataframe(st.session_state.df.head())
                                
                                with col3:
                                    # Revert all button
                                    if st.button("Revert All"):
                                        st.session_state.df = st.session_state.original_df_columns.copy()
                                        st.success("Reverted to original")
                                        st.dataframe(st.session_state.df.head())
                        
                        except Exception as e:
                            st.error(f"Error: {str(e)}")



                if selected_preprocess == "Edit Values":
                    st.dataframe(st.session_state.df.head())
                    
                    #df = st.session_state.df.copy()
                    
                    st.write("###### Edit/Replace Values in Data")

                    # Global replacement
                    with st.expander("Match pattern to replace values", expanded=False):
                        st.write("Replace values matching patterns (contains, starts with, ends with)")
                        
                        pattern_type = st.selectbox(
                            "Pattern type:",
                            options=["Contains", "Starts with", "Ends with", "Regex pattern"],
                            key="pattern_type"
                        )
                        
                        pattern_columns = st.multiselect(
                            "Columns to search:",
                            options=st.session_state.df.columns.tolist(),
                            key="pattern_columns"
                        )
                        
                        if not pattern_columns:
                            pattern_columns = st.session_state.df.columns.tolist()
                        
                        col_pattern, col_pattern_replace = st.columns(2)
                        with col_pattern:
                            pattern = st.text_input(
                                f"Pattern to find ({pattern_type}):",
                                key="pattern_input"
                            )
                        
                        with col_pattern_replace:
                            pattern_new = st.text_input(
                                "Replace with:",
                                placeholder="Leave empty for NaN",
                                key="pattern_new_value"
                            )
                        
                        if st.button("Apply Pattern Replace", key="apply_pattern_replace"):
                            if pattern:
                                try:
                                    # Store original for undo
                                    if 'df_before_pattern' not in st.session_state:
                                        st.session_state.df_before_pattern = st.session_state.df.copy()
                                    
                                    cells_modified = 0
                                    
                                    for col in pattern_columns:
                                        if pd.api.types.is_string_dtype(st.session_state.df[col]) or pd.api.types.is_object_dtype(st.session_state.df[col]):
                                            for idx in st.session_state.df.index:
                                                current_val = st.session_state.df.at[idx, col]
                                                if pd.notna(current_val):
                                                    str_val = str(current_val)
                                                    match = False
                                                    
                                                    if pattern_type == "Contains":
                                                        match = pattern in str_val
                                                    elif pattern_type == "Starts with":
                                                        match = str_val.startswith(pattern)
                                                    elif pattern_type == "Ends with":
                                                        match = str_val.endswith(pattern)
                                                    elif pattern_type == "Regex pattern":
                                                        import re
                                                        match = bool(re.search(pattern, str_val))
                                                    
                                                    if match:
                                                        if pattern_new == '':
                                                            df.at[idx, col] = np.nan
                                                        else:
                                                            # Replace the pattern in the string
                                                            if pattern_type == "Contains":
                                                                new_val = str_val.replace(pattern, pattern_new)
                                                            elif pattern_type == "Starts with":
                                                                new_val = pattern_new + str_val[len(pattern):]
                                                            elif pattern_type == "Ends with":
                                                                new_val = str_val[:-len(pattern)] + pattern_new
                                                            elif pattern_type == "Regex pattern":
                                                                import re
                                                                new_val = re.sub(pattern, pattern_new, str_val)
                                                            
                                                            df.at[idx, col] = new_val
                                                        cells_modified += 1
                                    
                                    #st.session_state.df = df
                                    st.success(f"Pattern replacement modified {cells_modified} cell(s)")
                                    st.dataframe(st.session_state.df.head())
                                    
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    # # Undo functionality
                    # st.write("---")
                    # st.write("### Undo Operations")
                    
                    # undo_col1, undo_col2, undo_col3 = st.columns(3)
                    
                    # with undo_col1:
                    #     if 'df_before_edit' in st.session_state:
                    #         if st.button("Undo Last Edit", key="undo_last_edit"):
                    #             st.session_state.df = st.session_state.df_before_edit.copy()
                    #             st.success("Undid last edit operation")
                    #             st.dataframe(st.session_state.df.head())
                    #             del st.session_state.df_before_edit
                    
                    # with undo_col2:
                    #     if 'df_before_global' in st.session_state:
                    #         if st.button("Undo Global Replace", key="undo_global"):
                    #             st.session_state.df = st.session_state.df_before_global.copy()
                    #             st.success("Undid global replacement")
                    #             st.dataframe(st.session_state.df.head())
                    #             del st.session_state.df_before_global
                    
                    # with undo_col3:
                    #     if st.button("Reset All Changes", key="reset_all_changes"):
                    #         # Reset to original
                    #         st.session_state.df = st.session_state.original_df.copy() if 'original_df' in st.session_state else df.copy()
                    #         st.success("Reset all changes to original")
                    #         st.dataframe(st.session_state.df.head())
                    #         # Clear all undo history
                    #         for key in ['df_before_edit', 'df_before_global', 'df_before_pattern']:
                    #             if key in st.session_state:
                    #                 del st.session_state[key]
                    
                    # # Download updated data
                    # st.write("---")
                    # st.write("### Download Updated Data")
                    
                    # updated_csv = st.session_state.df.to_csv(index=False).encode('utf-8')
                    # st.download_button(
                    #     "Download Edited Data",
                    #     updated_csv,
                    #     "edited_data.csv",
                    #     "text/csv",
                    #     key="download_edited"
                    # )



            except Exception as e:
                st.error(f"Error occurred while generating the chart: {str(e)}")

            st.markdown("<div style='margin-top:50px'></div>", unsafe_allow_html=True) # space
                    
            st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)  
               
                
        return df


        #st.markdown("<div style='margin-top:100px'></div>", unsafe_allow_html=True) # space
                    

        #st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)    

    def feature_engineering(self):
        if 'df' in st.session_state:
            updated_df = st.session_state.df.copy()
            # Feature engineering logic
            #st.write(updated_df.head())
            return updated_df
        else:
            st.error("DataFrame not available.")
            return None
        
     # using the create_csv_agent of Langstane LLM, converting df to csv into memory
    def detailed_dataframe_summary(self, df):
            st.sidebar.write("Feature Engineering is selected")


            if df not in st.session_state:
                st.session_state.df = df


            df = self.feature_engineering()
            
            # Load the data
            if df is not None:
                try:
                    updated_df = df.copy()

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
    #safe evaluation of feature engineering expressions

    def evaluate_feature_expr(self, feature_expr, df, pipeline_util=None):
        """
        Safe wrapper to evaluate a feature expression using PipelineManager
        """

        # apply 
        result_series = feu.safe_evaluate(feature_expr, df)

        return result_series