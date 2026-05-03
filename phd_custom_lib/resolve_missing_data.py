import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from IPython import display
import numpy as np
# streamlit extras
from streamlit_extras.dataframe_explorer import dataframe_explorer


# Class to handle resolution of missing data operations
class resolve_missing_data:
    def accuracy_missing_data(self, df):
        print("Missing Data Accuracy:")
        value_count = df.count().sum()
        element_count = df.size
        ratio_missing_data = (element_count - value_count) / element_count
        accuracy_missing_data = 1 - ratio_missing_data
        print(accuracy_missing_data)
        return accuracy_missing_data

    def predict_missing_data(self, df, column):
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

    def fix_missing_data(self, df, process='replace'):
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
