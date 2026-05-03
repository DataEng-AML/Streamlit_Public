import os
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from IPython import display
import numpy as np
# streamlit extras
from streamlit_extras.dataframe_explorer import dataframe_explorer


# Class to handle exploratory data analysis operations
class perform_eda:
    def __init__(self, df):
        self.original_data = df


    def user_chats(self):
        st.write("Explore with LLM:")

        user_question = st.text_input("Data Quality of your CSV: ")

        if user_question is not None and user_question != "":
            with st.spinner(text="In progress..."):              
                response = self.run(user_question)
                st.write(response)
                return response

    def head_data(self):
        st.write("Head of the data:")
        head_df = self.original_data.head()
        st.write(head_df)
        return head_df

    def tail_data(self):
        st.write("Tail of the data:")
        tail_df = self.original_data.tail()
        st.write(tail_df)
        return tail_df
