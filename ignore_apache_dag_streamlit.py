import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO

# Function to display basic data overview
def data_overview(data):
    st.subheader("Data Overview")
    st.write("Head of dataset:")
    st.write(data.head())  # Corrected: Call data.head() as a function
    st.write("Tail of dataset:")
    st.write(data.tail())  # Corrected: Call data.tail() as a function
    st.write("General Info of dataset:")
    
    # Capturing the output of data.info() to display it properly
    buffer = StringIO()
    data.info(buf=buffer)
    s = buffer.getvalue()
    st.text(s)  # Display info about the dataframe

    st.write("Columns in the dataset:")
    st.write(data.columns)

# Data cleaning function
def data_cleaning(data):
    st.subheader("Data Cleaning")
    # Convert 'Price per Unit' to float after removing '$'
    data['Price per Unit'] = data['Price per Unit'].str.replace('$', "").astype(float)
    st.write("After cleaning 'Price per Unit':")
    st.write(data.head(1)) 
    
    # Convert 'Units Sold', 'Total Sales', 'Operating Profit' to integers after removing commas and '$'
    data['Units Sold'] = data['Units Sold'].str.replace(',', "").astype(int)
    data['Total Sales'] = data['Total Sales'].str.replace('$', "").str.replace(',', "").astype(int)
    data['Operating Profit'] = data['Operating Profit'].str.replace('$', "").str.replace(',', "").astype(int)
    st.write("After cleaning 'Units Sold', 'Total Sales', 'Operating Profit':")
    st.write(data.head(1)) 
    
    # Rename columns to include "$"
    data.rename(columns={"Price per Unit": "Price per Unit($)"}, inplace=True)
    data.rename(columns={"Total Sales": "Total Sales($)"}, inplace=True)
    data.rename(columns={"Operating Profit": "Operating Profit($)"}, inplace=True)
    
    # Convert 'Invoice Date' to datetime and set it as index
    data['Invoice Date'] = pd.to_datetime(data['Invoice Date'])
    data = data.set_index(['Invoice Date'])  
    
    st.write("Data after cleaning and setting 'Invoice Date' as index:")
    st.write(data.head(5))
    
    return data  # Return cleaned data for further use

# Function to plot total sales by region
def plot_total_sales_by_region(data):
    # Clean the data
    cleaned_data = data_cleaning(data)
    
    # Group by region and sum the total sales
    data_to_plot = cleaned_data.groupby(['Region'])['Total Sales($)'].sum().sort_values(ascending=True)

    # Create the bar plot
    plt.figure(figsize=(4, 2))
    data_to_plot.plot(kind='bar', color='blue')

    plt.title('Total Sales by Region')
    plt.xlabel('Region')
    plt.ylabel('Total Sales in Millions ($)')
    
    # Display the plot in Streamlit
    st.pyplot(plt)

# Function to display summary statistics
def summary_statistics(data):
    st.subheader("Summary Statistics")
    st.write(data.describe())

# Function to display dataset info (e.g., column types, non-null counts)
def summary_info(data):
    st.subheader("Summary Information")
    buffer = StringIO()
    data.info(buf=buffer)
    s = buffer.getvalue()
    st.text(s)  # Display info about the dataframe

# Function to check missing data
def missing_data(data):
    st.subheader("Missing Data")
    missing_values = data.isnull().sum()
    st.write(missing_values)

# Streamlit App Layout
def main():
    st.title("Exploratory Data Analysis (EDA)")

    # File upload
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        data = pd.read_csv(uploaded_file)

        selected_operations = st.multiselect(
            "Data Exploration:", 
            ["Data Overview", "Data Cleaning", "Data Information", "Generate Plot", "Missing Data"]
        )

        for operation in selected_operations:
            if operation == "Data Overview": 
                data_overview(data)
            elif operation == "Data Cleaning": 
                data_cleaning(data)  # Perform cleaning and show results
            elif operation == "Data Information": 
                summary_info(data)
            elif operation == "Missing Data":
                missing_data(data)
            elif operation == "Generate Plot":
                plot_total_sales_by_region(data)  # Generate plot for total sales by region

# Run the app
if __name__ == '__main__':
    main()
