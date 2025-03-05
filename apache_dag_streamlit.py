import os
import streamlit as st
#import airflow
from airflow.models import DagBag, DagRun
from airflow.api.client.local_client import Client
from airflow.utils.state import State
from airflow.utils.dot_renderer import render_dag
import graphviz
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
from airflow.utils.timezone import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import subprocess
from airflow.models import TaskInstance
from airflow.utils.db import provide_session

import requests
from requests.auth import HTTPBasicAuth
import streamlit as st
import graphviz
from datetime import datetime
import time


####################

import base64
#import requests
#import pandas as pd
#import streamlit as st

####################


# Auto-refresh every 5 seconds

# Airflow REST API details
# Set up your Airflow Web server details
#AIRFLOW_URL = 'http://localhost:8080/api/v1/dags'  # Adjust URL if necessary

# Locally run Airflow
AIRFLOW_BASE_URL = 'http://localhost:8080'


# Provide Airflow credentials for the REST API
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "qwerty"

def get_task_logs(dag_id, task_id, run_id, try_number=1):
    """
    Fetch logs for a specific task instance using Airflow's REST API.
    """
    url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
    
    try:
        response = requests.get(url, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        response.raise_for_status()
        logs = response.json().get("content", "No logs found.")
        return logs
    except requests.exceptions.RequestException as e:
        st.error(f"Error retrieving task logs: {e}")
        return None
# Define status colors
STATUS_COLORS = {
    "success": "#28a745",   # Green
    "failed": "#dc3545",    # Red
    "running": "#007bff",   # Blue
    "queued": "#fd7e14",    # Orange
    "skipped": "#6c757d"     # Gray
}

# --- Utility Functions ---
def clear_cache():
    """Clear Streamlit's cache."""
    for key in st.session_state.keys():
        del st.session_state[key]
    st.cache_resource.clear()  # Clear resource cache
    st.rerun()

# --- Airflow Connection Setup ---
def connect_to_airflow():
    """Initialize and return an Airflow API client."""
    try:
        # Local client requires an api_base_url when using Airflow 2+
        # Adjust the api_base_url to match your Airflow environment
        client = Client(api_base_url="http://localhost:8080")  # Replace with your Airflow API URL
        return client
    except Exception as e:
        st.error(f"Failed to connect to Airflow: {e}")
        return None

# --- DAG Information ---
def get_dag_info(dag_id):
    """Retrieve a specific DAG from Airflow."""
    dag_bag = DagBag()  # Load DAGs
    dag = dag_bag.get_dag(dag_id)

    if dag is None:
        st.error(f"DAG '{dag_id}' not found!")
    return dag

def get_all_dags():
    """Load all DAGs from Airflow."""
    dag_bag = DagBag()
    return dag_bag.dags

def filter_dags(dags, search_term):
    """Filter DAGs based on a search term."""
    if not search_term:
        return dags  # Return all DAGs if no search term
    return {dag_id: dag for dag_id, dag in dags.items() if search_term.lower() in dag_id.lower()}

# --- DAG Rendering ---
#def render_dag_graph(dag):
#    """Generate a graphviz representation of the DAG."""
 #   try:
 #       dot = render_dag(dag)
        # Check if dot is a string before calling endswith
  #      if isinstance(dot, str):
 #           if not dot.endswith('\n'):
 #               dot += '\n'  # Ensure the dot string ends with a newline
            #return graphviz.Source(dot)
 #   except Exception as e:
 #       st.error(f"Error rendering DAG graph: {e}")
 #       return None

# Function to Render DAG Graph
def render_dag_graph(dag, dag_runs):
    graph = graphviz.Digraph('DAG')

    # Adding nodes with dynamic color updates
    for task in dag.tasks:
        latest_status = "queued"  # Default status
        for run in dag_runs:
            task_status = get_task_status(dag.dag_id, run.run_id, task.task_id)
            if task_status:
                latest_status = task_status.lower()
                break

        color = STATUS_COLORS.get(latest_status, "#000000")  # Default black if unknown

        graph.node(
            task.task_id,
            label=f"{task.task_id}\n({latest_status.capitalize()})",
            style="filled",
            fillcolor=color,
            fontcolor="white"
        )

    # Adding dependencies
    for task in dag.tasks:
        for downstream_task in task.downstream_list:
            graph.edge(task.task_id, downstream_task.task_id)

    return graph






def pause_dag(dag_id):
    """Pause a DAG in Airflow using CLI."""
    try:
        result = subprocess.run(['airflow', 'dags', 'pause', dag_id], 
                                capture_output=True, text=True, check=True)
        st.success(f"Successfully paused DAG '{dag_id}'.")
    except subprocess.CalledProcessError as e:
        st.error(f"Error pausing DAG: {e.stderr}")
        

def unpause_dag(dag_id):
    """Unpause a DAG in Airflow using CLI."""
    try:
        result = subprocess.run(['airflow', 'dags', 'unpause', dag_id], 
                                capture_output=True, text=True, check=True)
        st.success(f"Successfully unpaused DAG '{dag_id}'.")
    except subprocess.CalledProcessError as e:
        st.error(f"Error unpausing DAG: {e.stderr}")
        
        

# --- DAG Run Operations ---
def trigger_dag_run(dag_id, conf=None):
    """Trigger a new DAG run in Airflow."""
    try:
        client = connect_to_airflow()
        if client:
            client.trigger_dag(dag_id, conf=conf)  # Use the simpler trigger_dag function
            st.success(f"Successfully triggered DAG run for '{dag_id}'.")
        else:
            st.error(f"Failed to trigger DAG '{dag_id}'.")
    except Exception as e:
        st.error(f"Error triggering DAG run: {e}")


def get_dag_run_history(dag_id, num_runs=10):
    """Fetch the recent DAG run history from Airflow."""
    try:
        dag_runs = DagRun.find(dag_id=dag_id)  # Query the Airflow database directly
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)  # Sort by execution date
        return dag_runs[:num_runs]  # Return the most recent runs
    except Exception as e:
        st.error(f"Error fetching DAG run history: {e}")
        return []

# --- Task Logs ---
def get_task_logs(dag_id, task_id, try_number=1):
    """Retrieve logs for a specific task instance."""
    try:
        client = connect_to_airflow()
        if client:
            logs = client.get_task_instance_logs(
                dag_id=dag_id,
                #dag_run_id=dag_run_id,
                task_id=task_id,
                try_number=try_number,
                full_content=True
            )
            return logs.content
        else:
            return "Unable to retrieve logs: Airflow connection failed."
    except Exception as e:
        return f"Error retrieving task logs: {e}"


def display_task_status(run):
    color = STATUS_COLORS.get(run.state, "#000")
    st.markdown(f"<span style='color:{color}; font-weight:bold;'>Run ID: {run.dag_id}, State: {run.state.upper()}, Execution Date: {run.execution_date}</span>", unsafe_allow_html=True)


@provide_session
def get_task_status(dag_id, execution_date, task_id, session=None):
    """Fetch the status of a specific task in a DAG run."""
    try:
        task_instance = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == task_id,
                TaskInstance.execution_date == execution_date  # Ensure datetime here
            )
            .first()
        )
        return task_instance.state if task_instance else None
    except Exception as e:
        st.error(f"Error fetching task status for {task_id}: {e}")
        return None


# --- Main Streamlit App ---

def main():
    """Main Streamlit application."""
    st.title('Apache Airflow DAG Viewer')
    
    
    ####################### For Apache Airflow configuration and settings ########################
    
    # 1: Configure the Airflow home directory:
    os.environ['AIRFLOW_HOME'] = '/home/tony/airflow'
        
    # 2: Load the DAGs into a DagBag object
    dagbag = DagBag()
        
    #####################################################################################
    
        # 3: Fetch data from Airflow: for example DAGs
    dags = dagbag.dags
        
    try:
        for dag_id, dag in dags.items():
            print(f"DAG ID: {dag_id}, DAG Name: {dag.dag_id}")
        # Process the dags data
    except Exception as e:
        st.error(f"Failed to connect to Airflow API: {str(e)}")   

    st.write("List of the DAGs in my Apache Airflow")
    st.write(dags)
         
    
    ######################################################################################
    """Main Streamlit application."""
    st.title('Apache Airflow DAG Viewer')

    # Session State Initialization (Caching variables to avoid re-execution)
    if 'dags' not in st.session_state:
        st.session_state.dags = get_all_dags()

    # Sidebar for DAG Filtering
    st.sidebar.header("DAG Filters")
    #search_term = st.sidebar.text_input("Search DAGs by ID:")
    #filtered_dags = filter_dags(st.session_state.dags, search_term)
    search_term = st.sidebar.text_input("Search DAGs by ID:", key="search_term")
    filtered_dags = filter_dags(st.session_state.dags, search_term)
    dag_ids = list(filtered_dags.keys())

    # DAG Selection
    #selected_dag_id = st.sidebar.selectbox('Select a DAG', dag_ids)
    selected_dag_id = st.sidebar.selectbox('Select a DAG', dag_ids, key="selected_dag_id")

    if selected_dag_id:
        dag = get_dag_info(selected_dag_id)
        st.header(f"DAG: {dag.dag_id}")
        st.subheader('DAG Details')
        st.write(f"Description: {dag.description or 'No description'}")
        st.write(f"Schedule Interval: {dag.schedule_interval or 'Manual'}")

        st.subheader('DAG Run History')
        dag_runs = get_dag_run_history(dag.dag_id)
        if not dag:
            return # Prevents errors if there is no DAG

        # Load data from the uploaded CSV file (Move this here)
        uploaded_file = st.file_uploader("Upload CSV file", type=["csv"], key="file_uploader")
        if uploaded_file is not None:
            try:
                df = pd.read_csv(uploaded_file)
                st.session_state.df = df # Store DataFrame in session state
                st.write(df.head())
                
                # Convert the DataFrame to CSV and encode it to Base64
                csv_data = df.to_csv(index=False)
                encoded_csv = base64.b64encode(csv_data.encode('utf-8')).decode('utf-8')
            except Exception as e:
                st.error(f"Error reading CSV file: {e}")
                df = None
        elif 'df' in st.session_state:
            df = st.session_state.df
            st.write(df.head())
            
            # Convert the DataFrame to CSV and encode it to Base64
            csv_data = df.to_csv(index=False)
            encoded_csv = base64.b64encode(csv_data.encode('utf-8')).decode('utf-8')

            # Create a dropdown or radio buttons for method selection
            process_option = st.radio(
                "Select the process you want to apply to the data:",
                ("Handle Missing Data", "Remove Duplicates", "Custom Process")
            )

            # Button to trigger Airflow DAG based on selected method
            if st.button('Process Data'):
                # Set your own Airflow base URL and DAG ID
                AIRFLOW_BASE_URL = "http://localhost:8080"  # Replace with your Airflow base URL
                DAG_ID = "missing_data_pipeline"  # Replace with your DAG ID
                
                # API endpoint to trigger the DAG run
                url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dag_runs"

                # Prepare the payload with the encoded CSV data and selected method
                payload = {
                    "conf": {
                        "csv_data": encoded_csv,  # Base64 encoded data
                        "process_option": process_option  # The selected method
                    }
                }

                # Trigger the DAG run via POST request
                response = requests.post(
                    url,
                    json=payload,
                    headers={'Authorization': 'Bearer YOUR_AIRFLOW_API_KEY'}  # Add API token if required
                )

                # Check the response status
                if response.status_code == 200:
                    run_id = response.json()['dag_run_id']  # Get the run_id from the response
                    st.success(f"Airflow DAG triggered successfully! Run ID: {run_id}")
                    st.write(f"Waiting for Airflow to process the file...")
                else:
                    st.error(f"Failed to trigger Airflow DAG: {response.status_code}")

        else:
            st.warning("Please upload a CSV file.")
            df = None


    ############################################



        
        
    ############################################



        # --- DAG Details ---
        st.header(f"DAG: {dag.dag_id}")
        st.subheader('DAG Details')
        st.write(f"Description: {dag.description or 'No description'}")
        st.write(f"Schedule Interval: {dag.schedule_interval or 'Manual'}")

        # --- DAG Run History ---
        st.subheader('DAG Run History')
        dag_runs = get_dag_run_history(dag.dag_id)

        if dag_runs:
            for run in dag_runs:
                st.write(f"Run ID: {run.dag_id}, State: {run.state}, Execution Date: {run.execution_date}")
                display_task_status(run)
        else:
            st.write("No DAG runs found.")

        # --- DAG Control ---
        st.subheader("DAG Control")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button('Trigger DAG Run', key="trigger"):
                trigger_dag_run(dag.dag_id)
        with col2:
            if st.button('Pause DAG', key="pause"):
                pause_dag(dag.dag_id)
        with col3:
            if st.button('Unpause DAG', key="unpause"):
                unpause_dag(dag.dag_id)


        st.subheader("Simple Graphviz Test")
        try:
            simple_graph = graphviz.Digraph('test')
            simple_graph.node('A', 'Node A')
            simple_graph.node('B', 'Node B')
            simple_graph.edge('A', 'B')
            st.graphviz_chart(simple_graph)
        except Exception as e:
            st.error(f"Graphviz test failed: {e}")


        # --- DAG Graph ---


        st.subheader('DAG Graph')
        dag_runs = get_dag_run_history(dag.dag_id)  # Fetch latest runs
        graph = render_dag_graph(dag, dag_runs)     # Pass both dag and dag_runs
        if graph:
            st.graphviz_chart(graph)


        # --- Task Logs ---
        st.subheader('Task Logs')
        dag_run_ids = [run.dag_id for run in dag_runs] if dag_runs else []
        selected_dag_run_id = st.selectbox("Select a DAG Run ID for logs:", dag_run_ids, key = "selected_dag_run_id")

        if selected_dag_run_id:
            tasks = [task.task_id for task in dag.tasks]
            selected_task_id = st.selectbox("Select a Task ID for logs:", tasks, key ="selected_task_id")

            if selected_task_id:
                task_logs = get_task_logs(dag.dag_id, selected_dag_run_id, selected_task_id)
                st.code(task_logs, language='text')

if __name__ == "__main__":
    main()
