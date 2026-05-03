
import numpy as np
import pandas as pd
import os
import re
import copy
from collections import Counter
from tqdm import tqdm
from IPython.display import display, HTML, clear_output
import inflection
#from langchain_community.llms import OpenAI
from langchain_openai import OpenAI
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import ipywidgets as widgets
import inspect
from sklearn.metrics import accuracy_score

#####
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, PrivateAttr
from crewai import Agent, Crew
from crewai.task import Task
from crewai.tasks.task_output import TaskOutput
from typing import ClassVar, Optional
#####


####################################################################################


#####       BaseModel

# For Data Read
class DataFrameOutput(BaseModel):
    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


# For API 
class APIOutput(BaseModel):
    client: Optional[Any] = None
    class Config:
        arbitrary_types_allowed = True

    def get_client_repr(self) -> str:
        if self.client is not None:
            client_str = repr(self.client)
            return client_str.replace('APIOutput(client=', 'OpenAI', 1).strip()
        return 'No client available'


# For Standardized Data 
class StandardizedDataFrameOutput(BaseModel):
    standardized_df: pd.DataFrame

    def __init__(self):
        super().__init__(
            role="Data Fetcher", 
            goal="Fetch and read data of different types", 
            verbose=True, 
            backstory="Fetching data from file for standardization"
        )
    
    class Config:
        arbitrary_types_allowed = True


'''
Agents

DataReadAgent
APIAgent
AIStandardizationAgent
HumanStandardizationAgent
DisplayAgent
    -- Tab Displays
    -- Plot Displays

    
class CorrectAPIOutput(BaseModel):
    openai_correction: dict

    class Config:
        arbitrary_types_allowed = True
'''

#############################  APIAgent

class APIAgent(Agent):
    _openai_llm: Optional[APIOutput] = None
    _existing_shortcodes: set = PrivateAttr(default_factory=set)

    def __init__(self):
        super().__init__(
            role="API call", 
            goal="Make an API call and cleanse response", 
            verbose=True, 
            backstory="Getting a clean response from a prompt, output to be standardized"
      )

        _openai_llm = APIAgent._openai_llm 
        APIAgent._existing_shortcodes = set()

        class Config:
            arbitrary_types_allowed = True
    
    def load_openai_api_key(self):
        load_dotenv()
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("API key for OpenAI is required")

        # Initialize the OpenAI client if not already done
        if self._openai_llm  is None:
            openai_client = OpenAI(api_key=api_key)
            self._openai_llm  = openai_client

        return self._openai_llm 



class Crew:
    def __init__(self, agents=None, tasks=None, verbose=False, planning=True):
        self.agents = agents or []
        self.tasks = tasks or []
        self.verbose = verbose
        self.planning = planning

    def run_task(self, task, *args, **kwargs):
        if self.verbose:
            print(f"Running task: {task.description}")
        
        agent = task.agent
        method = getattr(agent, task.method_name)
        
        if callable(method):
            output = method(*args, **kwargs)
            return output
        else:
            raise AttributeError(f"The method {task.method_name} is not callable on {agent}")


class Task:
    def __init__(self, description, expected_output, agent, output_pydantic, method_name):
        self.description = description
        self.expected_output = expected_output
        self.agent = agent
        self.output_pydantic = output_pydantic
        self.method_name = method_name  # Add method_name to the task attributes


# Create a sequence of tasks that the crew will execute.

load_openai_api_key_task = Task(
    description="Load the api key and return openai from Langstane LLM",
    expected_output="openai string",
    agent=APIAgent(),
    output_pydantic=APIOutput,
    method_name='load_openai_api_key'
)


# Create the crew and plan for task execution
crew = Crew(
    agents=[APIAgent()],
    tasks=[load_openai_api_key_task],
    verbose=True,
    planning=False
)


#• Standardization of the feature names
#• Standardization of the data
#• Missing values
#• Duplication of the data
#• Anomalies
#• Primary keys for joining tables.
# Use this one