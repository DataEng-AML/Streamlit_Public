
# import numpy as np
# import pandas as pd
# import os
# import re
# import copy
# from collections import Counter
# import functools
# from tqdm import tqdm
# from IPython.display import display, HTML, clear_output
# import inflection
# from langchain_community.llms import OpenAI
# from dotenv import load_dotenv
# import matplotlib.pyplot as plt
# import ipywidgets as widgets
# import inspect
# from sklearn.metrics import accuracy_score

# #####
# from typing import Any, Dict, Optional
# from pydantic import BaseModel, Field, PrivateAttr
# from crewai import Agent, Crew
# from crewai.task import Task
# from crewai.tasks.task_output import TaskOutput
# from typing import ClassVar, Optional
# #####


# #####       BaseModel

# # For Data Read
# class DataFrameOutput(BaseModel):
#     data: pd.DataFrame

#     class Config:
#         arbitrary_types_allowed = True


# # For API 
# class APIOutput(BaseModel):
#     client: Optional[Any] = None
#     class Config:
#         arbitrary_types_allowed = True

#     def get_client_repr(self) -> str:
#         if self.client is not None:
#             client_str = repr(self.client)
#             return client_str.replace('APIOutput(client=', 'OpenAI', 1).strip()
#         return 'No client available'


# # For Standardized Data 
# class StandardizedDataFrameOutput(BaseModel):
#     standardized_df: pd.DataFrame

#     def __init__(self):
#         super().__init__(
#             role="Data Fetcher", 
#             goal="Fetch and read data of different types", 
#             verbose=True, 
#             backstory="Fetching data from file for standardization"
#         )
    
#     class Config:
#         arbitrary_types_allowed = True


# #############################  APIAgent

# class APIAgent(Agent):
#     _openai_llm: Optional[APIOutput] = None
#     _existing_shortcodes: set = PrivateAttr(default_factory=set)

#     def __init__(self):
#         super().__init__(
#             role="API call", 
#             goal="Make an API call and cleanse response", 
#             verbose=True, 
#             backstory="Getting a clean response from a prompt, output to be standardized"
#       )

#         _openai_llm = APIAgent._openai_llm 
#         APIAgent._existing_shortcodes = set()

#         class Config:
#             arbitrary_types_allowed = True
    
#     def load_openai_api_key(self):
#         load_dotenv()
#         api_key = os.getenv('OPENAI_API_KEY')
#         if not api_key:
#             raise ValueError("API key for OpenAI is required")

#         # Initialize the OpenAI client if not already done
#         if self._openai_llm  is None:
#             openai_client = OpenAI(api_key=api_key)
#             self._openai_llm  = openai_client

#         return self._openai_llm 

# #############################  CleanAPIAgent

# class CleanAPIAgent(APIAgent):
#     def __init__(self):
#         super().__init__()
#         self._response_cache = {}

#     @functools.lru_cache(maxsize=None)
#     def _cached_openai_call(self, prompt):
#         return self._openai_llm.invoke(input=prompt)

#     def get_openai_corrections(self, terms, _existing_shortcodes, prompt_template: str, progress_bar=None, num_responses=20):
#         if self._openai_llm is None:
#             self.load_openai_api_key()
        
#         all_existing_shortcodes = self._existing_shortcodes.union(_existing_shortcodes)
#         openai_corrections = {}

#         for term in terms:
#             prompt = prompt_template.format(term=term)
            
#             if prompt in self._response_cache:
#                 openai_corrections[term] = self._response_cache[prompt]
#             else:
#                 responses = []
#                 for _ in range(num_responses):
#                     response = self._cached_openai_call(prompt)
#                     cleaned_response = self.clean_response(response)
#                     if cleaned_response not in all_existing_shortcodes:
#                         responses.append(cleaned_response)

#                 if responses:
#                     mode_response = Counter(responses).most_common(1)[0][0]
#                     openai_corrections[term] = mode_response
#                     self._existing_shortcodes.add(mode_response)
#                     self._response_cache[prompt] = mode_response

#             if progress_bar:
#                 progress_bar.set_postfix_str(f"Processing = {term}")
#                 progress_bar.update(1)

#         return openai_corrections

#     def clean_response(self, response):
#         return response.strip().replace('\n', ' ').replace('"', '').replace("'", '').replace("Standardization", '').replace("standardized term: ",'').replace("The standard name for the ",'').replace("The standard name for ",'').replace(".    ",'').replace(" -> ",'').replace(".  ",'').strip()



# class Crew:
#     def __init__(self, agents=None, tasks=None, verbose=False, planning=True):
#         self.agents = agents or []
#         self.tasks = tasks or []
#         self.verbose = verbose
#         self.planning = planning

#     def run_task(self, task, *args, **kwargs):
#         if self.verbose:
#             print(f"Running task: {task.description}")
        
#         agent = task.agent
#         method = getattr(agent, task.method_name)
        
#         if callable(method):
#             output = method(*args, **kwargs)
#             return output
#         else:
#             raise AttributeError(f"The method {task.method_name} is not callable on {agent}")


# class Task:
#     def __init__(self, description, expected_output, agent, output_pydantic, method_name):
#         self.description = description
#         self.expected_output = expected_output
#         self.agent = agent
#         self.output_pydantic = output_pydantic
#         self.method_name = method_name  # Add method_name to the task attributes


# load_openai_api_key_task = Task(
#     description="Load the api key and return openai from Langstane LLM",
#     expected_output="openai string",
#     agent=APIAgent(),
#     output_pydantic=APIOutput,
#     method_name='load_openai_api_key'
# )

# get_openai_corrections_task = Task(
#     description="Get corrected API response for the term using relevant prompt",
#     expected_output="openai string",
#     agent=CleanAPIAgent(),
#     output_pydantic=APIOutput,
#     method_name='get_openai_corrections'
# )



# # Create the crew and plan for task execution
# crew = Crew(
#     agents=[APIAgent()],
#     tasks=[load_openai_api_key_task, get_openai_corrections_task],
#     verbose=True,
#     planning=False
# )


################## Start new code
import numpy as np
import pandas as pd
import os
import re
import copy
from collections import Counter
import functools
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

##### CrewAI imports
from typing import Any, Dict, Optional, List, Union
from pydantic import BaseModel, Field
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from crewai.task import TaskOutput
#####

##### Pydantic Models
class DataFrameOutput(BaseModel):
    data: pd.DataFrame = Field(..., description="The pandas DataFrame output")
    
    class Config:
        arbitrary_types_allowed = True

class APIOutput(BaseModel):
    client: Optional[Any] = Field(None, description="OpenAI client instance")
    api_key_loaded: bool = Field(False, description="Whether API key was successfully loaded")
    
    class Config:
        arbitrary_types_allowed = True

    def get_client_repr(self) -> str:
        if self.client is not None:
            return f"OpenAI(client={repr(self.client)})"
        return 'No client available'

class StandardizedDataFrameOutput(BaseModel):
    standardized_df: pd.DataFrame = Field(..., description="Standardized pandas DataFrame")
    
    class Config:
        arbitrary_types_allowed = True

class TermCorrectionsOutput(BaseModel):
    corrections: Dict[str, str] = Field(..., description="Dictionary of original terms to standardized corrections")
    prompt_template: str = Field(..., description="The prompt template used for standardization")
    
    class Config:
        arbitrary_types_allowed = True

class ProcessedTermsOutput(BaseModel):
    terms: List[str] = Field(..., description="List of unique terms extracted for processing")
    source_column: str = Field(..., description="The DataFrame column the terms were extracted from")
    
    class Config:
        arbitrary_types_allowed = True
#####

############################# APIAgent (Fixed with real implementation)
class APIAgent(Agent):
    def __init__(self):
        super().__init__(
            role="API Manager",
            goal="Manage OpenAI API connections and ensure proper authentication",
            backstory="Expert in handling API connections, authentication, and managing API resources efficiently",
            verbose=True,
            allow_delegation=False
        )
        self._openai_client = None

    def load_openai_api_key(self) -> APIOutput:
        """Load and initialize OpenAI API client"""
        load_dotenv()
        api_key = os.getenv('OPENAI_API_KEY')
        
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        try:
            self._openai_client = OpenAI(api_key=api_key, temperature=0.7)
            # Test the connection with a simple call
            test_response = self._openai_client.invoke("Say 'connected'")
            print(f"API Connection Test: {test_response.strip()}")
            
            return APIOutput(client=self._openai_client, api_key_loaded=True)
            
        except Exception as e:
            raise ValueError(f"Failed to initialize OpenAI client: {str(e)}")

############################# CleanAPIAgent (Fixed with real API integration)
class CleanAPIAgent(Agent):
    def __init__(self):
        super().__init__(
            role="API Response Cleaner",
            goal="Clean and standardize API responses from OpenAI",
            backstory="Specialist in processing and cleaning API responses, ensuring consistent and standardized output format",
            verbose=True,
            allow_delegation=False
        )
        self._response_cache = {}
        self._existing_shortcodes = set()

    def clean_response(self, response: str) -> str:
        """Clean and standardize API response"""
        cleaning_patterns = [
            r'(?i)standardization',
            r'(?i)standardized term:\s*',
            r'(?i)the standard name for (?:the\s*)?',
            r'\.\s+',
            r'->',
            r'[\"\']',
            r'\(.*?\)',
            r'\[.*?\]'
        ]
        
        cleaned = response.strip()
        for pattern in cleaning_patterns:
            cleaned = re.sub(pattern, '', cleaned)
        
        # Remove extra spaces and normalize
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned

    def get_openai_corrections(self, terms: List[str], prompt_template: str, 
                             api_client: Any, num_responses: int = 5) -> TermCorrectionsOutput:
        """Get corrected API responses for multiple terms using actual API client"""
        if api_client is None:
            raise ValueError("OpenAI client must be provided")
        
        openai_corrections = {}
        
        print(f"Processing {len(terms)} terms with OpenAI API...")
        
        for term in tqdm(terms, desc="Standardizing terms"):
            prompt = prompt_template.format(term=term)
            
            if prompt in self._response_cache:
                openai_corrections[term] = self._response_cache[prompt]
                continue
            
            responses = []
            try:
                for _ in range(num_responses):
                    response = api_client.invoke(prompt)
                    cleaned_response = self.clean_response(response)
                    
                    if (cleaned_response and 
                        cleaned_response not in self._existing_shortcodes and
                        cleaned_response != term):  # Avoid returning the same term
                        responses.append(cleaned_response)
                
                if responses:
                    # Use the most common response
                    counter = Counter(responses)
                    mode_response = counter.most_common(1)[0][0]
                    
                    openai_corrections[term] = mode_response
                    self._existing_shortcodes.add(mode_response)
                    self._response_cache[prompt] = mode_response
                else:
                    # Fallback: use original term if no good responses
                    openai_corrections[term] = term
                    
            except Exception as e:
                print(f"Error processing term '{term}': {str(e)}")
                openai_corrections[term] = term  # Fallback to original
        
        return TermCorrectionsOutput(
            corrections=openai_corrections,
            prompt_template=prompt_template
        )

############################# DataProcessingAgent
class DataProcessingAgent(Agent):
    def __init__(self):
        super().__init__(
            role="Data Processor",
            goal="Process and prepare data for API standardization",
            backstory="Expert in data preprocessing, term extraction, and preparing data for API processing",
            verbose=True,
            allow_delegation=False
        )

    def extract_terms_from_dataframe(self, df: pd.DataFrame, column_name: str = 'terms') -> ProcessedTermsOutput:
        """Extract unique terms from DataFrame column for processing"""
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in DataFrame")
        
        terms = df[column_name].dropna().astype(str).unique().tolist()
        terms = [term.strip() for term in terms if term.strip()]
        
        print(f"Extracted {len(terms)} unique terms from column '{column_name}'")
        
        return ProcessedTermsOutput(terms=terms, source_column=column_name)

    def apply_corrections_to_dataframe(self, df: pd.DataFrame, corrections: Dict[str, str], 
                                     source_column: str, target_column: str = 'standardized') -> StandardizedDataFrameOutput:
        """Apply standardized corrections back to the DataFrame"""
        df_copy = df.copy()
        
        # Create mapping function
        def standardize_term(term):
            if pd.isna(term):
                return term
            term_str = str(term).strip()
            return corrections.get(term_str, term_str)
        
        # Apply corrections
        df_copy[target_column] = df_copy[source_column].apply(standardize_term)
        
        print(f"Applied corrections to {len(df_copy)} rows")
        print(f"Sample corrections: {list(corrections.items())[:5]}")
        
        return StandardizedDataFrameOutput(standardized_df=df_copy)

############################# Crew Tasks with proper context passing
def create_data_standardization_crew(input_df: pd.DataFrame, 
                                   source_column: str = 'terms',
                                   prompt_template: str = "Standardize the following term for consistency: {term}",
                                   num_responses: int = 3) -> Crew:
    """Create a complete data standardization crew"""
    
    # Initialize agents
    api_agent = APIAgent()
    clean_api_agent = CleanAPIAgent()
    data_processor = DataProcessingAgent()
    
    # Task 1: Load API Key
    load_api_task = Task(
        description="Load OpenAI API key and initialize the client for data standardization",
        expected_output="Initialized OpenAI client ready for API calls with authentication confirmed",
        agent=api_agent,
        output_pydantic=APIOutput,
        context=[]
    )
    
    # Task 2: Extract Terms from DataFrame
    extract_terms_task = Task(
        description=f"Extract unique terms from '{source_column}' column for standardization",
        expected_output=f"List of unique terms from {source_column} column ready for API processing",
        agent=data_processor,
        output_pydantic=ProcessedTermsOutput,
        context=[],
        # Pass input data through function call
        callback=lambda: data_processor.extract_terms_from_dataframe(input_df, source_column)
    )
    
    # Task 3: Get API Corrections (now receives API client from previous task)
    corrections_task = Task(
        description="Get standardized corrections for terms using OpenAI API with proper cleaning",
        expected_output="Dictionary mapping original terms to their standardized versions",
        agent=clean_api_agent,
        output_pydantic=TermCorrectionsOutput,
        context=[load_api_task, extract_terms_task],
        # Use outputs from previous tasks
        callback=lambda: clean_api_agent.get_openai_corrections(
            terms=extract_terms_task.output.terms,
            prompt_template=prompt_template,
            api_client=load_api_task.output.client,
            num_responses=num_responses
        )
    )
    
    # Task 4: Apply Corrections to DataFrame
    apply_corrections_task = Task(
        description="Apply the standardized corrections back to the original DataFrame",
        expected_output="DataFrame with standardized terms applied to the target column",
        agent=data_processor,
        output_pydantic=StandardizedDataFrameOutput,
        context=[extract_terms_task, corrections_task],
        # Use outputs from previous tasks
        callback=lambda: data_processor.apply_corrections_to_dataframe(
            df=input_df,
            corrections=corrections_task.output.corrections,
            source_column=extract_terms_task.output.source_column,
            target_column=f"standardized_{source_column}"
        )
    )
    
    # Create and return crew
    crew = Crew(
        agents=[api_agent, clean_api_agent, data_processor],
        tasks=[load_api_task, extract_terms_task, corrections_task, apply_corrections_task],
        verbose=True,
        process=Process.sequential
    )
    
    return crew
