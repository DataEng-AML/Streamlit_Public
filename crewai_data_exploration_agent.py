"""
CrewAI Data Explorer - Conversational data analysis with multiple AI agents
"""

import pandas as pd
import numpy as np
from typing import ClassVar, Dict, List, Optional, Any, Type
import json
from datetime import datetime

# CrewAI imports
from crewai import Agent, Task, Crew, Process
from crewai.tools import BaseTool
from langchain.tools import tool, StructuredTool
from langchain_community.llms import Ollama
from pydantic import BaseModel, Field



class GetDatasetSummary(BaseModel):
    """Input schema for get_dataset_summary tool"""
    dataset_path: str = Field(..., description="Path to the dataset file")
    include_stats: bool = Field(
        default=True, 
        description="Whether to include statistical information"
    )
    sample_size: int = Field(
        default=1000, 
        description="Number of rows to sample for analysis"
    )


class DataExplorationTools(BaseTool):
    """Tools for CrewAI agents to explore datasets"""
    
    name: str = "Data Exploration Tools"
    description: str = "Tools for exploring and analyzing datasets"
    
    
    @classmethod
    def get_dataset_summary_impl(
        cls, 
        dataset_path: str, 
        include_stats: bool = True, 
        sample_size: int = 1000
         ) -> str:
            basic_info = {
            "rows": len(cls.df),
            "columns": len(cls.df.columns),
            "shape": cls.df.shape,
            "memory_mb": f"{cls.df.memory_usage(deep=True).sum() / 1024 / 1024:.2f}"
            },
            columns = {
            "all": list(cls.df.columns),
            "numeric": list(cls.df.select_dtypes(include=[np.number]).columns),
            "categorical": list(cls.df.select_dtypes(include=['object', 'category']).columns)
            },
            data_quality = {
            "missing_total": int(cls.df.isnull().sum().sum()),
            "missing_percentage": f"{(cls.df.isnull().sum().sum() / (len(cls.df) * len(cls.df.columns)) * 100):.2f}%",
            "duplicates": int(cls.df.duplicated().sum())
            },
            sample_data = cls.df.head(3).to_dict('records')


            summary = {
            "dataset_path": dataset_path,
            "basic_info": basic_info,
            "columns": columns,
            "data_quality": data_quality,
            "sample_data": sample_data
            }
        

            return f"Summary for {summary}"
    
    @tool
    def analyze_column(self, column_name: str) -> str:
        """Deep analysis of a specific column"""
        if column_name not in self.df.columns:
            return f"Column '{column_name}' not found"
        
        col = self.df[column_name]
        result = {
            "column": column_name,
            "dtype": str(col.dtype),
            "unique_values": int(col.nunique()),
            "missing": int(col.isnull().sum()),
            "missing_percentage": f"{(col.isnull().sum() / len(col) * 100):.2f}%"
        }
        
        if pd.api.types.is_numeric_dtype(col):
            result.update({
                "min": float(col.min()),
                "max": float(col.max()),
                "mean": float(col.mean()),
                "median": float(col.median()),
                "std": float(col.std())
            })
        else:
            top_values = col.value_counts().head(5).to_dict()
            result["top_values"] = top_values
        
        return json.dumps(result, indent=2)
    
    @tool
    def find_insights(self) -> str:
        """Find interesting insights in the data"""
        insights = []
        
        # Check for correlations
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 1:
            corr_matrix = self.df[numeric_cols].corr().abs()
            strong_corr = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    if corr_matrix.iloc[i, j] > 0.7:
                        strong_corr.append(f"{corr_matrix.columns[i]} ↔ {corr_matrix.columns[j]}: {corr_matrix.iloc[i, j]:.2f}")
            
            if strong_corr:
                insights.append("**Strong Correlations Found:**")
                insights.extend(strong_corr[:3])
        
        # Check data quality issues
        high_missing = [col for col in self.df.columns if self.df[col].isnull().mean() > 0.3]
        if high_missing:
            insights.append(f"**High Missing Values (>30%):** {high_missing}")
        
        # Find binary columns
        binary_cols = [col for col in self.df.columns if self.df[col].nunique() == 2]
        if binary_cols:
            insights.append(f"**Binary Columns:** {binary_cols}")
        
        return "\n".join(insights) if insights else "No major insights found"
    
    @tool
    def answer_question(self, question: str) -> str:
        """Answer specific questions about the data"""
        question_lower = question.lower()
        
        if "missing" in question_lower or "null" in question_lower:
            missing_sum = self.df.isnull().sum().sum()
            missing_pct = (missing_sum / (len(self.df) * len(self.df.columns))) * 100
            return f"Total missing values: {missing_sum} ({missing_pct:.2f}% of all data)"
        
        elif "duplicate" in question_lower:
            duplicates = self.df.duplicated().sum()
            return f"Duplicate rows: {duplicates}"
        
        elif "column" in question_lower and "how many" in question_lower:
            return f"Total columns: {len(self.df.columns)}"
        
        elif "row" in question_lower and "how many" in question_lower:
            return f"Total rows: {len(self.df)}"
        
        return f"I can answer questions about missing values, duplicates, columns, and rows. Try asking about those."


    # Mark as ClassVar to tell Pydantic this is not a model field
    get_dataset_summary: ClassVar[StructuredTool] = StructuredTool(
            name='get_dataset_summary',
            description='Get comprehensive summary of the dataset',
            args_schema=GetDatasetSummary,  # Now defined above
            func=get_dataset_summary_impl
        )







class CrewAIDataExplorer:
    """Main CrewAI orchestrator for data exploration"""
    
    def __init__(self, df: pd.DataFrame, model_name: str = "llama2"):
        self.df = df
        self.model_name = model_name
        self.llm = Ollama(model=model_name, temperature=0.1)
        self.tools = DataExplorationTools(df)
        self.crew = self._create_crew()
    
    def _create_crew(self):
        """Create CrewAI agents and tasks"""
        
        # Create agents
        analyst = Agent(
            role="Data Analyst",
            goal="Help users understand their data through conversation",
            backstory="You're a friendly data analyst who explains data concepts in simple terms.",
            tools=[self.tools.get_dataset_summary, self.tools.analyze_column],
            llm=self.llm,
            verbose=False
        )
        
        insights_agent = Agent(
            role="Insights Specialist",
            goal="Find patterns, correlations, and insights in data",
            backstory="You have a keen eye for spotting interesting patterns in datasets.",
            tools=[self.tools.find_insights],
            llm=self.llm,
            verbose=False
        )
        
        qa_agent = Agent(
            role="Q&A Specialist",
            goal="Answer specific questions about the dataset",
            backstory="You're excellent at answering direct questions about data.",
            tools=[self.tools.answer_question],
            llm=self.llm,
            verbose=False
        )
        
        # Create tasks
        conversation_task = Task(
            description="Have a helpful conversation about the dataset. Be friendly and informative.",
            agent=analyst,
            expected_output="A conversational response about the dataset"
        )
        
        insights_task = Task(
            description="Find interesting insights, patterns, and correlations in the data.",
            agent=insights_agent,
            expected_output="Key insights found in the data"
        )
        
        qa_task = Task(
            description="Answer specific questions the user has about the data.",
            agent=qa_agent,
            expected_output="Direct answer to the user's question"
        )
        
        # Create crew
        crew = Crew(
            agents=[analyst, insights_agent, qa_agent],
            tasks=[conversation_task, insights_task, qa_task],
            process=Process.sequential,
            verbose=False
        )
        
        return crew
    
    def chat(self, user_message: str) -> str:
        """Main method to get response from CrewAI"""
        try:
            result = self.crew.kickoff(inputs={"user_message": user_message})
            return str(result)
        except Exception as e:
            return f"Error: {str(e)}. Please try a different question."

# Simple wrapper for easy integration
def create_data_chat(df: pd.DataFrame, model: str = "llama2"):
    """Create a CrewAI data chat instance"""
    return CrewAIDataExplorer(df, model)