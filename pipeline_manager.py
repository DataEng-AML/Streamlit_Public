# pipeline_manager.py
import streamlit as st
import pandas as pd
from pathlib import Path
import json
from datetime import datetime, timezone
import os
import re

class PersistentPipeline:
    """Manages persistent pipeline state with Parquet files"""
    
    def __init__(self, pipeline_dir='pipeline_state'):
        self.pipeline_dir = Path(pipeline_dir)
        self.pipeline_dir.mkdir(exist_ok=True)
        self.metadata_file = self.pipeline_dir / 'metadata.json'
        self._load_metadata()
    
    def _load_metadata(self):
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {
                'steps': {}, 
                'current_data': None,
                'pipeline_name': 'Data Pipeline',
                'created_at': datetime.now(timezone.utc).astimezone().isoformat() #datetime.now().isoformat()
            }
    
    def _save_metadata(self):
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
    
    def save_data(self, df, step_name, agent_used=None):
        """Save data for a specific step"""
        import re
        import pandas as pd

        # Check input dataframe
        if df is None:
            raise ValueError("No dataframe provided to save_data")

        # Create a clean filename from step name
        filename = re.sub(r'[^a-z0-9_]+', '_', step_name.lower())
        file_path = self.pipeline_dir / f'{filename}.parquet'

        # Make a copy to avoid modifying the original
        dftosave = df.copy()
        df_to_save = dftosave.convert_dtypes()  # Now always assigned

        for col in df_to_save.columns:
            if df_to_save[col].dtype == "object" or str(df_to_save[col].dtype) == "string":
                series = df_to_save[col]

                # Try numeric
                numeric_coerced = pd.to_numeric(series, errors='coerce')
                if numeric_coerced.notna().mean() > 0.9:
                    df_to_save[col] = numeric_coerced
                    continue

                # Try datetime
                datetime_coerced = pd.to_datetime(series, errors='coerce')
                if datetime_coerced.notna().mean() > 0.9:
                    df_to_save[col] = datetime_coerced
                    continue

                # Otherwise keep as string
                df_to_save[col] = series.astype("string")

        # Save the dataframe outside the loop
        df_to_save.to_parquet(file_path, index=False, engine='pyarrow', compression='snappy')

        
        # Store metadata
        self.metadata['steps'][step_name] = {
            'file': str(file_path),
            'timestamp': datetime.now().isoformat(),
            'shape': [df.shape[0], df.shape[1]],
            'columns': list(df.columns),
            'agent_used': agent_used,
            'step_order': len(self.metadata['steps'])
        }
        self.metadata['current_data'] = step_name
        self._save_metadata()
        
        return step_name
    
    def load_data(self, step_name=None):
        """Load data from a specific step or latest"""
        if step_name is None:
            step_name = self.metadata.get('current_data')
        
        if step_name and step_name in self.metadata['steps']:
            file_path = self.metadata['steps'][step_name]['file']
            if Path(file_path).exists():
                return pd.read_parquet(file_path)
        return None
    
    def load_step_data(self, step_name):
        """Load data from a specific step by name"""
        return self.load_data(step_name)
    
    def list_steps(self):
        """Show all completed steps in order"""
        steps = self.metadata.get('steps', {})
        # Sort by step_order
        sorted_steps = sorted(steps.items(), key=lambda x: x[1].get('step_order', 0))
        return dict(sorted_steps)
    
    def get_step_info(self, step_name):
        """Get metadata for a specific step"""
        return self.metadata['steps'].get(step_name)
    
    def reset_to_step(self, step_name):
        """Rollback to a previous step"""
        if step_name in self.metadata['steps']:
            self.metadata['current_data'] = step_name
            
            # Delete all steps after this one
            steps_to_delete = []
            found = False
            for s in sorted(self.metadata['steps'].keys()):
                if s == step_name:
                    found = True
                elif found:
                    steps_to_delete.append(s)
            
            for s in steps_to_delete:
                # Delete the parquet file
                file_path = self.metadata['steps'][s].get('file')
                if file_path and Path(file_path).exists():
                    Path(file_path).unlink()
                # Remove from metadata
                del self.metadata['steps'][s]
            
            self._save_metadata()
            return True
        return False
    
    def clear_all(self):
        """Clear all pipeline state"""
        # Delete all parquet files
        for step_name, info in self.metadata['steps'].items():
            file_path = info.get('file')
            if file_path and Path(file_path).exists():
                Path(file_path).unlink()
        
        # Reset metadata
        self.metadata = {
            'steps': {}, 
            'current_data': None,
            'pipeline_name': 'Data Pipeline',
            'created_at': datetime.now().isoformat()
        }
        self._save_metadata()
    
    def export_current_data(self, format='csv'):
        """Export current data to CSV or Excel"""
        current_df = self.load_data()
        if current_df is not None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if format == 'csv':
                filename = f"exported_data_{timestamp}.csv"
                current_df.to_csv(filename, index=False)
            elif format == 'excel':
                filename = f"exported_data_{timestamp}.xlsx"
                current_df.to_excel(filename, index=False)
            return filename
        return None
    
    def get_pipeline_summary(self):
        """Get a summary of the pipeline"""
        steps = self.list_steps()
        current = self.load_data()
        
        return {
            'total_steps': len(steps),
            'completed_steps': list(steps.keys()),
            'current_step': self.metadata.get('current_data'),
            'current_shape': current.shape if current is not None else None,
            'current_columns': list(current.columns) if current is not None else None
        }

# Initialize pipeline in session state
def init_pipeline():
    if 'pipeline' not in st.session_state:
        st.session_state.pipeline = PersistentPipeline()
    return st.session_state.pipeline