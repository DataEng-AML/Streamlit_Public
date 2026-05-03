"""
Streamlit wrapper for CrewAI Data Explorer
Simple integration wrapper
"""

import streamlit as st
import pandas as pd
from crewai_data_exploration_agent import create_data_chat, CrewAIDataExplorer

class WrapperCrewAIExplorer:
    """Wrapper to integrate CrewAI into Streamlit"""
    
    def __init__(self):
        self.setup_session_state()
    
    def setup_session_state(self):
        """Initialize session state variables"""
        if 'crewai_initialized' not in st.session_state:
            st.session_state.crewai_initialized = False
        if 'crewai_instance' not in st.session_state:
            st.session_state.crewai_instance = None
        if 'crewai_chat_history' not in st.session_state:
            st.session_state.crewai_chat_history = []
        if 'crewai_model' not in st.session_state:
            st.session_state.crewai_model = "llama2"
    
    def initialize_crewai(self, df: pd.DataFrame):
        """Initialize CrewAI with the dataframe"""
        try:
            st.session_state.crewai_instance = create_data_chat(df, st.session_state.crewai_model)
            st.session_state.crewai_initialized = True
            st.session_state.crewai_chat_history = []
            return True
        except Exception as e:
            st.error(f"Failed to initialize CrewAI: {str(e)}")
            return False
    
    def render_chat_interface(self, df: pd.DataFrame):
        """Render the chat interface"""
        
        # Model selection in sidebar
        st.session_state.crewai_model = st.sidebar.selectbox(
            "Ollama Model",
            ["llama2", "mistral", "neural-chat"],
            index=0,
            key="crewai_model_select"
        )
        
        # Initialize button
        if st.sidebar.button("🚀 Initialize CrewAI", key="init_crewai_btn"):
            with st.spinner("Setting up AI agents..."):
                if self.initialize_crewai(df):
                    st.sidebar.success("CrewAI ready!")
                    st.rerun()
        
        # If not initialized, show message
        if not st.session_state.crewai_initialized:
            st.info("""
            **To use CrewAI Data Chat:**
            1. Select Ollama model in sidebar
            2. Click 'Initialize CrewAI'
            3. Start asking questions below
            """)
            return
        
        # Chat interface
        st.markdown("### 💬 Chat with Your Data")
        
        # Display chat history
        chat_container = st.container(height=300)
        with chat_container:
            for msg in st.session_state.crewai_chat_history:
                if msg["role"] == "user":
                    st.markdown(f"**👤 You:** {msg['content']}")
                    st.markdown("---")
                else:
                    st.markdown(f"**🤖 CrewAI:** {msg['content']}")
                    st.markdown("---")
        
        # Chat input
        col1, col2 = st.columns([4, 1])
        with col1:
            user_input = st.text_input(
                "Ask anything about your data:",
                placeholder="e.g., Tell me about this dataset, analyze column X, find insights...",
                key="crewai_chat_input",
                label_visibility="collapsed"
            )
        
        with col2:
            send_button = st.button("Ask", type="primary", width='stretch')
        
        # Process user input
        if send_button and user_input:
            # Add user message to history
            st.session_state.crewai_chat_history.append({
                "role": "user",
                "content": user_input
            })
            
            # Get AI response
            with st.spinner("🤖 AI agents are analyzing..."):
                response = st.session_state.crewai_instance.chat(user_input)
                
                # Add AI response to history
                st.session_state.crewai_chat_history.append({
                    "role": "assistant",
                    "content": response
                })
            
            st.rerun()
        
        # Quick questions
        st.markdown("### 💡 Try Asking:")
        quick_cols = st.columns(3)
        quick_questions = [
            "Tell me about this dataset",
            "Find interesting insights",
            "Analyze data quality",
            "What columns are available?",
            "Are there missing values?",
            "Suggest visualizations"
        ]
        
        for i, question in enumerate(quick_questions):
            with quick_cols[i % 3]:
                if st.button(question, key=f"quick_{i}", width='stretch'):
                    st.session_state.crewai_chat_history.append({
                        "role": "user",
                        "content": question
                    })
                    st.rerun()
        
        # Clear chat button
        if st.button("🗑️ Clear Chat", key="clear_chat"):
            st.session_state.crewai_chat_history = []
            st.rerun()
    
    def get_simple_chat_input(self):
        """Simple text input for CrewAI (for minimal integration)"""
        user_input = st.text_area(
            "Ask CrewAI about your data:",
            placeholder="Type your question here...",
            height=100
        )
        
        if st.button("Get Analysis", key="crewai_analyze"):
            if user_input and st.session_state.crewai_instance:
                with st.spinner("Analyzing..."):
                    response = st.session_state.crewai_instance.chat(user_input)
                    st.write("**CrewAI Analysis:**")
                    st.write(response)
            else:
                st.warning("Please initialize CrewAI first and enter a question")
        
        return user_input