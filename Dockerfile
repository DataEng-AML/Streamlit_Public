# 1. Use an official, stable Python base image
FROM python:3.10-slim

# 2. Set environment variables to prevent Python from writing .pyc files 
# and to ensure logs are sent straight to the terminal
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 3. Install system dependencies (including GDAL for geospatial libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. Create a non-root user for security (Hugging Face preference)
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

# 5. Copy ALL requirement files PLUS the new constraints file
COPY --chown=user requirements.txt requirements_base.txt requirements_crewai.txt constraints.txt ./

# 6. Use the constraints file to force pip to behave
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --upgrade -c constraints.txt -r requirements.txt
    

# 7. Copy the rest of your Streamlit application
COPY --chown=user . .

# 8. Expose the port Hugging Face expects
EXPOSE 7860

# 9. Launch Streamlit
# We use 0.0.0.0 and port 7860 to ensure it connects to the HF Space
#CMD ["streamlit", "run", "test101.py", "--server.port", "7860", "--server.address", "0.0.0.0"]
CMD ["streamlit", "run", "test101.py", "--server.port=7860", "--server.address=0.0.0.0", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
