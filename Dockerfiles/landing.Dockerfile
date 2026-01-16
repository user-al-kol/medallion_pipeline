# landing-container
FROM jupyter/pyspark-notebook:latest

USER root

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip 

# Create working directory
WORKDIR /app

# COPY files
COPY src/landing /app/landing_code
COPY src/common /app/landing_code/common

# Command
CMD ["python","landing_code/main.py"]
