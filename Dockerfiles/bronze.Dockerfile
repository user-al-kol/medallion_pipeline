# bronze-container
FROM jupyter/pyspark-notebook:latest

USER root

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip 

# Install sqlite
RUN apt-get update && apt-get install -y \
    sqlite3 \
    libsqlite3-dev  

# Install jdbc
RUN wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar \
    -P /usr/local/spark/jars/


# Create working directory
WORKDIR /app
# Copy scripts
COPY src/bronze /app/bronze_code
COPY src/common /app/bronze_code/common

# Command 
CMD ["python","bronze_code/main.py"]