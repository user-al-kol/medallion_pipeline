# raw-container
FROM python:slim

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip

# Create work directory
WORKDIR /app
COPY src/raw /app/raw_code
COPY src/common /app/raw_code/common
# Run the main script
CMD ["python", "raw_code/main.py"]
