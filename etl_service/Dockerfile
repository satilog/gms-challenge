# Use Python base image
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Copy the entire etl_service folder, including requirements.txt
COPY ./etl_service /app/etl_service

# Copy the db folder for the SQL initialization script
COPY ./db /app/db

# Copy the data folder for the source files
COPY ./data /app/data

# Install system dependencies required by psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev gcc

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /app/etl_service/requirements.txt

# Set PYTHONPATH to include /app
ENV PYTHONPATH=/app

# Set the command to execute the main script
CMD ["python", "/app/etl_service/src/main.py"]