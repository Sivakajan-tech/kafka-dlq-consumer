# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

RUN pip install setuptools

# Copy and Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the python script into the container
COPY consumer.py .

# Run the script when the container starts
CMD ["python", "consumer.py"]
