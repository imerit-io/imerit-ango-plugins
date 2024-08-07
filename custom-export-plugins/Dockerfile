# Use the base Python 3.8 image
FROM python:3.8.13-slim-buster

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Create and activate a virtual environment
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code to the container
COPY app.py .

# Copy the scripts directory to the container
COPY scripts /app/scripts

# Copy the folder into the image
COPY jj_support /app/jj_support

# When plugin run with rest connector default port is 8080
EXPOSE 8080

# Set the command to run the Python script
CMD ["python", "app.py"]

#### OLD BELOW ####

# # Use the base Python 3.8 image
# FROM python:3.8.13-slim-buster

# # Set the working directory inside the container
# WORKDIR /app

# # Copy the requirements.txt file to the container
# COPY requirements.txt .

# # Copy the .env file
# # COPY .env .

# # Create and activate a virtual environment
# RUN python -m venv /venv
# ENV PATH="/venv/bin:$PATH"

# # Install the Python dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy the application code to the container
# COPY app.py .

# # Copy the scripts directory to the container
# COPY scripts /app/scripts

# # When plugin run with rest connector default port is 8080
# EXPOSE 8080

# # Set the command to run the Python script
# CMD ["python", "app.py"]


# # # Use the base Python 3.8 image
# # FROM python:3.8.13-slim-buster

# # # Set the working directory inside the container
# # WORKDIR /app

# # # Copy the requirements.txt file to the container
# # COPY requirements.txt .

# # # Copy the .env file
# # # COPY .env .

# # # Create and activate a virtual environment
# # RUN python -m venv /venv
# # ENV PATH="/venv/bin:$PATH"

# # # Install the Python dependencies
# # RUN pip install --no-cache-dir -r requirements.txt

# # # Copy the application code to the container
# # COPY app.py .

# # # Create a directory to hold the scripts
# # RUN mkdir /app/scripts

# # # When plugin run with rest connector default port is 8080
# # EXPOSE 8080

# # # Set the command to run the Python script
# # CMD ["python", "app.py"]


# # # Use the base Python 3.8 image
# # FROM python:3.8.10

# # # Set the working directory inside the container
# # WORKDIR /app

# # # Set the scripts directory path
# # ENV SCRIPTS_DIR=/app/scripts

# # # Copy the requirements.txt file to the container
# # COPY requirements.txt .

# # # Copy the .env file
# # COPY .env .

# # # Create and activate a virtual environment
# # RUN python -m venv /venv
# # ENV PATH="/venv/bin:$PATH"

# # # Install the Python dependencies
# # RUN pip install --no-cache-dir -r requirements.txt

# # # Copy the application code to the container
# # COPY app.py .

# # # Create a directory to hold the scripts
# # RUN mkdir /app/scripts

# # # Set the command to run the Python script
# # CMD ["python", "app.py"]
