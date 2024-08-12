# Start from a base image
FROM python:3.9-slim-buster

# Specify the directory for the app
WORKDIR /app

# Install pipenv
RUN pip install pipenv

# Copy Pipfile and Pipfile.lock
COPY Pipfile Pipfile.lock ./

# Install dependencies
RUN pipenv install --system --deploy

# Copy the rest of the application code
COPY . .

# Expose the port that the app runs on
EXPOSE 8000

# Define the command to start the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
