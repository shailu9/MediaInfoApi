# Use an official Python base image
FROM python:3.11-slim

# Install ffmpeg and other system dependencies
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure /tmp is writable for temporary files
RUN chmod 1777 /tmp

# Expose port
EXPOSE 9090

# Start the FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9090"]