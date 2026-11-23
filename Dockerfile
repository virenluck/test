
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing bytecode
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create directory for Excel log persistence
RUN mkdir -p /app/data

# Default configuration
ENV PORT=8000
ENV EXCEL_FILE=/app/data/BTC_Trading_Log.xlsx

# Expose FastAPI port
EXPOSE 8000

# Start FastAPI application
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
