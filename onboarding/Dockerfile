FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Note: This Dockerfile must be built from the parent directory (shopify-automated-pipelines)
# Build command: docker build -f onboarding/Dockerfile .

# Copy requirements files first for better caching
COPY onboarding/requirements.txt /app/onboarding/
COPY historical/requirements.txt /app/historical/

# Upgrade pip first
RUN pip install --upgrade pip

# Install Python dependencies with increased timeout and retries
RUN pip install --no-cache-dir --timeout=300 --retries=5 -r /app/onboarding/requirements.txt
RUN pip install --no-cache-dir --timeout=300 --retries=5 -r /app/historical/requirements.txt

# Copy the onboarding app files
COPY onboarding/ /app/onboarding/

# Copy the historical pipeline (needed for subprocess calls)
COPY historical/ /app/historical/

# Set environment variables for Streamlit
ENV PORT=8080
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8080/_stcore/health || exit 1

# Run Streamlit
CMD streamlit run onboarding/app.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    --theme.primaryColor="#17D4BE" \
    --theme.backgroundColor="#FFFFFF" \
    --theme.secondaryBackgroundColor="#F8FFFE" \
    --theme.textColor="#002C5F"