FROM python:3.10-slim

# Turn off write caching to prevent auxiliary files from building in disk storage
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Pull requirements using the relative sub-folder execution layer context path
COPY ingest_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Extract runtime data ingestion logic and master single-source configurations
COPY ingest_pipeline/ingest_data.py .
COPY config-dev.yaml .

# Execute the isolated ingestion step immediately upon task container invocation
CMD ["python", "ingest_data.py"]