# Use the official deep learning platform base container as requested by your framework
FROM gcr.io/deeplearning-platform-release/base-cpu

# Prevent Python from writing auxiliary .pyc status cache tracking logs to disk
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /

# 1. Mount your data transformations and cleaning library code modules
COPY ./src/ /src

# 2. Extract your multi-stage Kubeflow pipeline graph definitions
COPY training_pipeline.py /
COPY batch_prediction.py /

# 3. Mount the development environmental parameter resource profiles
COPY config-dev.yaml /

# 4. Copy and install heavy analytical frame processing dependencies
COPY requirements.txt /
RUN pip install --upgrade pip && pip install --no-cache-dir -r /requirements.txt

# Debug trace verification to log internal layout compilation maps within Cloud Build
RUN ls -la / && ls -la /src/