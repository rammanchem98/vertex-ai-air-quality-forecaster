from google.cloud import aiplatform

# Initialize the Vertex client
aiplatform.init(project="london-air-quality-pipeline", location="us-central1")

# Submit the pipeline directly to the API
job = aiplatform.PipelineJob(
    display_name="air-quality-batch-prediction",
    template_path="batch_prediction_pipeline.json",
    enable_caching=False
)

job.submit()
print("Prediction pipeline submitted successfully!.")