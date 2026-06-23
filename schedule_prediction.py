import yaml
import os
from google.cloud import aiplatform

# 1. Load your project configuration environment
env = os.getenv("env", "dev")
with open(f"config-{env}.yaml", "r") as file:
    config = yaml.safe_load(file)

PROJECT_ID = config["prediction_pipeline"]["components"]["batch_prediction_component"]["parameters"]["project"]
PIPELINE_ROOT = config["prediction_pipeline"]["root"]
PACKAGE_PATH = config["prediction_pipeline"]["package_path"] # points to your batch_prediction_pipeline.json

# 2. Initialize Vertex AI SDK
aiplatform.init(project=PROJECT_ID, location="us-central1")

print(f"Creating Daily Pipeline Schedule for {PACKAGE_PATH}...")

# 3. Define and deploy the automated Cron Schedule
pipeline_schedule = aiplatform.PipelineJobSchedule(
    pipeline_job=aiplatform.PipelineJob(
        display_name="air-quality-daily-batch-prediction",
        template_path=PACKAGE_PATH,
        pipeline_root=PIPELINE_ROOT,
        enable_caching=True, # Keeps prediction costs low by using cached extraction tasks if data hasn't changed
    ),
    display_name="air-quality-prediction-cron",
)

# 4. Start the schedule using a standard UNIX cron expression (Runs daily at 2:00 AM UTC)
pipeline_schedule.create(
    cron="0 2 * * *",
    max_concurrent_run_count=1 # Prevents overlapping pipeline runs if data processing lags
)

print("Pipeline schedule successfully deployed to Vertex AI!")