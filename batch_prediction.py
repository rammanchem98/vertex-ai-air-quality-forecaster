import yaml
from kfp import dsl
from kfp.dsl import component, Dataset, Input, Model, Artifact, OutputPath, Output, Metrics
from kfp import compiler
import os
import sys

env = os.getenv("env", "dev")
with open(f"config-{env}.yaml", "r") as file:
    config = yaml.safe_load(file)

PIPELINE_ROOT = config["prediction_pipeline"]["root"]
PIPELINE_NAME = config["prediction_pipeline"]["name"]
PACKAGE_PATH = config["prediction_pipeline"]["package_path"]
PROJECT_ID = config["prediction_pipeline"]["components"]["batch_prediction_component"]["parameters"]["project"]
SQL_QUERY = config["prediction_pipeline"]["components"]["get_data"]["parameters"]["sql_query"]
DATASET_ID = config["prediction_pipeline"]["components"]["write_to_BQ_component"]["parameters"]["dataset_id"]
TABLE_ID = config["prediction_pipeline"]["components"]["write_to_BQ_component"]["parameters"]["table_id"]


@component(base_image=config["training_pipeline"]["components"]["get_data"]["base_image"])
def get_data(project_id: str, sql_query: str, output_data: Output[Dataset]):
    from google.cloud import bigquery
    import logging
    import sys
    import traceback
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    try:
        client = bigquery.Client(project=project_id)
        df = client.query(sql_query).to_dataframe()
        df.to_csv(output_data.path, index=False)
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e


@component(base_image=config["training_pipeline"]["components"]["preprocessing"]["base_image"])
def check_data_empty(dataset: Input[Dataset], is_empty_flag: OutputPath(str)):
    import pandas as pd
    import logging
    import sys
    import traceback
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    try:
        df = pd.read_csv(dataset.path)
        if df.empty or len(df) == 0:
            with open(is_empty_flag, "w") as f:
                f.write("true")
        else:
            with open(is_empty_flag, "w") as f:
                f.write("false")
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e


@component(base_image=config["training_pipeline"]["components"]["preprocessing"]["base_image"])
def preprocessing(raw_input_data: Input[Dataset], input_data_preprocessed: Output[Dataset]):
    import pandas as pd
    import logging
    import traceback
    from src.preprocessing import get_preprocessing

    logging.getLogger().setLevel(logging.INFO)
    try:
        df = pd.read_csv(raw_input_data.path)
        processed_df = get_preprocessing(df=df, is_inference=True)
        processed_df.to_csv(input_data_preprocessed.path, index=False)
    except Exception as e:
        logging.error(traceback.format_exc())
        raise e


@component(base_image=config["prediction_pipeline"]["components"]["batch_prediction_component"]["base_image"])
def batch_prediction(
        project_id: str,
        dataset_id: str,
        table_id: str,
        raw_dataset: Input[Dataset],
        dataset_in: Input[Dataset],
        input_model: Input[Model],
        input_scaler: Input[Artifact]
):
    import pandas as pd
    import pickle
    import logging
    import sys
    import traceback
    import numpy as np
    from google.cloud import bigquery

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)

    try:
        client = bigquery.Client(project=project_id)
        raw_df = pd.read_csv(raw_dataset.path)
        preprocessing_df = pd.read_csv(dataset_in.path)

        with open(input_scaler.path, 'rb') as f:
            scaler = pickle.load(f)

        with open(input_model.path, 'rb') as f:
            model = pickle.load(f)

        logger.info("Transforming features and running inference...")
        scaled_features = scaler.transform(preprocessing_df)
        scaled_features = np.nan_to_num(scaled_features)
        predictions = model.predict(scaled_features)

        naive_timestamps = pd.to_datetime(raw_df['update_timestamp']).dt.tz_localize(None)

        predicted_output_df = pd.DataFrame({
            'city': raw_df['city'],
            'updated_timestamp': naive_timestamps,
            'predicted_air_quality': predictions,
        })

        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job = client.load_table_from_dataframe(predicted_output_df, table_ref)
        job.result()
        logger.info("Batch prediction completely uploaded.")
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e



@component(
    base_image=config["prediction_pipeline"]["components"]["batch_prediction_component"]["base_image"]
)
def evaluate_live_prediction(
        project_id: str,
        dataset_id: str,
        prediction_table: str,
        ground_truth_table: str,
        metrics: Output[Metrics]
):
    import pandas as pd
    import logging
    import sys
    import traceback
    from google.cloud import bigquery

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    logger = logging.getLogger(__name__)
    try:
        logger.info("Starting live performance evaluation...")
        client = bigquery.Client(project=project_id)

        query = f"""
            SELECT COUNT(*) as common_records 
            FROM `{project_id}.{dataset_id}.{prediction_table}` p
            JOIN `{project_id}.{dataset_id}.{ground_truth_table}` g
            ON p.city = g.city AND p.updated_timestamp = g.update_timestamp
        """
        df = client.query(query).to_dataframe()
        record_count = int(df.iloc[0]['common_records'])

        # 2. Log the numerical metric explicitly to populate executor_output.json
        metrics.log_metric("shared_sync_records", record_count)
        logger.info(f"Evaluation complete. Shared sync records checked: {record_count}")

    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


@dsl.pipeline(name=PIPELINE_NAME, pipeline_root=PIPELINE_ROOT)
def batch_pipeline(
        project_id: str = PROJECT_ID,
        sql_query: str = SQL_QUERY,
        dataset_id: str = DATASET_ID,
        table_id: str = TABLE_ID,
):
    model_importer = dsl.importer(
        artifact_uri="gs://air-quality-project/pipeline_root/production/latest_model.pkl",
        artifact_class=Model,
        reimport=False
    ).output

    scaler_importer = dsl.importer(
        artifact_uri="gs://air-quality-project/pipeline_root/production/latest_scaler.pkl",
        artifact_class=Artifact,
        reimport=False
    ).output

    data_task = get_data(project_id=project_id, sql_query=sql_query)
    data_task.set_caching_options(True)

    check_data_task = check_data_empty(dataset=data_task.outputs["output_data"])
    check_data_task.set_caching_options(True)

    with dsl.If(check_data_task.outputs["is_empty_flag"] == "false",name="Verifying Prediction input is not empty"):
        preprocess_task = preprocessing(raw_input_data=data_task.outputs["output_data"])
        preprocess_task.set_caching_options(True)

        batch_predict_task = batch_prediction(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            raw_dataset=data_task.outputs["output_data"],
            dataset_in=preprocess_task.outputs["input_data_preprocessed"],
            input_model=model_importer,
            input_scaler=scaler_importer
        )
        batch_predict_task.set_caching_options(True)

        eval_task = evaluate_live_prediction(
            project_id=project_id,
            dataset_id=dataset_id,
            prediction_table=table_id,
            ground_truth_table="world_air_quality"
        )
        eval_task.set_caching_options(True)
        eval_task.after(batch_predict_task)


if __name__ == "__main__":
    compiler.Compiler().compile(pipeline_func=batch_pipeline, package_path=PACKAGE_PATH)
    print("Pipeline compiled successfully.")