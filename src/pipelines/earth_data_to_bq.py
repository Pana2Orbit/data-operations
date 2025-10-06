import json
from datetime import datetime, timedelta

import pandas as pd
from kfp.v2 import dsl
from kfp.v2.dsl import component, Dataset, Input, Output

from src.services.earth_data import EarthDataClient
from src.services.google import Google


@component(
    base_image="python:3.13-slim",
    packages_to_install=[
        "earthaccess",
        "pandas",
        "xarray",
        "python-dotenv",
        "loguru",
        "coloredlogs",
        "netcdf4",
        "pyarrow",
    ],
)
def get_data_component(
    dataset_name: str,
    dataset_version: str,
    start_date: str,
    end_date: str,
    polygon_str: str,
    output_dataset: Output[Dataset] = None,
):
    """
    Get data from EarthData and save it to a parquet file as a Vertex AI pipeline dataset.
    """
    polygon = json.loads(polygon_str)
    client = EarthDataClient()
    df = client.get_data(dataset_name, dataset_version, start_date, end_date, polygon)
    df.to_parquet(output_dataset.path)


@component(base_image="python:3.13-slim", packages_to_install=["pandas", "pyarrow"])
def clean_data_component(
    input_dataset: Input[Dataset], output_dataset: Output[Dataset] = None
):
    """
    Clean the data by removing NaNs and duplicates.
    """

    df = pd.read_parquet(input_dataset.path)
    df = df.dropna()
    df = df.drop_duplicates()
    df.to_parquet(output_dataset.path)


@component(
    base_image="python:3.13-slim",
    packages_to_install=["google-cloud-bigquery", "pandas", "pyarrow", "python-dotenv"],
)
def upload_to_bq_component(
    input_dataset: Input[Dataset], bq_dataset: str, table_id: str
):
    """
    Upload the cleaned data to BigQuery.
    """
    df = pd.read_parquet(input_dataset.path)
    google = Google()
    _ = google.bigquery.upload_data_from_dataframe(df, bq_dataset, table_id)


@dsl.pipeline(
    name="earth-data-to-bq-pipeline",
    description="Pipeline to get earth data, clean it, and upload to BigQuery",
)
def earth_data_pipeline(
    dataset_name: str,
    dataset_version: str,
    polygon_str: str,
    bq_dataset: str,
    table_id: str,
    start_date: str = None,
    end_date: str = None,
):
    """
    Get Earth data, clean it, and upload to BigQuery.
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    get_data_task = get_data_component(
        dataset_name=dataset_name,
        dataset_version=dataset_version,
        start_date=start_date,
        end_date=end_date,
        polygon_str=polygon_str,
    )
    clean_data_task = clean_data_component(
        input_dataset=get_data_task.outputs["output_dataset"]
    )
    _ = upload_to_bq_component(
        input_dataset=clean_data_task.outputs["output_dataset"],
        bq_dataset=bq_dataset,
        table_id=table_id,
    )
