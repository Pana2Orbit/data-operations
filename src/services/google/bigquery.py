from pathlib import Path
import os
from typing import Union
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

from src.services.utils import get_logger

load_dotenv()

logger = get_logger()

class BigQueryClient:
    """
    A client for interacting with Google BigQuery.
    """
    def __init__(self):
        raw_project = os.getenv("GCP_PROJECT_ID")
        self.location = os.getenv("BIGQUERY_LOCATION", "US")

        # Normalize and validate project id if provided
        if raw_project:
            proj = raw_project.strip().lower()
            # valid project id characters: lowercase letters, digits and hyphens
            if not proj or any(c for c in proj if not (c.isalnum() or c == "-")):
                raise ValueError(
                    "Invalid GCP_PROJECT_ID environment variable; must contain only lowercase letters, digits or hyphens."
                )
            if proj != raw_project:
                logger.info("Normalized GCP_PROJECT_ID from '%s' to '%s'", raw_project, proj)
            self.client = bigquery.Client(project=proj, location=self.location)
            self.project_id = proj
        else:
            # Let the client pick up default project from ADC / gcloud config
            self.client = bigquery.Client(location=self.location)
            self.project_id = getattr(self.client, "project", None)

        logger.info("BigQuery client initialized (project=%s, location=%s)", self.project_id, self.location)

    def get_data(self, query: Union[str, Path]) -> pd.DataFrame:
        """
        Execute a BigQuery query and return the results as a pandas DataFrame.

        Args:
            query: Either a SQL query string or a path to a .sql file.

        Returns:
            pd.DataFrame: The query results.
        """
        if isinstance(query, str) and Path(query).exists():
            with open(query, 'r', encoding='utf-8') as f:
                sql = f.read()
        else:
            sql = str(query)

        query_job = self.client.query(sql)
        results = query_job.result()
        df = results.to_dataframe()
        return df

    def upload_data_from_dataframe(self, df: pd.DataFrame, dataset: str, table_id: str):
        """
        Upload a pandas DataFrame to a specified BigQuery table.

        Args:
            df: The pandas DataFrame to upload.
            dataset: The BigQuery dataset name.
            table_id: The BigQuery table ID in the format `dataset.table`.
        """
        # Ensure the dataset exists. Use the client's notion of project when possible.
        try:
            dataset_ref = self.client.dataset(dataset)
            self.client.get_dataset(dataset_ref)
            logger.info("Dataset %s already exists (project=%s).", dataset, self.project_id)
        except Exception as e:
            # If dataset missing, create it in the configured location
            if "Not found" in str(e) or getattr(e, 'code', None) == 404:
                dataset_obj = bigquery.Dataset(dataset_ref)
                dataset_obj.location = self.location
                self.client.create_dataset(dataset_obj)
                logger.info("Created dataset %s in location %s (project=%s).", dataset, self.location, self.project_id)
            else:
                logger.error("Error checking/creating dataset: %s", e)
                raise

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        # Build destination; if project_id is None, let client infer it by using dataset.table
        if self.project_id:
            destination = f"{self.project_id}.{dataset}.{table_id}"
        else:
            destination = f"{dataset}.{table_id}"

        load_job = self.client.load_table_from_dataframe(df, destination, job_config=job_config)
        result = load_job.result()
        if result.errors:
            for error in result.errors:
                logger.error(f"Error uploading data to BigQuery: {error}")
        else:
            logger.info("Data uploaded successfully.")
