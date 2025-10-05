import pandas as pd
from src.services.earth_data import EarthDataClient
from src.services.google import Google
from src.services.utils import get_logger

logger = get_logger(__name__)


def extract_and_load_no2(
    dataset_name: str = "TEMPO_NO2_L3",
    dataset_version: str = "V03",
    start_date: str = "2025-01-01 00:00:00",
    end_date: str = "2025-01-01 15:59:59",
    polygon: list[tuple[float, float]] = None,
) -> None:
    """
    Extract data from EarthData and load it into BigQuery.
    """
    client = EarthDataClient()
    google = Google()
    logger.info(
        f"Extracting data for {dataset_name} version {dataset_version} from {start_date} to {end_date}"
    )
    df = client.get_data(dataset_name, dataset_version, start_date, end_date, polygon)
    logger.info(
        f"Extracted {len(df)} records from {dataset_name} version {dataset_version}."
    )
    if df.empty:
        logger.warning("No data extracted. Exiting.")
        raise ValueError("No data extracted from EarthData.")

    df = df.dropna()
    df = df.drop_duplicates()
    df['time'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Data cleaned. {len(df)} records remaining after cleaning.")
    try:
        logger.info("Uploading data to BigQuery...")
        _ = google.bigquery.upload_data_from_dataframe(
            df=df,
            dataset="earth_data",
            table_id="no2_historical",
        )
    except Exception as e:
        logger.error(f"Error uploading data to BigQuery: {e}")
        raise
    logger.info("Data successfully loaded into BigQuery.")
    return None
