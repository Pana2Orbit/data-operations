# -*- coding: utf-8 -*-
"""
Ingesta completa de AirNow API (O3, PM2.5) para California.
Descarga datos desde 1 de enero hasta 31 de diciembre de 2024 (hora por hora)
y los carga en BigQuery, igual que el script MERRA-2.
"""

import os
import datetime as dt
import logging
import time
import requests
import pandas as pd
from matplotlib.path import Path as PolygonPath
from google.cloud import bigquery
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# ---------------------------------------------
# Configuración y logging
# ---------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("airnow_ingest")

AIRNOW_API_KEY = os.getenv("AIRNOW_API_KEY")
BQ_PROJECT = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET_ID")
BQ_TABLE   = os.getenv("BIGQUERY_TABLE_ID_AIRNOW", "airnow_hourly_2024_full")
BQ_LOC     = os.getenv("BIGQUERY_LOCATION", "US")

if not all([AIRNOW_API_KEY, BQ_PROJECT, BQ_DATASET, BQ_TABLE]):
    raise RuntimeError("Faltan variables de entorno requeridas para AirNow o BigQuery.")

# ---------------------------------------------
# Rango completo: 1-ene-2024 → 31-dic-2024
# ---------------------------------------------
START_DATE = dt.datetime(2024, 1, 1)
END_DATE   = dt.datetime(2024, 12, 31, 23)

# Polígono de California
polygon_coords = [
    (-120.0091050, 41.9727325),
    (-124.6045661, 41.8898826),
    (-120.4462801, 33.9044735),
    (-117.1073262, 32.6184122),
    (-114.2955756, 32.6554188),
    (-114.1637748, 34.3047333),
    (-114.7349117, 35.0995465),
    (-120.0948112, 39.0254518),
    (-120.0091050, 41.9727325),
]
poly_path = PolygonPath(polygon_coords)
BBOX = "-124.6046,32.6184,-114.1637,41.9727"

# ---------------------------------------------
# Funciones auxiliares
# ---------------------------------------------
def iso_for_day(d):
    return d.strftime("%Y-%m-%dT00"), (d + dt.timedelta(days=1)).strftime("%Y-%m-%dT00")

def fetch_day(t0, t1):
    """Descarga 24 horas de datos de AirNow para un día."""
    url = "https://www.airnowapi.org/aq/data/"
    params = {
        "startDate": t0,
        "endDate": t1,
        "parameters": "OZONE,PM25",
        "BBOX": BBOX,
        "dataType": "C",
        "format": "application/json",
        "verbose": "0",
        "nowcastonly": "0",
        "API_KEY": AIRNOW_API_KEY,
    }

    for tries in range(5):
        r = requests.get(url, params=params, timeout=90)
        if r.status_code == 200:
            break
        wait = 2 ** tries
        logger.warning(f"[airnow] HTTP {r.status_code}. retry in {wait}s...")
        time.sleep(wait)
    else:
        logger.error(f"[airnow] sin respuesta para {t0} → {t1}")
        return pd.DataFrame()

    try:
        data = r.json()
    except Exception:
        logger.error(f"[airnow] error parseando JSON para {t0}")
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    for c in ("Concentration", "Latitude", "Longitude", "HourObserved"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Combina fecha + hora → UTC
    if {"DateObserved", "HourObserved"}.issubset(df.columns):
        df["datetime_local"] = pd.to_datetime(
            df["DateObserved"].astype(str) + " " + df["HourObserved"].astype(str) + ":00",
            errors="coerce"
        )
        try:
            df["time_utc"] = (
                df["datetime_local"]
                .dt.tz_localize("America/Los_Angeles", ambiguous="infer", nonexistent="shift_forward")
                .dt.tz_convert("UTC")
                .dt.tz_localize(None)
            )
        except Exception:
            df["time_utc"] = pd.NaT

    if {"Latitude", "Longitude"}.issubset(df.columns):
        df["inside_poly"] = poly_path.contains_points(df[["Longitude", "Latitude"]].to_numpy())
        df = df[df["inside_poly"]].copy()
        df.rename(columns={"Latitude": "latitude", "Longitude": "longitude"}, inplace=True)

    df["source"] = "AirNow API"
    return df

def fetch_year(start_date, end_date):
    """Descarga datos día por día para todo un año."""
    cur = start_date
    frames = []
    while cur <= end_date:
        t0, t1 = iso_for_day(cur)
        logger.info(f"Descargando {t0} → {t1}")
        df = fetch_day(t0, t1)
        if not df.empty:
            frames.append(df)
        else:
            logger.warning(f"⚠️ Día vacío: {t0}")
        cur += dt.timedelta(days=1)
        time.sleep(0.3)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------------------------------------------
# BigQuery helpers
# ---------------------------------------------
def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "US"):
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    try:
        client.get_dataset(ds_ref)
        return
    except Exception:
        ds_ref.location = location
        client.create_dataset(ds_ref)
        logger.info(f"Dataset creado: {ds_ref.full_dataset_id}")

def upload_df(client: bigquery.Client, df: pd.DataFrame, dataset: str, table: str):
    if df.empty:
        logger.warning("⚠️ DataFrame vacío, no se sube nada.")
        return
    table_ref = f"{client.project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info(f"✅ {len(df):,} filas subidas a {table_ref}")

# ---------------------------------------------
# MAIN
# ---------------------------------------------
def run_ingest():
    client = bigquery.Client(project=BQ_PROJECT)
    ensure_dataset(client, BQ_DATASET, location=BQ_LOC)

    logger.info("🚀 Iniciando ingesta AirNow anual (2024-01-01 → 2024-12-31)")
    df = fetch_year(START_DATE, END_DATE)
    if df.empty:
        logger.warning("No se descargaron datos válidos del año.")
        return
    upload_df(client, df, dataset=BQ_DATASET, table=BQ_TABLE)
    logger.info(f"✅ Ingesta completada: {len(df):,} filas subidas.")

def ingest_http(request=None):
    out = run_ingest()
    return (out, 200) if not isinstance(out, tuple) else out

if __name__ == "__main__":
    run_ingest()
