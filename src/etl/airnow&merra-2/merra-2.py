import os
from pathlib import Path
import datetime as dt
import logging

import earthaccess as ea
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.path import Path as PolygonPath

# Carga .env solo en local (no en producción)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

from google.cloud import bigquery

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("merra2_ingest")

# -----------------------------
# Parámetros
# -----------------------------
SHORT_NAME = os.getenv("MERRA2_SHORT_NAME", "M2T1NXSLV")
VERSION    = os.getenv("MERRA2_VERSION", "5.12.4")
KEEP_VARS  = os.getenv("MERRA2_KEEP_VARS", "T2M,T2MDEW,RH2M,QV2M,U10M,V10M,PS,SLP,TS").split(",")

# Rango: último año (UTC) hasta ayer 23:59:59
# end_dt   = (dt.datetime.utcnow().replace(microsecond=0, second=59, minute=59))
# start_dt = (end_dt - dt.timedelta(days=int(os.getenv("DAYS_BACK", "365")))).replace(hour=0, minute=0, second=0)
# DATE_START = start_dt.strftime("%Y-%m-%d %H:%M:%S")
# DATE_END   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

# Rango fijo: 01-ene-2024 00:00:00 a 31-dic-2024 23:59:59 (UTC)
start_dt = dt.datetime(2024, 1, 1, 0, 0, 0)
end_dt   = dt.datetime(2024, 12, 31, 23, 59, 59)

DATE_START = start_dt.strftime("%Y-%m-%d %H:%M:%S")
DATE_END   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

# Polígono de California (lon, lat) — ajusta si quieres
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

# Directorio temporal (obligatorio en Cloud Functions/Run)
DATA_DIR = Path(os.getenv("WORK_DIR", "/tmp/data_extraction"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# BigQuery (dataset y tabla)
BQ_PROJECT = os.getenv("BQ_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
BQ_DATASET = os.getenv("BIGQUERY_DATASET_ID")
BQ_TABLE   = os.getenv("BIGQUERY_TABLE_ID")      # solo el nombre de tabla, p.ej. "merra2_hourly"
BQ_LOC     = os.getenv("BIGQUERY_LOCATION", "US")

# -----------------------------
# Login Earthdata (desde variables/Secret Manager)
# NO persistimos a ~/.netrc
# -----------------------------
# Las credenciales se leen automáticamente de las variables de entorno
# EARTH_ACCESS_USERNAME y EARTH_ACCESS_PASSWORD.
auth = ea.login(persist=False)
if not auth.authenticated:
    raise RuntimeError("Earthaccess login failed. Revisa EARTH_ACCESS_USERNAME/PASSWORD (usa Secret Manager en Cloud).")

logger.info(f"Searching {SHORT_NAME} v{VERSION} from {DATE_START} to {DATE_END} ...")

# -----------------------------
# Buscar y descargar
# -----------------------------
results = ea.search_data(
    short_name=SHORT_NAME,
    version=VERSION,
    temporal=(DATE_START, DATE_END),
    polygon=polygon_coords,
)
logger.info(f"Granules found: {len(results)}")

# -----------------------------
# Utilidades
# -----------------------------
def to_minus180_180(arr):
    return ((arr + 180) % 360) - 180

def get_bbox(poly):
    lons = [p[0] for p in poly]; lats = [p[1] for p in poly]
    return min(lons), min(lats), max(lons), max(lats)

lon_min, lat_min, lon_max, lat_max = get_bbox(polygon_coords)
poly_path = PolygonPath(polygon_coords)

def ds_to_dataframe(ds: xr.Dataset, keep_vars=None) -> pd.DataFrame:
    # MERRA-2 suele estar en 0..360; lo llevamos a -180..180
    try:
        if float(ds.lon.max()) > 180:
            ds = ds.assign_coords(lon=to_minus180_180(ds.lon)).sortby("lon")
    except Exception:
        pass

    ds_clip = ds.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
    if keep_vars:
        present = [v for v in keep_vars if v in ds_clip.data_vars]
        if present:
            ds_clip = ds_clip[present]

    ds_clip = ds_clip.rename({"lat": "latitude", "lon": "longitude"})
    df = ds_clip.to_dataframe().reset_index()
    df.dropna(how="any", inplace=True)

    if not df.empty:
        mask = poly_path.contains_points(df[["longitude", "latitude"]].to_numpy())
        df = df.loc[mask]

    cols = ["time","latitude","longitude"] + [c for c in df.columns if c not in ("time","latitude","longitude")]
    return df[cols]

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
    table_ref = f"{client.project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

# -----------------------------
# Proceso y carga en BigQuery
# -----------------------------
def run_ingest():
    if not (BQ_PROJECT and BQ_DATASET and BQ_TABLE):
        raise RuntimeError("Faltan BQ_PROJECT_ID/BIGQUERY_DATASET_ID/BIGQUERY_TABLE_ID")

    client = bigquery.Client(project=BQ_PROJECT)
    ensure_dataset(client, BQ_DATASET, location=BQ_LOC)

    loaded_total = 0

    for i, gran in enumerate(results, start=1):
        # Descarga a /tmp y procesa 1 por 1 (memoria amigable)
        try:
            saved_paths = ea.download(gran, local_path=str(DATA_DIR))
        except Exception as e:
            logger.error(f"Falló download granule: {e}")
            continue

        for fpath in (saved_paths or []):
            try:
                fp = str(fpath)
                ds = xr.open_dataset(fp, decode_times=True, engine="h5netcdf")
                df = ds_to_dataframe(ds, keep_vars=KEEP_VARS)
                ds.close()

                if df.empty:
                    logger.info(f"[{i}] vacío tras filtros → {fp}")
                    continue

                # campos extra
                df["time"] = pd.to_datetime(df["time"], utc=True)
                df["source_product"] = SHORT_NAME
                df["source_version"] = VERSION

                upload_df(client, df, dataset=BQ_DATASET, table=BQ_TABLE)

                loaded_total += len(df)
                logger.info(f"[{i}] uploaded: {len(df):,} rows (total {loaded_total:,})")
            except Exception as e:
                logger.error(f"Error procesando {fpath}: {e}")
            finally:
                # Limpia archivo para no llenar /tmp
                try:
                    Path(fpath).unlink(missing_ok=True)
                except Exception:
                    pass

    logger.info(f"Done. Total rows uploaded: {loaded_total:,}")
    return {"rows_total": loaded_total, "granules": len(results)}

# -----------------------------
# Entrypoints
# -----------------------------
def ingest_http(request=None):
    """
    Entrypoint HTTP para Cloud Functions/Run.
    - Cloud Run: mapea a / (Flask no es necesario)
    - Cloud Functions: exporta este callable
    """
    out = run_ingest()
    return (out, 200) if not isinstance(out, tuple) else out

if __name__ == "__main__":
    run_ingest()
