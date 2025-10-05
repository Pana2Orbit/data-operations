import os
import tempfile
import shutil
from typing import List, Tuple

import earthaccess as ea
import pandas as pd
import xarray as xr

from matplotlib.path import Path as PolygonPath
from src.services.utils import get_logger

logger = get_logger("earthdata-client")


class EarthDataClient:
    """A singleton client for interacting with NASA EarthData.

    Notes
    -----
    - `get_data` returns a pandas.DataFrame constructed from one or more
      granules returned by `ea.search_data`.
    - Files are downloaded and processed one-at-a-time into temporary files
      to avoid keeping all raw granules in memory simultaneously.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.client = ea
        return cls._instance

    def __init__(self):
        auth = ea.login(
            os.getenv("EARTHDATA_USERNAME"), os.getenv("EARTHDATA_PASSWORD")
        )
        self.is_authenticated = auth.authenticated
        if not self.is_authenticated:
            raise ValueError(
                "Failed to authenticate with EarthData. Check your credentials."
            )

    def _iter_granules(self, search_result) -> List:
        """Normalize `ea.search_data` return value into a list of granule objects.

        `ea.search_data` can return either a list of granule dicts or a dict
        containing a "files" list. This helper returns a list of granule-like
        objects that can be passed to `ea.download`.
        """
        if search_result is None:
            return []
        if isinstance(search_result, dict) and "files" in search_result:
            return list(search_result["files"]) or []
        if isinstance(search_result, list):
            return search_result
        try:
            return list(search_result)
        except (TypeError, ValueError, RuntimeError):
            return []

    def _filter_polygon(
        self, data: pd.DataFrame, polygon: list[tuple[float, float]]
    ) -> pd.DataFrame:
        """Filter a DataFrame by a polygon.

        Parameters
        ----------
        polygon : list[tuple[float, float]]
            A list of (longitude, latitude) tuples defining the polygon.

        Returns
        -------
        pd.DataFrame
            The filtered DataFrame.
        """
        polygon_path = PolygonPath(polygon)
        mask = polygon_path.contains_points(data[['longitude', 'latitude']].values)
        return data[mask]

    def get_data(
        self,
        dataset_name: str,
        dataset_version: str,
        start_date: str,
        end_date: str,
        polygon: List[Tuple[float, float]],
    ) -> pd.DataFrame:
        """Search for granules and return their concatenated contents as a DataFrame.

        Behavior and guarantees:
        - Generic: does not assume particular variable names. It converts each
          xarray.Dataset to a DataFrame via `Dataset.to_dataframe()` then
          resets the index. This yields a general, tabular representation.
        - Memory conscious: processes one downloaded file at a time and removes
          temporary files after use.

        Parameters
        ----------
        dataset_name, dataset_version, start_date, end_date, polygon
            Passed directly to `ea.search_data`.

        Returns
        -------
        pd.DataFrame
            Concatenation of the per-granule DataFrames. If no data found an
            empty DataFrame is returned.
        """
        if not self.is_authenticated:
            raise ValueError("Client is not authenticated. Please login first.")

        search_results = ea.search_data(
            short_name=dataset_name,
            version=dataset_version,
            temporal=(start_date, end_date),
            polygon=polygon,
        )

        if len(search_results) == 0:
            logger.warning("No granules found.")
            return pd.DataFrame()
        logger.info(f"Found {len(search_results)} granules.")

        frames: List[pd.DataFrame] = []

        tmpdir = tempfile.mkdtemp(prefix="earthdata_")
        try:
            file_paths = []
            for granule in search_results:
                try:
                    dl = ea.download(granule, local_path=tmpdir)
                    file_paths.append(str(dl[0]))
                except (RuntimeError, OSError, ValueError, TypeError) as e:
                    logger.warning(
                        f"Failed to download granule:\n{granule}\nDue to: {e}"
                    )
                    continue

                for fp in file_paths:
                    try:
                        ds = xr.open_dataset(fp)
                    except (OSError, ValueError, RuntimeError, TypeError):
                        continue

                    try:
                        df = ds.to_dataframe().reset_index()
                        if not df.empty:
                            frames.append(df)
                    finally:
                        try:
                            ds.close()
                        except (RuntimeError, OSError):
                            pass

                    # Remove the file to avoid accumulating many files
                    try:
                        if os.path.exists(fp):
                            os.remove(fp)
                    except (OSError, PermissionError):
                        pass

            if frames:
                result_df = pd.concat(frames, axis=0, ignore_index=True, copy=False)
            else:
                logger.warning("No valid data found.")
                result_df = pd.DataFrame()

            result_df = self._filter_polygon(result_df, polygon)
            return result_df
        finally:
            # cleanup temporary directory
            try:
                shutil.rmtree(tmpdir)
            except (OSError, PermissionError):
                pass
