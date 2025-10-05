import os
from src.etl.extract_load_no2 import extract_and_load_no2

def main():
    username = os.getenv("EARTH_ACCESS_USERNAME")
    password = os.getenv("EARTH_ACCESS_PASSWORD")
    if not username or not password:
        raise RuntimeError("Missing EARTH_ACCESS_USERNAME or EARTH_ACCESS_PASSWORD")

    short_name = "TEMPO_NO2_L3"
    version = "V03"
    date_start = "2024-04-01 00:00:00"
    date_end = "2024-04-30 23:59:59"
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

    extract_and_load_no2(
        dataset_name=short_name,
        dataset_version=version,
        start_date=date_start,
        end_date=date_end,
        polygon=polygon_coords,
    )

if __name__ == "__main__":
    main()
