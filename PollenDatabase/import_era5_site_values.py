"""Extract site-level ERA5 values from regional NetCDF files and upsert PostgreSQL.

The CDS files produced by download_era5_study_regions.py may be ordinary NetCDF
files or ZIP archives (despite the .nc extension) containing separate instant
and accumulation NetCDF streams. Both layouts are supported.
"""

import argparse
import math
import os
import tempfile
import zipfile
from contextlib import ExitStack
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_INPUT_DIR = Path("H:/SPIROMICS_ERA5")
DEFAULT_DATABASE = "pollen_dashboard"
GIT_PATH = Path(__file__).resolve().parent

VARIABLE_ALIASES = {
    "temperature_2m_k": ("t2m", "2m_temperature"),
    "dewpoint_2m_k": ("d2m", "2m_dewpoint_temperature"),
    "u10_ms": ("u10", "10m_u_component_of_wind"),
    "v10_ms": ("v10", "10m_v_component_of_wind"),
    "precipitation_m": ("tp", "total_precipitation"),
    "solar_radiation_j_m2": ("ssrd", "surface_solar_radiation_downwards"),
    "relative_humidity_2m_percent": ("relative_humidity_2m_percent",),
    "surface_pressure_pa": ("sp", "surface_pressure"),
    "boundary_layer_height_m": ("blh", "boundary_layer_height"),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Extract nearest-grid ERA5 values for pollen sites and upsert PostgreSQL."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument(
        "--region",
        action="append",
        help="Optional study-region name or folder slug. Repeat to import multiple regions.",
    )
    parser.add_argument(
        "--site-id",
        action="append",
        dest="site_ids",
        help="Optional site_id filter. Repeat to import multiple sites.",
    )
    parser.add_argument("--start", help="Optional inclusive UTC date/time filter.")
    parser.add_argument("--end", help="Optional exclusive UTC date/time filter.")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def require_dependencies():
    try:
        import psycopg2
        from psycopg2.extras import execute_values
        from dotenv import load_dotenv
        import xarray as xr
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency %s. Install with: "
            "pip install numpy pandas xarray netCDF4 psycopg2-binary python-dotenv"
            % exc.name
        ) from exc
    return psycopg2, execute_values, load_dotenv, xr


def safe_region_name(value):
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def parse_utc(value):
    if not value:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp


def connect_database(database, psycopg2, load_dotenv):
    load_dotenv(dotenv_path=GIT_PATH / ".env")
    return psycopg2.connect(
        dbname=database,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PW"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def ensure_era5_table(conn):
    query = """
        CREATE TABLE IF NOT EXISTS era5_hourly_site (
            site_id VARCHAR(100) NOT NULL REFERENCES site(site_id),
            moment TIMESTAMPTZ NOT NULL,
            temperature_2m_k DOUBLE PRECISION,
            dewpoint_2m_k DOUBLE PRECISION,
            precipitation_m DOUBLE PRECISION,
            u10_ms DOUBLE PRECISION,
            v10_ms DOUBLE PRECISION,
            wind_speed_10m_ms DOUBLE PRECISION,
            wind_from_deg DOUBLE PRECISION,
            surface_pressure_pa DOUBLE PRECISION,
            boundary_layer_height_m DOUBLE PRECISION,
            era5_source_id VARCHAR(255),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (site_id, moment)
        );

        ALTER TABLE era5_hourly_site
            ADD COLUMN IF NOT EXISTS relative_humidity_2m_percent DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS surface_solar_radiation_downwards_j_m2 DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS downward_irradiation_w_m2 DOUBLE PRECISION;

        CREATE INDEX IF NOT EXISTS idx_era5_hourly_site_time
            ON era5_hourly_site (site_id, moment);
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def fetch_sites(conn, site_ids=None):
    query = """
        SELECT
            s.site_id,
            COALESCE(c.study_region_name, c.city_name, 'Unknown') AS study_region_name,
            ST_X(s.location) AS longitude,
            ST_Y(s.location) AS latitude
        FROM site s
        LEFT JOIN city c ON c.city_id = s.city_id
        WHERE s.location IS NOT NULL
    """
    params = []
    if site_ids:
        query += " AND s.site_id = ANY(%s)"
        params.append(site_ids)
    query += " ORDER BY study_region_name, s.site_id;"

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("No geocoded sites matched the requested database filters.")
    return [
        {
            "site_id": row[0],
            "study_region_name": row[1],
            "region_slug": safe_region_name(row[1]),
            "longitude": float(row[2]),
            "latitude": float(row[3]),
        }
        for row in rows
    ]


def discover_files(input_dir, requested_regions=None):
    if not input_dir.exists():
        raise FileNotFoundError(
            "ERA5 input directory does not exist: %s. The detected folder on this machine is %s."
            % (input_dir, DEFAULT_INPUT_DIR)
        )
    requested = {safe_region_name(value) for value in requested_regions or []}
    files = []
    for path in sorted(input_dir.rglob("*.nc")):
        region_slug = safe_region_name(path.parent.name)
        if requested and region_slug not in requested:
            continue
        files.append((region_slug, path))
    if not files:
        raise RuntimeError("No .nc files matched the requested input and region filters.")
    return files


def open_archive_datasets(path, xr, stack):
    if not zipfile.is_zipfile(path):
        return [stack.enter_context(xr.open_dataset(path))]

    temporary_dir = stack.enter_context(tempfile.TemporaryDirectory(prefix="era5_nc_"))
    datasets = []
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".nc")]
        if not members:
            raise RuntimeError("ZIP-formatted ERA5 file contains no NetCDF members: %s" % path)
        for index, member in enumerate(members):
            extracted = Path(temporary_dir) / ("%02d_%s" % (index, Path(member).name))
            with archive.open(member) as source, extracted.open("wb") as target:
                while True:
                    block = source.read(1024 * 1024)
                    if not block:
                        break
                    target.write(block)
            datasets.append(stack.enter_context(xr.open_dataset(extracted)))
    return datasets


def find_name(names, candidates):
    for candidate in candidates:
        if candidate in names:
            return candidate
    return None


def normalize_site_longitude(longitude, longitude_values):
    values = np.asarray(longitude_values)
    if values.size and np.nanmin(values) >= 0.0 and longitude < 0.0:
        return longitude % 360.0
    return longitude


def data_array_to_series(data_array, time_name):
    array = data_array.squeeze(drop=True)
    if time_name not in array.dims:
        return None
    series = array.to_series()
    if isinstance(series.index, pd.MultiIndex):
        series = series.groupby(level=time_name).first()
    series.index = pd.to_datetime(series.index, utc=True)
    return series


def extract_dataset_for_site(dataset, site):
    latitude_name = find_name(dataset.coords, ("latitude", "lat"))
    longitude_name = find_name(dataset.coords, ("longitude", "lon"))
    time_name = find_name(dataset.coords, ("valid_time", "time"))
    if not latitude_name or not longitude_name or not time_name:
        raise RuntimeError("NetCDF lacks recognizable latitude, longitude, or time coordinates.")

    longitude = normalize_site_longitude(site["longitude"], dataset[longitude_name].values)
    selected = dataset.sel(
        {latitude_name: site["latitude"], longitude_name: longitude},
        method="nearest",
    )
    columns = {}
    units = {}
    for output_name, aliases in VARIABLE_ALIASES.items():
        source_name = find_name(selected.data_vars, aliases)
        if not source_name:
            continue
        series = data_array_to_series(selected[source_name], time_name)
        if series is not None:
            columns[output_name] = series
            units[output_name] = str(selected[source_name].attrs.get("units", "")).lower()
    if not columns:
        return pd.DataFrame(), units
    frame = pd.concat(columns, axis=1)
    frame.index.name = "moment"
    return frame.reset_index(), units


def combine_site_streams(datasets, site):
    frames = []
    units = {}
    for dataset in datasets:
        frame, frame_units = extract_dataset_for_site(dataset, site)
        if not frame.empty:
            frames.append(frame.set_index("moment"))
            units.update(frame_units)
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, axis=1)
    combined = combined.loc[:, ~combined.columns.duplicated(keep="first")]
    combined = combined.reset_index().sort_values("moment")

    if "precipitation_m" in combined:
        precip_units = units.get("precipitation_m", "")
        if "mm" in precip_units:
            combined["precipitation_m"] = combined["precipitation_m"] / 1000.0

    if "solar_radiation_j_m2" in combined:
        solar_units = units.get("solar_radiation_j_m2", "")
        if "w" in solar_units and "j" not in solar_units:
            combined["downward_irradiation_w_m2"] = combined["solar_radiation_j_m2"]
            combined["solar_radiation_j_m2"] = combined["solar_radiation_j_m2"] * 3600.0
        else:
            combined["downward_irradiation_w_m2"] = combined["solar_radiation_j_m2"] / 3600.0

    if {"temperature_2m_k", "dewpoint_2m_k"}.issubset(combined.columns):
        temperature_c = combined["temperature_2m_k"] - 273.15
        dewpoint_c = combined["dewpoint_2m_k"] - 273.15
        saturation = 6.112 * np.exp((17.67 * temperature_c) / (temperature_c + 243.5))
        actual = 6.112 * np.exp((17.67 * dewpoint_c) / (dewpoint_c + 243.5))
        combined["relative_humidity_2m_percent"] = np.clip(100.0 * actual / saturation, 0.0, 100.0)

    if {"u10_ms", "v10_ms"}.issubset(combined.columns):
        combined["wind_speed_10m_ms"] = np.hypot(combined["u10_ms"], combined["v10_ms"])
        combined["wind_from_deg"] = (
            np.degrees(np.arctan2(-combined["u10_ms"], -combined["v10_ms"])) + 360.0
        ) % 360.0
    return combined


def clean_value(value):
    if value is None or pd.isna(value):
        return None
    return float(value)


def records_for_site(frame, site_id, source_id, start, end):
    if start is not None:
        frame = frame[frame["moment"] >= start]
    if end is not None:
        frame = frame[frame["moment"] < end]

    value_columns = [
        "temperature_2m_k",
        "dewpoint_2m_k",
        "precipitation_m",
        "u10_ms",
        "v10_ms",
        "wind_speed_10m_ms",
        "wind_from_deg",
        "relative_humidity_2m_percent",
        "surface_solar_radiation_downwards_j_m2",
        "downward_irradiation_w_m2",
        "surface_pressure_pa",
        "boundary_layer_height_m",
    ]
    frame = frame.rename(columns={"solar_radiation_j_m2": "surface_solar_radiation_downwards_j_m2"})
    records = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        records.append(
            (
                site_id,
                pd.Timestamp(values["moment"]).to_pydatetime(),
                *(clean_value(values.get(column)) for column in value_columns),
                source_id,
            )
        )
    return records


def upsert_records(conn, records, execute_values, batch_size):
    if not records:
        return 0
    query = """
        INSERT INTO era5_hourly_site (
            site_id, moment, temperature_2m_k, dewpoint_2m_k, precipitation_m,
            u10_ms, v10_ms, wind_speed_10m_ms, wind_from_deg,
            relative_humidity_2m_percent,
            surface_solar_radiation_downwards_j_m2, downward_irradiation_w_m2,
            surface_pressure_pa, boundary_layer_height_m, era5_source_id, updated_at
        ) VALUES %s
        ON CONFLICT (site_id, moment) DO UPDATE SET
            temperature_2m_k = EXCLUDED.temperature_2m_k,
            dewpoint_2m_k = EXCLUDED.dewpoint_2m_k,
            precipitation_m = EXCLUDED.precipitation_m,
            u10_ms = EXCLUDED.u10_ms,
            v10_ms = EXCLUDED.v10_ms,
            wind_speed_10m_ms = EXCLUDED.wind_speed_10m_ms,
            wind_from_deg = EXCLUDED.wind_from_deg,
            relative_humidity_2m_percent = EXCLUDED.relative_humidity_2m_percent,
            surface_solar_radiation_downwards_j_m2 = EXCLUDED.surface_solar_radiation_downwards_j_m2,
            downward_irradiation_w_m2 = EXCLUDED.downward_irradiation_w_m2,
            surface_pressure_pa = EXCLUDED.surface_pressure_pa,
            boundary_layer_height_m = EXCLUDED.boundary_layer_height_m,
            era5_source_id = EXCLUDED.era5_source_id,
            updated_at = NOW();
    """
    template = "(" + ",".join(["%s"] * 15) + ",NOW())"
    with conn.cursor() as cur:
        execute_values(cur, query, records, template=template, page_size=batch_size)
    conn.commit()
    return len(records)


def main():
    args = parse_args()
    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")
    start = parse_utc(args.start)
    end = parse_utc(args.end)
    if start is not None and end is not None and end <= start:
        raise ValueError("--end must be later than --start")

    psycopg2, execute_values, load_dotenv, xr = require_dependencies()
    files = discover_files(args.input_dir, args.region)
    conn = connect_database(args.database, psycopg2, load_dotenv)
    try:
        if not args.dry_run:
            ensure_era5_table(conn)
        sites = fetch_sites(conn, args.site_ids)
        requested = {safe_region_name(value) for value in args.region or []}
        if requested:
            sites = [site for site in sites if site["region_slug"] in requested]

        sites_by_region = {}
        for site in sites:
            sites_by_region.setdefault(site["region_slug"], []).append(site)

        total = 0
        for region_slug, path in files:
            region_sites = sites_by_region.get(region_slug, [])
            if not region_sites:
                print("Skipping %s: no database sites match region '%s'" % (path, region_slug))
                continue
            print("Reading %s for %i sites" % (path, len(region_sites)))
            with ExitStack() as stack:
                datasets = open_archive_datasets(path, xr, stack)
                file_records = []
                for site in region_sites:
                    frame = combine_site_streams(datasets, site)
                    file_records.extend(
                        records_for_site(frame, site["site_id"], path.name, start, end)
                    )
            if args.dry_run:
                print("Dry run: extracted %i site-hour rows" % len(file_records))
            else:
                count = upsert_records(conn, file_records, execute_values, args.batch_size)
                total += count
                print("Upserted %i site-hour rows" % count)

        if args.dry_run:
            print("Dry run complete; database was not modified.")
        else:
            print("Complete: upserted %i ERA5 site-hour rows into %s." % (total, args.database))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
