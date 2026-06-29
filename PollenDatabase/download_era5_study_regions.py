"""Download hourly ERA5 single-level NetCDF files for study regions.

The CDS ERA5 single-level product does not expose 2 m relative humidity
directly, so this downloader requests 2 m dewpoint temperature alongside
2 m temperature and can derive a relative_humidity_2m_percent variable.
"""

import argparse
import calendar
import datetime as dt
import os
from pathlib import Path

import pandas as pd

try:
    import cdsapi
except ModuleNotFoundError as exc:
    raise SystemExit(
        "The cdsapi package is required. Install it with: pip install cdsapi"
    ) from exc


DEFAULT_CDSAPIRC = Path("C:/Users/larki/.cdsapirc")
DEFAULT_OUTPUT_DIR = Path("H:/SPIROMICS_ERA5")
CBSA_TABLE = Path(__file__).resolve().parent / "CBSA_Table.csv"

ERA5_SINGLE_LEVEL_VARIABLES = [
    "2m_temperature",
    "2m_dewpoint_temperature",
    "surface_solar_radiation_downwards",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
]

STUDY_REGION_CITY_IDS = {
    "San Francisco": [41860, 42100, 41940, 40900, 46700, 42220],
    "Los Angeles": [31080, 37100, 12540, 40140, 41740],
    "Salt Lake City": [41620, 36260, 25720, 39340],
    "Ann Arbor": [11460, 19820, 33780, 10300, 27100, 29620],
    "Winston-Salem": [20500, 25780, 39580, 41820, 38240, 24660, 15500, 49180],
    "Baltimore": [12580, 23900, 49620, 37980, 20100, 20660, 47900],
    "New York City": [35620, 39100, 14860, 35300, 25540, 35980, 39300, 12100, 45940, 10900, 20700],
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download hourly ERA5 NetCDF files for pollen study regions."
    )
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument(
        "--end-date",
        help=(
            "Inclusive final date to download. Defaults to yesterday UTC for the "
            "requested year, so the script does not ask CDS for future data."
        ),
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cdsapirc", type=Path, default=DEFAULT_CDSAPIRC)
    parser.add_argument("--cbsa-table", type=Path, default=CBSA_TABLE)
    parser.add_argument(
        "--region",
        action="append",
        choices=sorted(STUDY_REGION_CITY_IDS),
        help="Study region to download. Repeat to limit the run. Defaults to all regions.",
    )
    parser.add_argument(
        "--padding-degrees",
        type=float,
        default=0.75,
        help="Latitude/longitude padding around each region's CBSA centroid bounds.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Download files even when a target NetCDF already exists.",
    )
    parser.add_argument(
        "--skip-derived-rh",
        action="store_true",
        help=(
            "Do not add relative_humidity_2m_percent to downloaded files. "
            "By default, this is derived from 2 m temperature and dewpoint "
            "temperature when xarray is installed."
        ),
    )
    return parser.parse_args()


def parse_date(value):
    return dt.date.fromisoformat(value)


def default_end_date(year):
    yesterday = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    year_end = dt.date(year, 12, 31)
    return min(yesterday, year_end)


def month_windows(start_date, end_date):
    current = dt.date(start_date.year, start_date.month, 1)
    while current <= end_date:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_start = max(start_date, current)
        month_end = min(end_date, dt.date(current.year, current.month, last_day))
        if month_start <= month_end:
            yield month_start, month_end
        if current.month == 12:
            current = dt.date(current.year + 1, 1, 1)
        else:
            current = dt.date(current.year, current.month + 1, 1)


def load_region_areas(cbsa_table, padding_degrees):
    cbsa = pd.read_csv(cbsa_table, dtype={"GEOID": int})
    cbsa["INTPTLAT"] = cbsa["INTPTLAT"].astype(float)
    cbsa["INTPTLON"] = cbsa["INTPTLON"].astype(float)

    areas = {}
    for region_name, city_ids in STUDY_REGION_CITY_IDS.items():
        rows = cbsa[cbsa["GEOID"].isin(city_ids)]
        missing = sorted(set(city_ids) - set(rows["GEOID"].tolist()))
        if missing:
            raise ValueError(
                "Missing CBSA rows for %s: %s" % (region_name, ", ".join(map(str, missing)))
            )

        north = rows["INTPTLAT"].max() + padding_degrees
        south = rows["INTPTLAT"].min() - padding_degrees
        west = rows["INTPTLON"].min() - padding_degrees
        east = rows["INTPTLON"].max() + padding_degrees
        areas[region_name] = [
            round(float(north), 4),
            round(float(west), 4),
            round(float(south), 4),
            round(float(east), 4),
        ]
    return areas


def safe_region_name(region_name):
    return region_name.lower().replace("-", "_").replace(" ", "_")


def days_for_request(start_date, end_date):
    return [
        "%02d" % day
        for day in range(start_date.day, end_date.day + 1)
    ]


def build_request(area, start_date, end_date):
    return {
        "product_type": ["reanalysis"],
        "variable": ERA5_SINGLE_LEVEL_VARIABLES,
        "year": ["%04d" % start_date.year],
        "month": ["%02d" % start_date.month],
        "day": days_for_request(start_date, end_date),
        "time": ["%02d:00" % hour for hour in range(24)],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": area,
    }


def find_data_var(dataset, candidates):
    for candidate in candidates:
        if candidate in dataset.data_vars:
            return candidate
    return None


def add_relative_humidity(path):
    try:
        import numpy as np
        import xarray as xr
    except ModuleNotFoundError:
        print(
            "numpy/xarray is not installed; leaving relative humidity derivation "
            "for later: %s" % path
        )
        return

    try:
        with xr.open_dataset(path) as ds:
            ds = ds.load()
    except ValueError as exc:
        print(
            "Could not derive relative humidity because xarray cannot open this "
            "NetCDF with the installed IO backends. The downloaded ERA5 file was "
            "kept unchanged: %s" % path
        )
        print("Install netCDF4 or h5netcdf to enable this step, or run with --skip-derived-rh.")
        print("xarray error: %s" % exc)
        return

    temperature_name = find_data_var(ds, ["t2m", "2m_temperature"])
    dewpoint_name = find_data_var(ds, ["d2m", "2m_dewpoint_temperature"])
    if not temperature_name or not dewpoint_name:
        print(
            "Could not find 2 m temperature and dewpoint fields; leaving "
            "relative humidity derivation for later: %s" % path
        )
        ds.close()
        return

    temperature_c = ds[temperature_name] - 273.15
    dewpoint_c = ds[dewpoint_name] - 273.15
    saturation_vapor_pressure = 6.112 * np.exp(
        (17.67 * temperature_c) / (temperature_c + 243.5)
    )
    actual_vapor_pressure = 6.112 * np.exp(
        (17.67 * dewpoint_c) / (dewpoint_c + 243.5)
    )
    relative_humidity = (100.0 * actual_vapor_pressure / saturation_vapor_pressure).clip(
        min=0.0,
        max=100.0,
    )
    relative_humidity.attrs.update(
        {
            "long_name": "2 m relative humidity derived from ERA5 temperature and dewpoint",
            "units": "%",
            "source": "Derived from 2m_temperature and 2m_dewpoint_temperature",
        }
    )
    ds["relative_humidity_2m_percent"] = relative_humidity

    tmp_path = path.with_name(path.stem + ".tmp.nc")
    ds.to_netcdf(tmp_path)
    ds.close()
    os.replace(tmp_path, path)
    print("Added derived relative_humidity_2m_percent to %s" % path)


def download_region_month(
    client,
    output_dir,
    region_name,
    area,
    start_date,
    end_date,
    overwrite,
    derive_relative_humidity,
):
    region_slug = safe_region_name(region_name)
    target = output_dir / region_slug / (
        "%s_era5_hourly_%04d_%02d.nc" % (region_slug, start_date.year, start_date.month)
    )
    if target.exists() and not overwrite:
        print("Skipping existing file: %s" % target)
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    request = build_request(area, start_date, end_date)
    print(
        "Downloading %s %s to %s with area [N, W, S, E]=%s"
        % (region_name, start_date.strftime("%Y-%m"), target, area)
    )
    client.retrieve("reanalysis-era5-single-levels", request, str(target))
    if derive_relative_humidity:
        add_relative_humidity(target)


def main():
    args = parse_args()
    if args.cdsapirc.exists():
        os.environ.setdefault("CDSAPI_RC", str(args.cdsapirc))
    else:
        raise FileNotFoundError("CDS API config was not found: %s" % args.cdsapirc)

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date) if args.end_date else default_end_date(args.year)
    if start_date.year != args.year or end_date.year != args.year:
        raise ValueError("--start-date and --end-date must stay within --year")
    if end_date < start_date:
        raise ValueError("--end-date must be on or after --start-date")

    region_names = args.region or sorted(STUDY_REGION_CITY_IDS)
    areas = load_region_areas(args.cbsa_table, args.padding_degrees)
    client = cdsapi.Client()

    for region_name in region_names:
        for month_start, month_end in month_windows(start_date, end_date):
            download_region_month(
                client,
                args.output_dir,
                region_name,
                areas[region_name],
                month_start,
                month_end,
                args.overwrite,
                not args.skip_derived_rh,
            )


if __name__ == "__main__":
    main()
