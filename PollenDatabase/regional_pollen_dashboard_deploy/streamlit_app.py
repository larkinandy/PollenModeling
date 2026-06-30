### streamlit_app.py
### Summary: Deployable Streamlit dashboard using bundled pollen Parquet data.

from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


SCRIPT_PATH = Path(__file__).resolve()
DATA_DIR = SCRIPT_PATH.parent / "data"
SITES_FILE = DATA_DIR / "sites.parquet"
POLLEN_FILE = DATA_DIR / "pollen_hourly.parquet"
METEOROLOGY_FILE = DATA_DIR / "era5_hourly_site.parquet"
DEFAULT_REGION = "Ann Arbor"
SPRING_ONSET_GDD_THRESHOLD = 50.0
RAIN_WASHOUT_PRECIP_MM = 0.1
REGION_TIMEZONES = {
    "Ann Arbor": "America/Detroit",
    "Baltimore": "America/New_York",
    "Los Angeles": "America/Los_Angeles",
    "New York City": "America/New_York",
    "Salt Lake City": "America/Denver",
    "San Francisco": "America/Los_Angeles",
    "Winston-Salem": "America/New_York",
}

POLLEN_CATEGORY_CODES = [
    ("Total Pollen", "POL"),
    ("Total Tree Pollen", "TRE"),
    ("Total Grass Pollen", "GRA"),
    ("Total Weed/Shrub Pollen", "WEE"),
    ("Total Mold", "MOL"),
    ("Other Particulate", "OTHPAR"),
    ("Quercus (Oak)", "QUE"),
    ("Acer (Maple)", "ACE"),
    ("Betula (Birch)", "BET"),
    ("Ulmus (Elm)", "ULM"),
    ("Fraxinus (Ash)", "FRA"),
    ("Populus (Poplar)", "POP"),
    ("Pinaceae (Pine)", "PIN"),
]

METEOROLOGY_VARIABLES = {
    "Pollen concentration": ("concentration", "Pollen concentration"),
    "Temperature": ("temperature_2m_c", "Temperature, C"),
    "Relative humidity": ("relative_humidity_percent", "Relative humidity, %"),
    "Downward irradiation": ("downward_irradiation_w_m2", "Downward irradiation, W/m2"),
    "Precipitation": ("precipitation_mm", "Precipitation, mm"),
    "Wind speed": ("wind_speed_10m_ms", "Wind speed, m/s"),
    "Growing degree days": ("cumulative_gdd_c_day", "Cumulative GDD, C-day"),
    "Chilling degree days": ("cumulative_chilling_c_day", "Cumulative chilling, C-day"),
    "Cumulative precipitation": ("cumulative_precipitation_mm", "Cumulative precipitation, mm"),
    "Cumulative solar radiation": (
        "cumulative_solar_radiation_kwh_m2",
        "Cumulative solar radiation, kWh/m2",
    ),
    "Mean daytime radiation": ("mean_daytime_radiation_w_m2", "Mean daytime radiation, W/m2"),
}

WIND_SPEED_ORDER = ["0-1 m/s", "1-2 m/s", "2-4 m/s", "4-6 m/s", "6+ m/s"]


def normalize_moment_column(df):
    if "moment" in df.columns:
        df = df.copy()
        df["moment"] = pd.to_datetime(df["moment"], utc=True)
    return df


@st.cache_data(show_spinner=False)
def load_sites():
    if not SITES_FILE.exists():
        raise RuntimeError("Missing deploy data file: %s" % SITES_FILE)
    sites = pd.read_parquet(SITES_FILE)
    if "last_updated" in sites.columns:
        sites["last_updated"] = pd.to_datetime(sites["last_updated"], utc=True)
    return sites


@st.cache_data(show_spinner=False)
def load_pollen():
    if not POLLEN_FILE.exists():
        raise RuntimeError("Missing deploy data file: %s" % POLLEN_FILE)
    return normalize_moment_column(pd.read_parquet(POLLEN_FILE))


@st.cache_data(show_spinner=False)
def load_meteorology():
    if not METEOROLOGY_FILE.exists():
        raise RuntimeError("Missing deploy data file: %s" % METEOROLOGY_FILE)
    return normalize_moment_column(pd.read_parquet(METEOROLOGY_FILE))


def load_regions():
    sites = load_sites()
    return sorted(sites["study_region_name"].dropna().unique().tolist())


@st.cache_data(show_spinner=False)
def load_regional_data(region_name):
    sites = load_sites()
    pollen = load_pollen()
    meteorology = load_meteorology()
    sites = sites[sites["study_region_name"] == region_name].copy()
    pollen = pollen[pollen["study_region_name"] == region_name].copy()
    meteorology = meteorology[meteorology["study_region_name"] == region_name].copy()

    if sites.empty:
        raise RuntimeError("No sites were found for region '%s'." % region_name)
    if pollen.empty:
        raise RuntimeError("No hourly pollen records were found for region '%s'." % region_name)
    if meteorology.empty:
        raise RuntimeError("No ERA5 records were found for region '%s'." % region_name)

    return sites, pollen, meteorology


def local_timezone_for_region(region_name):
    return REGION_TIMEZONES.get(region_name, "UTC")


def apply_period(df, frequency, timezone_name="UTC"):
    df = df.copy()
    if frequency == "Daily":
        df["period"] = df["moment"].dt.floor("D")
    elif frequency == "Hour of day":
        df["period"] = df["moment"].dt.tz_convert(timezone_name).dt.hour
    else:
        df["period"] = df["moment"].dt.floor("h")
    return df


def aggregate_regional_timeseries(pollen, category, included_sites, frequency, timezone_name="UTC"):
    pollen_subset = pollen[
        (pollen["site_id"].isin(included_sites)) & (pollen["category"] == category)
    ].copy()

    pollen_subset = apply_period(pollen_subset, frequency, timezone_name)

    pollen_region = (
        pollen_subset
        .groupby("period", as_index=False)
        .agg(
            pcount=("pcount", "sum"),
            cubic_meters=("cubic_meters", "sum"),
            pollen_site_count=("site_id", "nunique"),
        )
    )
    pollen_region["concentration"] = pollen_region["pcount"] / pollen_region["cubic_meters"].replace(0, np.nan)

    return pollen_region.sort_values("period")


def aggregate_site_summary(pollen, category, included_sites, frequency, timezone_name="UTC"):
    pollen_subset = pollen[
        (pollen["site_id"].isin(included_sites)) & (pollen["category"] == category)
    ].copy()

    pollen_subset = apply_period(pollen_subset, frequency, timezone_name)

    pollen_site = (
        pollen_subset
        .groupby(["site_id", "period"], as_index=False)
        .agg(pcount=("pcount", "sum"), cubic_meters=("cubic_meters", "sum"))
    )
    pollen_site["concentration"] = pollen_site["pcount"] / pollen_site["cubic_meters"].replace(0, np.nan)
    return pollen_site


def aggregate_regional_from_site_summary(site_period):
    if site_period.empty:
        return pd.DataFrame(columns=["period", "pcount", "cubic_meters", "pollen_site_count", "concentration"])

    regional = (
        site_period
        .groupby("period", as_index=False)
        .agg(
            pcount=("pcount", "sum"),
            cubic_meters=("cubic_meters", "sum"),
            pollen_site_count=("site_id", "nunique"),
        )
    )
    regional["concentration"] = regional["pcount"] / regional["cubic_meters"].replace(0, np.nan)
    return regional.sort_values("period")


def non_pollen_particulate_concentration(pollen, included_sites):
    non_pollen_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & pollen["category"].isin(["Total Mold", "Other Particulate"])
        & pollen["moment"].notna()
    ].copy()
    if non_pollen_rows.empty:
        return pd.DataFrame(columns=["site_id", "moment", "non_pollen_particulate_concentration"])

    hourly = (
        non_pollen_rows
        .groupby(["site_id", "moment"], as_index=False)
        .agg(
            non_pollen_pcount=("pcount", "sum"),
            cubic_meters=("cubic_meters", "max"),
        )
    )
    hourly["non_pollen_particulate_concentration"] = (
        hourly["non_pollen_pcount"] / hourly["cubic_meters"].replace(0, np.nan)
    )
    return hourly[["site_id", "moment", "non_pollen_particulate_concentration"]]


@st.cache_data(show_spinner=False)
def add_rain_washout_to_pollen(pollen, meteorology, rain_washout_hours):
    pollen = pollen.copy()
    if rain_washout_hours <= 0 or meteorology.empty:
        pollen["rain_washout_precip_mm"] = 0.0
        pollen["rain_washout"] = False
        return pollen

    met_precip = meteorology[["site_id", "moment", "precipitation_m"]].copy()
    met_precip["precipitation_mm"] = met_precip["precipitation_m"].fillna(0.0) * 1000.0
    washout_frames = []
    for site_id, site_rows in met_precip.groupby("site_id", sort=False):
        site_rows = site_rows.sort_values("moment").copy()
        site_rows["rain_washout_precip_mm"] = (
            site_rows["precipitation_mm"]
            .shift(1)
            .rolling(rain_washout_hours, min_periods=1)
            .sum()
            .fillna(0.0)
        )
        washout_frames.append(site_rows[["site_id", "moment", "rain_washout_precip_mm"]])
    washout = pd.concat(washout_frames, ignore_index=True)
    washout["rain_washout"] = washout["rain_washout_precip_mm"] >= RAIN_WASHOUT_PRECIP_MM
    pollen = pollen.merge(washout, on=["site_id", "moment"], how="left")
    pollen["rain_washout_precip_mm"] = pollen["rain_washout_precip_mm"].fillna(0.0)
    pollen["rain_washout"] = pollen["rain_washout"].fillna(False)
    return pollen


def prepare_qa_hourly_records(pollen, category, included_sites, max_non_pollen_particulate):
    category_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & (pollen["category"] == category)
        & pollen["moment"].notna()
        & pollen["concentration"].notna()
    ].copy()
    if category_rows.empty:
        return category_rows

    non_pollen_rows = non_pollen_particulate_concentration(pollen, included_sites)
    category_rows = category_rows.merge(non_pollen_rows, on=["site_id", "moment"], how="left")
    if "rain_washout" not in category_rows.columns:
        category_rows["rain_washout"] = False
    category_rows["day"] = category_rows["moment"].dt.floor("D")
    category_rows["qa_eligible_hour"] = (
        category_rows["non_pollen_particulate_concentration"] <= max_non_pollen_particulate
    ) & (~category_rows["rain_washout"])
    return category_rows


def valid_site_days(
    pollen,
    category,
    included_sites,
    valid_day_hours_cutoff,
    max_non_pollen_particulate,
    qa_hourly=None,
):
    category_rows = (
        qa_hourly.copy()
        if qa_hourly is not None
        else prepare_qa_hourly_records(pollen, category, included_sites, max_non_pollen_particulate)
    )
    if category_rows.empty:
        return pd.DataFrame(columns=["site_id", "day", "valid_hours", "qa_valid_day"])

    category_rows = category_rows[category_rows["qa_eligible_hour"]].copy()
    if category_rows.empty:
        return pd.DataFrame(columns=["site_id", "day", "valid_hours", "qa_valid_day"])

    qa = (
        category_rows
        .groupby(["site_id", "day"], as_index=False)
        .agg(valid_hours=("moment", "nunique"))
    )
    qa["qa_valid_day"] = qa["valid_hours"] >= valid_day_hours_cutoff
    return qa


def apply_qa_threshold_to_pollen(
    pollen,
    category,
    included_sites,
    valid_day_hours_cutoff,
    max_non_pollen_particulate,
    qa_days=None,
):
    qa = (
        qa_days
        if qa_days is not None
        else valid_site_days(
            pollen,
            category,
            included_sites,
            valid_day_hours_cutoff,
            max_non_pollen_particulate,
        )
    )
    valid_days = qa[qa["qa_valid_day"]][["site_id", "day"]]
    if valid_days.empty:
        return pollen.iloc[0:0].copy()

    pollen_with_day = pollen.copy()
    pollen_with_day["day"] = pollen_with_day["moment"].dt.floor("D")
    filtered = pollen_with_day.merge(valid_days, on=["site_id", "day"], how="inner")
    return filtered.drop(columns=["day"])


def rolling_series(series, window):
    if window <= 1:
        return series
    return series.rolling(window=window, min_periods=1, center=True).mean()


def make_pollen_timeseries(regional, site_period, sites, category, rolling_window, frequency, timezone_name="UTC"):
    plot_data = regional.dropna(subset=["concentration"]).copy()
    plot_data["smoothed_concentration"] = rolling_series(plot_data["concentration"], rolling_window)

    fig = go.Figure()
    site_plot = site_period.dropna(subset=["concentration"]).merge(
        sites[["site_id", "name"]],
        on="site_id",
        how="left",
    )
    for site_name, site_rows in site_plot.groupby("name", sort=True):
        site_rows = site_rows.sort_values("period")
        fig.add_trace(
            go.Scatter(
                x=site_rows["period"],
                y=site_rows["concentration"],
                mode="lines",
                line=dict(width=1),
                opacity=0.35,
                name=site_name,
                legendgroup="sites",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=plot_data["period"],
            y=plot_data["concentration"],
            mode="lines",
            line=dict(color="rgba(31, 119, 180, 0.55)", width=2),
            name="Regional concentration",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=plot_data["period"],
            y=plot_data["smoothed_concentration"],
            mode="lines",
            line=dict(color="#1f77b4", width=3),
            name="Smoothed trend",
        )
    )
    fig.update_layout(
        title="%s time series" % category,
        height=430,
        xaxis_title="Hour of day, %s" % timezone_name if frequency == "Hour of day" else "Time",
        yaxis_title="Concentration",
        legend=dict(orientation="h"),
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def make_site_comparison(site_period, sites, category):
    if site_period.empty:
        fig = go.Figure()
        fig.update_layout(title="No monitor records match current filters")
        return fig

    site_period = site_period.dropna(subset=["concentration"]).merge(
        sites[["site_id", "name"]],
        on="site_id",
        how="left",
    )
    if site_period.empty:
        fig = go.Figure()
        fig.update_layout(title="No monitor records pass the QA threshold")
        return fig

    summary = (
        site_period
        .groupby(["site_id", "name"], as_index=False)
        .agg(
            median_concentration=("concentration", "median"),
        )
    )
    site_order = summary.sort_values("median_concentration", ascending=False)["name"].tolist()

    fig = px.box(
        site_period,
        x="name",
        y="concentration",
        points="outliers",
        category_orders={"name": site_order},
        labels={
            "name": "Monitor",
            "concentration": "Concentration",
        },
        title="Site-specific %s distribution" % category,
        height=380,
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=55, b=80),
        xaxis_tickangle=-30,
    )
    return fig


@st.cache_data(show_spinner=False)
def monitor_health_summary(pollen, sites, included_sites, max_non_pollen_particulate):
    health_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & pollen["category"].isin(["Total Pollen", "Total Mold", "Other Particulate"])
        & pollen["moment"].notna()
    ].copy()
    if health_rows.empty:
        return pd.DataFrame()

    latest_moment = health_rows["moment"].max()
    site_order = sites[sites["site_id"].isin(included_sites)][["site_id", "name"]].copy()
    summaries = []

    for label, days in [("Past 7 days", 7)]:
        start = latest_moment - pd.Timedelta(days=days)
        window = health_rows[(health_rows["moment"] > start) & (health_rows["moment"] <= latest_moment)].copy()
        rain_window = pollen[
            (pollen["site_id"].isin(included_sites))
            & (pollen["moment"] > start)
            & (pollen["moment"] <= latest_moment)
            & (pollen["category"] == "Total Pollen")
        ].copy()
        rain_summary = (
            rain_window.groupby("site_id", as_index=False).agg(rain_washout_hours=("rain_washout", "sum"))
            if "rain_washout" in rain_window.columns
            else site_order[["site_id"]].assign(rain_washout_hours=0)
        )
        if window.empty:
            summary = site_order[["site_id"]].copy()
            summary["avg_flow_m3_hr"] = np.nan
            summary["median_flow_m3_hr"] = np.nan
            summary["valid_hours"] = 0
            summary["cumulative_particulate_load"] = 0.0
            summary["cumulative_non_pollen_load"] = 0.0
            summary["avg_particulate_concentration"] = np.nan
            summary["p95_non_pollen_concentration"] = np.nan
            summary["hours_above_qa_particulate_threshold"] = 0
            summary["last_measurement"] = pd.NaT
        else:
            hourly = (
                window
                .pivot_table(
                    index=["site_id", "moment"],
                    columns="category",
                    values=["pcount", "cubic_meters"],
                    aggfunc={"pcount": "sum", "cubic_meters": "max"},
                )
            )
            hourly.columns = ["_".join(col).strip() for col in hourly.columns.to_flat_index()]
            hourly = hourly.reset_index()

            for column in [
                "pcount_Total Pollen",
                "pcount_Total Mold",
                "pcount_Other Particulate",
                "cubic_meters_Total Pollen",
                "cubic_meters_Total Mold",
                "cubic_meters_Other Particulate",
            ]:
                if column not in hourly.columns:
                    hourly[column] = 0.0

            hourly["flow_m3"] = hourly[
                ["cubic_meters_Total Pollen", "cubic_meters_Total Mold", "cubic_meters_Other Particulate"]
            ].max(axis=1)
            hourly["total_particulate_count"] = (
                hourly["pcount_Total Pollen"]
                + hourly["pcount_Total Mold"]
                + hourly["pcount_Other Particulate"]
            )
            hourly["non_pollen_particulate_count"] = (
                hourly["pcount_Total Mold"] + hourly["pcount_Other Particulate"]
            )
            hourly["total_particulate_concentration"] = (
                hourly["total_particulate_count"] / hourly["flow_m3"].replace(0, np.nan)
            )
            hourly["non_pollen_particulate_concentration"] = (
                hourly["non_pollen_particulate_count"] / hourly["flow_m3"].replace(0, np.nan)
            )
            hourly["above_qa_particulate_threshold"] = (
                hourly["non_pollen_particulate_concentration"] > max_non_pollen_particulate
            )

            summary = (
                hourly
                .groupby("site_id", as_index=False)
                .agg(
                    avg_flow_m3_hr=("flow_m3", "mean"),
                    median_flow_m3_hr=("flow_m3", "median"),
                    valid_hours=("moment", "nunique"),
                    cumulative_particulate_load=("total_particulate_count", "sum"),
                    cumulative_non_pollen_load=("non_pollen_particulate_count", "sum"),
                    avg_particulate_concentration=("total_particulate_concentration", "mean"),
                    p95_non_pollen_concentration=("non_pollen_particulate_concentration", lambda x: x.quantile(0.95)),
                    hours_above_qa_particulate_threshold=("above_qa_particulate_threshold", "sum"),
                    last_measurement=("moment", "max"),
                )
            )
            summary = site_order[["site_id"]].merge(summary, on="site_id", how="left")
            fill_zero_columns = [
                "valid_hours",
                "cumulative_particulate_load",
                "cumulative_non_pollen_load",
                "hours_above_qa_particulate_threshold",
            ]
            summary[fill_zero_columns] = summary[fill_zero_columns].fillna(0)

        summary = summary.merge(rain_summary, on="site_id", how="left")
        summary["rain_washout_hours"] = summary["rain_washout_hours"].fillna(0)
        summary["window"] = label
        summary["uptime_pct"] = 100.0 * summary["valid_hours"] / (days * 24)
        summary["hours_since_last"] = (
            (latest_moment - summary["last_measurement"]).dt.total_seconds() / 3600.0
        )
        summaries.append(summary)

    if not summaries:
        return pd.DataFrame()

    status_columns = [
        "site_id",
        "name",
        "sensor_id",
        "status_code",
        "status_message",
        "status_description",
        "mode_description",
        "last_updated",
    ]
    available_status_columns = [column for column in status_columns if column in sites.columns]
    result = pd.concat(summaries, ignore_index=True).merge(
        sites[available_status_columns],
        on="site_id",
        how="left",
    )
    return result


def monitor_health_window_label(pollen, included_sites):
    health_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & pollen["category"].isin(["Total Pollen", "Total Mold", "Other Particulate"])
        & pollen["moment"].notna()
    ]
    if health_rows.empty:
        return "Monitor Health"

    end_date = health_rows["moment"].max()
    start_date = end_date - pd.Timedelta(days=7)
    return "Monitor Health (%s - %s)" % (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )


def make_monitor_health_panel(pollen, sites, included_sites, max_non_pollen_particulate):
    health = monitor_health_summary(pollen, sites, included_sites, max_non_pollen_particulate)
    if health.empty:
        st.info("No monitor health records match current filters.")
        return

    display = health.copy()
    display["avg_flow_m3_hr"] = display["avg_flow_m3_hr"].round(4)
    display["median_flow_m3_hr"] = display["median_flow_m3_hr"].round(4)
    display["uptime_pct"] = display["uptime_pct"].round(1)
    display["cumulative_particulate_load"] = display["cumulative_particulate_load"].round(0).astype("Int64")
    display["cumulative_non_pollen_load"] = display["cumulative_non_pollen_load"].round(0).astype("Int64")
    display["avg_particulate_concentration"] = display["avg_particulate_concentration"].round(1)
    display["p95_non_pollen_concentration"] = display["p95_non_pollen_concentration"].round(1)
    display["hours_above_qa_particulate_threshold"] = (
        display["hours_above_qa_particulate_threshold"].fillna(0).round(0).astype("Int64")
    )
    display["rain_washout_hours"] = display["rain_washout_hours"].fillna(0).round(0).astype("Int64")
    display["hours_since_last"] = display["hours_since_last"].round(1)
    def clean_status_value(value):
        if pd.isna(value):
            return None
        text = str(value).strip()
        if not text or text.lower() == "nan":
            return None
        return text

    def format_current_status(row):
        mode = clean_status_value(row.get("mode_description"))
        detail = clean_status_value(row.get("status_description")) or clean_status_value(row.get("status_message"))
        if mode and detail:
            return "%s / %s" % (mode, detail)
        return mode or detail or "Unknown"

    display["current_status"] = display.apply(format_current_status, axis=1)

    display = display[
        [
            "name",
            "current_status",
            "avg_flow_m3_hr",
            "uptime_pct",
            "cumulative_particulate_load",
            "hours_above_qa_particulate_threshold",
            "rain_washout_hours",
        ]
    ].rename(
        columns={
            "name": "Monitor",
            "current_status": "Current status",
            "avg_flow_m3_hr": "Avg flow m3/hr",
            "uptime_pct": "Uptime %",
            "cumulative_particulate_load": "Particulate load",
            "hours_above_qa_particulate_threshold": "Hours above QA threshold",
            "rain_washout_hours": "Rain washout hours",
        }
    )

    st.dataframe(display, use_container_width=True, hide_index=True)


def make_qa_grid(
    pollen,
    sites,
    category,
    included_sites,
    valid_day_hours_cutoff,
    max_non_pollen_particulate,
    qa_hourly=None,
):
    site_order = sites[sites["site_id"].isin(included_sites)][["site_id", "name"]].copy()
    if site_order.empty:
        fig = go.Figure()
        fig.update_layout(title="No monitors match current filters")
        return fig

    category_rows = (
        qa_hourly.copy()
        if qa_hourly is not None
        else prepare_qa_hourly_records(pollen, category, included_sites, max_non_pollen_particulate)
    )
    if category_rows.empty:
        fig = go.Figure()
        fig.update_layout(title="No measurement records match current filters")
        return fig

    rain_counts = (
        category_rows[category_rows["rain_washout"]]
        .groupby(["site_id", "day"], as_index=False)
        .agg(rain_washout_hours=("moment", "nunique"))
    )
    category_rows = category_rows[category_rows["qa_eligible_hour"]].copy()
    if category_rows.empty:
        fig = go.Figure()
        fig.update_layout(title="No measurement records pass the non-pollen particulate cutoff")
        return fig

    all_days = pd.date_range(category_rows["day"].min(), category_rows["day"].max(), freq="D", tz="UTC")

    counts = (
        category_rows
        .groupby(["site_id", "day"], as_index=False)
        .agg(valid_hours=("moment", "nunique"))
    )
    full_index = pd.MultiIndex.from_product(
        [site_order["site_id"], all_days],
        names=["site_id", "day"],
    )
    qa = counts.set_index(["site_id", "day"]).reindex(full_index).reset_index()
    qa["valid_hours"] = qa["valid_hours"].fillna(0).astype(int)
    qa = qa.merge(rain_counts, on=["site_id", "day"], how="left")
    qa["rain_washout_hours"] = qa["rain_washout_hours"].fillna(0).astype(int)
    qa = qa.merge(site_order, on="site_id", how="left")
    qa["qa_code"] = np.select(
        [qa["valid_hours"] >= valid_day_hours_cutoff, qa["valid_hours"] > 0],
        [2, 1],
        default=0,
    )
    qa["status"] = np.select(
        [qa["qa_code"] == 2, qa["qa_code"] == 1],
        ["Valid day", "Partial day"],
        default="No measurements",
    )

    matrix = (
        qa
        .pivot(index="name", columns="day", values="qa_code")
        .reindex(site_order["name"])
    )
    hour_matrix = (
        qa
        .pivot(index="name", columns="day", values="valid_hours")
        .reindex(site_order["name"])
    )
    rain_matrix = qa.pivot(index="name", columns="day", values="rain_washout_hours").reindex(site_order["name"])
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.to_numpy(),
            x=[day.strftime("%Y-%m-%d") for day in matrix.columns],
            y=matrix.index,
            customdata=np.dstack([hour_matrix.to_numpy(), rain_matrix.to_numpy()]),
            colorscale=[
                [0.0, "#d0d5dd"],
                [0.333, "#d0d5dd"],
                [0.334, "#f4c542"],
                [0.666, "#f4c542"],
                [0.667, "#2ca25f"],
                [1.0, "#2ca25f"],
            ],
            zmin=0,
            zmax=2,
            showscale=False,
            hovertemplate=(
                "Monitor=%{y}<br>"
                "Date=%{x}<br>"
                "Valid hours=%{customdata[0]}<br>"
                "Rain washout hours=%{customdata[1]}"
                "<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Measurement QA by monitor and day: valid hours per site-day",
        height=max(320, 38 * len(site_order) + 120),
        xaxis_title="Day",
        yaxis_title="Monitor",
        margin=dict(l=20, r=20, t=55, b=80),
    )
    return fig


def add_seasonal_meteorology_metrics(site_met, timezone_name, gdd_base_c, cumulative_window_days):
    site_met = site_met.sort_values("moment").copy()
    site_met["local_day"] = site_met["moment"].dt.tz_convert(timezone_name).dt.floor("D")
    site_met["solar_radiation_kwh_m2"] = site_met["downward_irradiation_w_m2"].fillna(0.0) / 1000.0
    site_met["daytime_radiation_w_m2"] = site_met["downward_irradiation_w_m2"].where(
        site_met["downward_irradiation_w_m2"] > 20.0
    )
    daily = (
        site_met.groupby("local_day", as_index=False)
        .agg(
            daily_mean_temperature_c=("temperature_2m_c", "mean"),
            daily_precipitation_mm=("precipitation_mm", "sum"),
            daily_solar_radiation_kwh_m2=("solar_radiation_kwh_m2", "sum"),
            mean_daytime_radiation_w_m2=("daytime_radiation_w_m2", "mean"),
        )
    )
    daily["gdd_c_day"] = np.maximum(daily["daily_mean_temperature_c"] - gdd_base_c, 0.0)
    daily["chilling_c_day"] = np.maximum(gdd_base_c - daily["daily_mean_temperature_c"], 0.0)
    daily["cumulative_gdd_c_day"] = daily["gdd_c_day"].cumsum()
    daily["cumulative_chilling_c_day"] = daily["chilling_c_day"].cumsum()
    daily["cumulative_precipitation_mm"] = daily["daily_precipitation_mm"].rolling(
        cumulative_window_days, min_periods=1
    ).sum()
    daily["cumulative_solar_radiation_kwh_m2"] = daily["daily_solar_radiation_kwh_m2"].rolling(
        cumulative_window_days, min_periods=1
    ).sum()
    return site_met.merge(
        daily[
            [
                "local_day",
                "gdd_c_day",
                "cumulative_gdd_c_day",
                "chilling_c_day",
                "cumulative_chilling_c_day",
                "cumulative_precipitation_mm",
                "cumulative_solar_radiation_kwh_m2",
                "mean_daytime_radiation_w_m2",
            ]
        ],
        on="local_day",
        how="left",
    )


def build_meteorology_analysis_table(
    meteorology,
    pollen,
    site_ids,
    category,
    timezone_name,
    gdd_base_c,
    cumulative_window_days,
):
    frames = []
    pollen = normalize_moment_column(pollen)
    for site_id in site_ids:
        site_met = meteorology[meteorology["site_id"] == site_id].copy()
        if site_met.empty:
            continue
        site_pollen = pollen[
            (pollen["site_id"] == site_id) & (pollen["category"] == category)
        ][["moment", "concentration"]].copy()
        site_met["temperature_2m_c"] = site_met["temperature_2m_k"] - 273.15
        site_met["precipitation_mm"] = site_met["precipitation_m"] * 1000.0
        site_met = add_seasonal_meteorology_metrics(
            site_met, timezone_name, gdd_base_c, cumulative_window_days
        )
        frames.append(site_met.merge(site_pollen, on="moment", how="left"))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def append_regional_average_analysis(analysis):
    if analysis.empty:
        return analysis
    numeric_columns = [
        column
        for column in analysis.select_dtypes(include=[np.number]).columns
        if column not in {"site_id", "sensor_id"}
    ]
    regional = analysis.groupby("moment", as_index=False).agg(
        {column: "mean" for column in numeric_columns}
    )
    if "local_day" in analysis.columns:
        local_day = analysis.groupby("moment", as_index=False).agg(local_day=("local_day", "first"))
        regional = regional.merge(local_day, on="moment", how="left")
    regional["site_id"] = "__regional_average__"
    regional["monitor_name"] = "Regional average"
    return pd.concat([regional, analysis], ignore_index=True, sort=False)


def restrict_to_pollen_measurement_window(analysis):
    pollen_rows = analysis.dropna(subset=["concentration"])
    if pollen_rows.empty:
        return analysis
    return analysis[
        analysis["moment"].between(pollen_rows["moment"].min(), pollen_rows["moment"].max())
    ].copy()


def aggregate_meteorology_timeseries(analysis, selected_variables, frequency, timezone_name):
    subset = restrict_to_pollen_measurement_window(analysis)
    available_columns = [
        METEOROLOGY_VARIABLES[name][0]
        for name in selected_variables
        if METEOROLOGY_VARIABLES[name][0] in subset.columns
    ]
    if not available_columns:
        return pd.DataFrame(columns=["period"])
    id_columns = ["monitor_name"] if "monitor_name" in subset.columns else []
    subset = subset[["moment"] + id_columns + available_columns].copy()
    local_moment = subset["moment"].dt.tz_convert(timezone_name)
    if frequency == "Daily":
        subset["period"] = local_moment.dt.floor("D").dt.tz_localize(None)
    elif frequency == "Hour of day":
        subset["period"] = local_moment.dt.hour
    else:
        subset["period"] = local_moment.dt.floor("h").dt.tz_localize(None)
    aggregation = {
        column: "sum" if column == "precipitation_mm" else "mean"
        for column in available_columns
    }
    grouped = subset.groupby(id_columns + ["period"], as_index=False).agg(aggregation).sort_values("period")
    if frequency == "Daily":
        grouped["period_label"] = pd.to_datetime(grouped["period"]).dt.strftime("%Y-%m-%d")
    elif frequency == "Hour of day":
        grouped["period_label"] = grouped["period"]
    else:
        grouped["period_label"] = pd.to_datetime(grouped["period"]).dt.strftime("%Y-%m-%d %H:%M")
    return grouped


def assign_monitor_colors(monitor_names):
    palette = px.colors.qualitative.Safe + px.colors.qualitative.Set2 + px.colors.qualitative.Dark24
    return {
        name: palette[index % len(palette)]
        for index, name in enumerate(sorted(pd.Series(monitor_names).dropna().unique()))
    }


def meteorology_scatter_plot(aggregated, variable_x, variable_y, frequency):
    x_column, x_label = METEOROLOGY_VARIABLES[variable_x]
    y_column, y_label = METEOROLOGY_VARIABLES[variable_y]
    subset = aggregated.dropna(subset=[x_column, y_column]).copy()
    if subset.empty:
        fig = go.Figure()
        fig.update_layout(title="No paired values match current filters")
        return fig
    correlation = subset[x_column].corr(subset[y_column])
    r_label = "n/a" if pd.isna(correlation) else "%.2f" % correlation
    fig = px.scatter(
        subset,
        x=x_column,
        y=y_column,
        color="monitor_name",
        render_mode="svg",
        color_discrete_map=assign_monitor_colors(subset["monitor_name"]),
        hover_data=["period"],
        labels={x_column: x_label, y_column: y_label, "monitor_name": "Monitor"},
        title="%s vs %s (%s, Pearson r=%s)" % (variable_y, variable_x, frequency.lower(), r_label),
        height=430,
    )
    fig.update_layout(margin=dict(l=20, r=20, t=55, b=20))
    return fig


def meteorology_timeseries_plot(aggregated, selected_variables, frequency, timezone_name, spring_onset):
    fig = go.Figure()
    colors = assign_monitor_colors(aggregated["monitor_name"])
    dashes = ["solid", "dot"]
    for name, color in colors.items():
        fig.add_trace(go.Scatter(x=[], y=[], mode="lines", line=dict(color=color, width=3), name=name))
    for index, variable_name in enumerate(selected_variables):
        column, label = METEOROLOGY_VARIABLES[variable_name]
        fig.add_trace(
            go.Scatter(x=[], y=[], mode="lines", line=dict(color="#444444", dash=dashes[index]), name=variable_name)
        )
        yaxis_name = "y" if index == 0 else "y2"
        for monitor_name, rows in aggregated.groupby("monitor_name", sort=True):
            fig.add_trace(
                go.Scatter(
                    x=rows["period_label"],
                    y=rows[column],
                    mode="lines",
                    line=dict(color=colors[monitor_name], dash=dashes[index]),
                    yaxis=yaxis_name,
                    showlegend=False,
                )
            )
        axis_key = "yaxis" if index == 0 else "yaxis2"
        axis = dict(title=label)
        if index == 1:
            axis.update(overlaying="y", side="right", showgrid=False)
        fig.update_layout(**{axis_key: axis})
    fig.update_layout(
        title="Meteorology time series (%s)" % frequency.lower(),
        height=430,
        xaxis_title="Hour of day, %s" % timezone_name if frequency == "Hour of day" else "Time",
        legend=dict(orientation="h"),
        margin=dict(l=20, r=20, t=55, b=20),
    )
    if frequency != "Hour of day" and not pd.isna(spring_onset):
        spring_x = pd.Timestamp(spring_onset).strftime("%Y-%m-%d %H:%M" if frequency == "Hourly" else "%Y-%m-%d")
        fig.add_shape(type="line", x0=spring_x, x1=spring_x, y0=0, y1=1, xref="x", yref="paper", line=dict(color="#2ca25f", width=2, dash="dash"))
        fig.add_annotation(x=spring_x, y=1, xref="x", yref="paper", text="Spring onset", showarrow=False, xanchor="left", yanchor="bottom", font=dict(color="#2ca25f"))
    return fig


def wind_rose_plot(analysis):
    subset = analysis.dropna(subset=["wind_from_deg", "wind_speed_10m_ms"]).copy()
    if subset.empty:
        fig = go.Figure()
        fig.update_layout(title="No wind observations match current filters")
        return fig
    subset["sector"] = (np.floor(subset["wind_from_deg"] / 15.0) * 15.0).astype(int)
    subset["speed_group"] = pd.cut(
        subset["wind_speed_10m_ms"],
        bins=[0, 1, 2, 4, 6, np.inf],
        labels=WIND_SPEED_ORDER,
        include_lowest=True,
    )
    grouped = subset.dropna(subset=["speed_group"]).groupby(["sector", "speed_group"], observed=False, as_index=False).size()
    grouped = grouped.rename(columns={"size": "hour_count"})
    grouped = grouped[grouped["hour_count"] > 0]
    fig = px.bar_polar(
        grouped,
        r="hour_count",
        theta="sector",
        color="speed_group",
        category_orders={"speed_group": WIND_SPEED_ORDER},
        color_discrete_sequence=px.colors.sequential.Blues[2:7],
        title="Wind direction by speed",
    )
    fig.update_layout(barmode="stack", height=380, margin=dict(l=20, r=20, t=60, b=20))
    return fig


def meteorology_correlation_matrix(aggregated):
    columns = {
        "Pollen": "concentration",
        "Temperature": "temperature_2m_c",
        "Relative humidity": "relative_humidity_percent",
        "Irradiation": "downward_irradiation_w_m2",
        "Precipitation": "precipitation_mm",
        "Wind speed": "wind_speed_10m_ms",
        "GDD": "cumulative_gdd_c_day",
        "Chilling": "cumulative_chilling_c_day",
        "Cum. precip": "cumulative_precipitation_mm",
        "Cum. solar": "cumulative_solar_radiation_kwh_m2",
        "Daytime radiation": "mean_daytime_radiation_w_m2",
    }
    available = {label: column for label, column in columns.items() if column in aggregated.columns}
    corr = aggregated[list(available.values())].rename(columns={v: k for k, v in available.items()}).corr()
    if corr.empty:
        fig = go.Figure()
        fig.update_layout(title="No numeric values are available for correlation")
        return fig
    fig = go.Figure(
        go.Heatmap(
            z=corr.to_numpy(),
            x=corr.columns,
            y=corr.index,
            zmin=-1,
            zmax=1,
            colorscale="RdBu",
            reversescale=True,
            text=corr.round(2).astype(str).to_numpy(),
            texttemplate="%{text}",
            hovertemplate="%{y} vs %{x}: %{z:.2f}<extra></extra>",
        )
    )
    fig.update_layout(title="Correlation matrix", height=430, margin=dict(l=20, r=20, t=55, b=20))
    return fig


def spring_onset_date(analysis):
    if analysis.empty:
        return pd.NaT
    daily = analysis.dropna(subset=["local_day", "cumulative_gdd_c_day"]).groupby(
        "local_day", as_index=False
    ).agg(cumulative_gdd_c_day=("cumulative_gdd_c_day", "mean"))
    spring = daily[daily["cumulative_gdd_c_day"] >= SPRING_ONSET_GDD_THRESHOLD]
    return spring["local_day"].iloc[0] if not spring.empty else pd.NaT


def require_password():
    try:
        expected_password = st.secrets.get("dashboard_password")
    except Exception:
        expected_password = None

    if not expected_password:
        return True

    if st.session_state.get("authenticated"):
        return True

    st.title("Regional Pollen Dashboard")
    password = st.text_input("Password", type="password")
    if st.button("Enter"):
        if password == expected_password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    return False


def main():
    st.set_page_config(
        page_title="Regional Pollen Dashboard",
        layout="wide",
    )

    if not require_password():
        st.stop()

    st.title("Regional Pollen Dashboard")
    active_view = st.radio(
        "Dashboard view",
        options=["Trends", "Meteorology"],
        horizontal=True,
        label_visibility="collapsed",
    )

    try:
        regions = load_regions()
    except Exception as exc:
        st.error("Could not load dashboard regions: %s" % exc)
        return

    if not regions:
        st.error("No study regions were found in the deploy data.")
        return

    default_region_index = regions.index(DEFAULT_REGION) if DEFAULT_REGION in regions else 0

    with st.sidebar:
        st.header("Regional Controls")
        region_name = st.selectbox("Study region", options=regions, index=default_region_index)
        region_timezone = local_timezone_for_region(region_name)

    try:
        sites, pollen, meteorology = load_regional_data(region_name)
    except Exception as exc:
        st.error("Could not load regional dashboard data: %s" % exc)
        return

    site_name_lookup = dict(zip(sites["name"], sites["site_id"]))

    with st.sidebar:
        site_names = list(site_name_lookup.keys())
        included_site_names = st.multiselect(
            "Included monitors",
            options=site_names,
            default=site_names,
            help="Keep all selected for regional analysis, or temporarily remove a monitor to check sensitivity.",
        )

        if not included_site_names:
            st.warning("Select at least one monitor.")
            return

        category_options = [category for category, _code in POLLEN_CATEGORY_CODES]
        category = st.selectbox(
            "Pollen series",
            options=category_options,
            index=category_options.index("Total Tree Pollen")
            if "Total Tree Pollen" in category_options
            else 0,
        )
        frequency = st.radio("Time aggregation", options=["Hourly", "Daily", "Hour of day"], index=1, horizontal=True)
        rolling_window = st.slider(
            "Smoothing window",
            min_value=1,
            max_value=21,
            value=3 if frequency == "Daily" else 6,
            step=1,
        )

        available_dates = pollen["moment"].dropna()
        min_date = available_dates.min().date()
        max_date = available_dates.max().date()
        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        st.subheader("QA")
        valid_day_hours_cutoff = st.slider(
            "Valid day cutoff, hours",
            min_value=1,
            max_value=24,
            value=12,
            step=1,
        )
        max_non_pollen_particulate = st.slider(
            "Max non-pollen particulate concentration",
            min_value=0,
            max_value=500000,
            value=150000,
            step=5000,
            format="%d",
            help="Counts per cubic meter from Total Mold plus Other Particulate. Hours above this cutoff do not count toward a valid day.",
        )
        rain_washout_hours = st.slider(
            "Rain washout lookback, hours",
            min_value=0,
            max_value=24,
            value=6,
            step=1,
            help="Exclude pollen records when at least 0.1 mm fell during the preceding hours.",
        )

        if active_view == "Meteorology":
            st.subheader("Meteorology")
            monitor_options = ["Regional average"] + included_site_names
            meteorology_monitor_names = st.multiselect(
                "Meteorology monitors",
                options=monitor_options,
                default=["Regional average"],
            )
            if not meteorology_monitor_names:
                st.warning("Select at least one meteorology monitor or the regional average.")
                return
            gdd_base_c = st.radio("GDD base, C", options=[5, 10], index=0, horizontal=True)
            cumulative_window_days = st.radio(
                "Cumulative precipitation and solar window, days",
                options=[7, 14, 30],
                index=1,
                horizontal=True,
            )

    included_sites = [site_name_lookup[name] for name in included_site_names]
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_ts = pd.Timestamp(date_range[0], tz="UTC")
        end_ts = pd.Timestamp(date_range[1], tz="UTC") + pd.Timedelta(days=1)
        pollen = pollen[(pollen["moment"] >= start_ts) & (pollen["moment"] < end_ts)].copy()
        meteorology = meteorology[
            (meteorology["moment"] >= start_ts) & (meteorology["moment"] < end_ts)
        ].copy()

    pollen = add_rain_washout_to_pollen(pollen, meteorology, rain_washout_hours)

    qa_hourly = prepare_qa_hourly_records(
        pollen,
        category,
        included_sites,
        max_non_pollen_particulate,
    )
    qa_days = valid_site_days(
        pollen,
        category,
        included_sites,
        valid_day_hours_cutoff,
        max_non_pollen_particulate,
        qa_hourly=qa_hourly,
    )
    qa_filtered_pollen = apply_qa_threshold_to_pollen(
        pollen=pollen,
        category=category,
        included_sites=included_sites,
        valid_day_hours_cutoff=valid_day_hours_cutoff,
        max_non_pollen_particulate=max_non_pollen_particulate,
        qa_days=qa_days,
    )

    site_period = aggregate_site_summary(
        pollen=qa_filtered_pollen,
        category=category,
        included_sites=included_sites,
        frequency=frequency,
        timezone_name=region_timezone,
    )
    regional = aggregate_regional_from_site_summary(site_period)

    meteorology_analysis = pd.DataFrame()
    spring_onset = pd.NaT
    if active_view == "Meteorology":
        meteorology_analysis = build_meteorology_analysis_table(
            meteorology,
            qa_filtered_pollen,
            included_sites,
            category,
            region_timezone,
            gdd_base_c,
            cumulative_window_days,
        )
        if not meteorology_analysis.empty:
            meteorology_analysis = meteorology_analysis.dropna(subset=["concentration"]).copy()
            monitor_lookup = dict(zip(sites["site_id"], sites["name"]))
            meteorology_analysis["monitor_name"] = meteorology_analysis["site_id"].map(monitor_lookup)
            meteorology_analysis = append_regional_average_analysis(meteorology_analysis)
            meteorology_analysis = meteorology_analysis[
                meteorology_analysis["monitor_name"].isin(meteorology_monitor_names)
            ].copy()
            spring_onset = spring_onset_date(meteorology_analysis)
        if meteorology_analysis.empty:
            st.warning("No paired pollen and ERA5 records match the current filters.")
            return

    pollen_records = regional["concentration"].dropna()
    metric_cols = st.columns(5)
    metric_cols[0].metric("Study region", region_name)
    metric_cols[1].metric("Included monitors", len(included_sites))
    metric_cols[2].metric("Mean %s" % category, "%.1f" % pollen_records.mean() if not pollen_records.empty else "n/a")
    metric_cols[3].metric("Peak %s" % category, "%.1f" % pollen_records.max() if not pollen_records.empty else "n/a")
    if active_view == "Meteorology":
        metric_cols[4].metric(
            "Spring onset",
            pd.Timestamp(spring_onset).strftime("%Y-%m-%d") if not pd.isna(spring_onset) else "Not reached",
            "GDD base %.0f C >= %.0f" % (gdd_base_c, SPRING_ONSET_GDD_THRESHOLD),
        )
    else:
        metric_cols[4].metric(
            "Valid site-days",
            "%i / %i" % (int(qa_days["qa_valid_day"].sum()), len(qa_days)) if not qa_days.empty else "0 / 0",
        )

    if active_view == "Meteorology":
        variable_options = list(METEOROLOGY_VARIABLES.keys())
        variable_cols = st.columns(2)
        with variable_cols[0]:
            variable_x = st.selectbox("Variable 1", options=variable_options, index=0)
        with variable_cols[1]:
            second_options = [option for option in variable_options if option != variable_x]
            default_second = "Growing degree days"
            variable_y = st.selectbox(
                "Variable 2",
                options=second_options,
                index=second_options.index(default_second) if default_second in second_options else 0,
            )
        selected_variables = [variable_x, variable_y]
        aggregated_meteorology = aggregate_meteorology_timeseries(
            meteorology_analysis,
            list(METEOROLOGY_VARIABLES.keys()),
            frequency,
            region_timezone,
        )

        top_left, top_right = st.columns([1.0, 1.2])
        bottom_left, bottom_right = st.columns([1.0, 1.0])
        with top_left:
            st.subheader("Variable Comparison")
            st.plotly_chart(
                meteorology_scatter_plot(aggregated_meteorology, variable_x, variable_y, frequency),
                use_container_width=True,
            )
        with top_right:
            st.subheader("Meteorology Time Series")
            st.plotly_chart(
                meteorology_timeseries_plot(
                    aggregated_meteorology,
                    selected_variables,
                    frequency,
                    region_timezone,
                    spring_onset,
                ),
                use_container_width=True,
            )
        with bottom_left:
            st.subheader("Wind Rose")
            st.plotly_chart(wind_rose_plot(meteorology_analysis), use_container_width=True)
        with bottom_right:
            st.subheader("Correlation Matrix")
            st.plotly_chart(meteorology_correlation_matrix(aggregated_meteorology), use_container_width=True)
        return

    top_left, top_right = st.columns([1.15, 1.0])

    with top_left:
        st.plotly_chart(
            make_pollen_timeseries(regional, site_period, sites, category, rolling_window, frequency, region_timezone),
            use_container_width=True,
        )

    with top_right:
        st.plotly_chart(make_site_comparison(site_period, sites, category), use_container_width=True)

    bottom_left, bottom_right = st.columns([1.0, 1.0])

    with bottom_left:
        st.subheader(monitor_health_window_label(pollen, included_sites))
        make_monitor_health_panel(pollen, sites, included_sites, max_non_pollen_particulate)

    with bottom_right:
        st.plotly_chart(
            make_qa_grid(
                pollen,
                sites,
                category,
                included_sites,
                valid_day_hours_cutoff,
                max_non_pollen_particulate,
                qa_hourly=qa_hourly,
            ),
            use_container_width=True,
        )
        legend_cols = st.columns(3)
        partial_label = (
            "1-%i valid hours" % (valid_day_hours_cutoff - 1)
            if valid_day_hours_cutoff > 1
            else "No partial-day range"
        )
        legend_items = [
            ("#2ca25f", "%i+ valid hours and <= %s non-pollen/m3" % (valid_day_hours_cutoff, f"{max_non_pollen_particulate:,}")),
            ("#f4c542", "%s and <= %s non-pollen/m3" % (partial_label, f"{max_non_pollen_particulate:,}")),
            ("#d0d5dd", "No measurements"),
        ]
        for col, (color, label) in zip(legend_cols, legend_items):
            col.markdown(
                (
                    "<div style='display:flex;align-items:center;gap:0.45rem;'>"
                    "<span style='display:inline-block;width:0.85rem;height:0.85rem;"
                    "border-radius:0.15rem;background:%s;border:1px solid rgba(0,0,0,0.18);'></span>"
                    "<span style='font-size:0.9rem;'>%s</span>"
                    "</div>"
                )
                % (color, label),
                unsafe_allow_html=True,
            )


if __name__ == "__main__":
    main()
