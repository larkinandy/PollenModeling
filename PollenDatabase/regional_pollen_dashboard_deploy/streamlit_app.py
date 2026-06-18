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
DEFAULT_REGION = "Ann Arbor"
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


def load_regions():
    sites = load_sites()
    return sorted(sites["study_region_name"].dropna().unique().tolist())


def load_regional_data(region_name):
    sites = load_sites()
    pollen = load_pollen()
    sites = sites[sites["study_region_name"] == region_name].copy()
    pollen = pollen[pollen["study_region_name"] == region_name].copy()

    if sites.empty:
        raise RuntimeError("No sites were found for region '%s'." % region_name)
    if pollen.empty:
        raise RuntimeError("No hourly pollen records were found for region '%s'." % region_name)

    return sites, pollen


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


def valid_site_days(pollen, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate):
    category_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & (pollen["category"] == category)
        & pollen["moment"].notna()
        & pollen["concentration"].notna()
    ].copy()
    if category_rows.empty:
        return pd.DataFrame(columns=["site_id", "day", "valid_hours", "qa_valid_day"])

    non_pollen_rows = non_pollen_particulate_concentration(pollen, included_sites)
    category_rows = category_rows.merge(non_pollen_rows, on=["site_id", "moment"], how="left")
    category_rows = category_rows[
        category_rows["non_pollen_particulate_concentration"] <= max_non_pollen_particulate
    ].copy()
    if category_rows.empty:
        return pd.DataFrame(columns=["site_id", "day", "valid_hours", "qa_valid_day"])

    category_rows["day"] = category_rows["moment"].dt.floor("D")
    qa = (
        category_rows
        .groupby(["site_id", "day"], as_index=False)
        .agg(valid_hours=("moment", "nunique"))
    )
    qa["qa_valid_day"] = qa["valid_hours"] >= valid_day_hours_cutoff
    return qa


def apply_qa_threshold_to_pollen(pollen, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate):
    qa = valid_site_days(pollen, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate)
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
        ]
    ].rename(
        columns={
            "name": "Monitor",
            "current_status": "Current status",
            "avg_flow_m3_hr": "Avg flow m3/hr",
            "uptime_pct": "Uptime %",
            "cumulative_particulate_load": "Particulate load",
            "hours_above_qa_particulate_threshold": "Hours above QA threshold",
        }
    )

    st.dataframe(display, use_container_width=True, hide_index=True)


def make_qa_grid(pollen, sites, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate):
    site_order = sites[sites["site_id"].isin(included_sites)][["site_id", "name"]].copy()
    if site_order.empty:
        fig = go.Figure()
        fig.update_layout(title="No monitors match current filters")
        return fig

    category_rows = pollen[
        (pollen["site_id"].isin(included_sites))
        & (pollen["category"] == category)
        & pollen["moment"].notna()
        & pollen["concentration"].notna()
    ].copy()
    if category_rows.empty:
        fig = go.Figure()
        fig.update_layout(title="No measurement records match current filters")
        return fig

    non_pollen_rows = non_pollen_particulate_concentration(pollen, included_sites)
    category_rows = category_rows.merge(non_pollen_rows, on=["site_id", "moment"], how="left")
    category_rows = category_rows[
        category_rows["non_pollen_particulate_concentration"] <= max_non_pollen_particulate
    ].copy()
    if category_rows.empty:
        fig = go.Figure()
        fig.update_layout(title="No measurement records pass the non-pollen particulate cutoff")
        return fig

    category_rows["day"] = category_rows["moment"].dt.floor("D")
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
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix.to_numpy(),
            x=[day.strftime("%Y-%m-%d") for day in matrix.columns],
            y=matrix.index,
            customdata=hour_matrix.to_numpy(),
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
                "Valid hours=%{customdata}"
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
        sites, pollen = load_regional_data(region_name)
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

    included_sites = [site_name_lookup[name] for name in included_site_names]
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_ts = pd.Timestamp(date_range[0], tz="UTC")
        end_ts = pd.Timestamp(date_range[1], tz="UTC") + pd.Timedelta(days=1)
        pollen = pollen[(pollen["moment"] >= start_ts) & (pollen["moment"] < end_ts)].copy()

    qa_filtered_pollen = apply_qa_threshold_to_pollen(
        pollen=pollen,
        category=category,
        included_sites=included_sites,
        valid_day_hours_cutoff=valid_day_hours_cutoff,
        max_non_pollen_particulate=max_non_pollen_particulate,
    )

    regional = aggregate_regional_timeseries(
        pollen=qa_filtered_pollen,
        category=category,
        included_sites=included_sites,
        frequency=frequency,
        timezone_name=region_timezone,
    )
    site_period = aggregate_site_summary(
        pollen=qa_filtered_pollen,
        category=category,
        included_sites=included_sites,
        frequency=frequency,
        timezone_name=region_timezone,
    )
    qa_days = valid_site_days(pollen, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate)

    pollen_records = regional["concentration"].dropna()
    metric_cols = st.columns(5)
    metric_cols[0].metric("Study region", region_name)
    metric_cols[1].metric("Included monitors", len(included_sites))
    metric_cols[2].metric("Mean %s" % category, "%.1f" % pollen_records.mean() if not pollen_records.empty else "n/a")
    metric_cols[3].metric("Peak %s" % category, "%.1f" % pollen_records.max() if not pollen_records.empty else "n/a")
    metric_cols[4].metric(
        "Valid site-days",
        "%i / %i" % (int(qa_days["qa_valid_day"].sum()), len(qa_days)) if not qa_days.empty else "0 / 0",
    )

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
            make_qa_grid(pollen, sites, category, included_sites, valid_day_hours_cutoff, max_non_pollen_particulate),
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
