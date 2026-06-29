### fit_ann_arbor_upwind_tree_pollen_model.py
### Summary: Standalone fitter for upwind tree-count distance kernels and hourly tree pollen.

import argparse
import io
import json
import math
import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import psycopg2
except ModuleNotFoundError:
    psycopg2 = None


ANN_ARBOR_SITES = [
    "1CB55387-9B0A-4B0E-A9B5-5C46DFCCD85F",
    "542035FF-1676-40AC-ABC1-94318890B0A3",
    "8ECAE008-6884-4176-8234-CB26E6A8CA42",
    "98A4CFC0-7AA3-4626-9081-06CAE3562B92",
    "9BC52308-6C41-4095-B7E0-B7C68C04FE40",
    "A0991A67-152E-4EED-8559-4458F98284C4",
    "C62F9C47-C84B-448A-8232-FA13769C37B4",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Fit distance relationships between upwind tree "
            "counts and hourly Ann Arbor tree pollen measurements."
        )
    )
    parser.add_argument("--dbname", default="pollen_dashboard")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--output-dir", default="upwind_tree_pollen_fit_outputs")
    parser.add_argument("--site-id", action="append", dest="site_ids")
    parser.add_argument("--source-version-id", type=int)
    parser.add_argument("--min-pcount", type=float, default=10.0)
    parser.add_argument(
        "--temporal-resolution",
        choices=["hourly", "daily"],
        default="hourly",
        help=(
            "Hourly fits each qualifying measurement. Daily uses mean daily tree pollen "
            "and summed hourly upwind tree-count bins within each site-day."
        ),
    )
    parser.add_argument(
        "--interaction-mode",
        choices=["separable", "lag_distance_surface"],
        default="separable",
        help=(
            "separable searches one lag/kernel with one distance curve. "
            "lag_distance_surface fits one constrained distance curve per searched lag hour."
        ),
    )
    parser.add_argument("--max-lag-hours", type=int, default=24)
    parser.add_argument(
        "--lag-hour",
        action="append",
        type=int,
        dest="lag_hours",
        help="Specific lag hour to search. Repeat to search a subset instead of 0..max-lag-hours.",
    )
    parser.add_argument(
        "--lag-kernel",
        action="append",
        choices=["exact", "gaussian", "monotone_decreasing", "monotone_increasing"],
        help=(
            "Lag kernel to search. Repeat to search a subset. If omitted, all "
            "available kernels are included in the autotune grid."
        ),
    )
    parser.add_argument(
        "--lag-window-hours",
        type=int,
        default=3,
        help="Window used by gaussian and monotone lag kernels.",
    )
    parser.add_argument(
        "--lag-window-direction",
        action="append",
        choices=["older", "recent", "centered"],
        help=(
            "Direction for non-exact lag windows. Repeat to search a subset. "
            "If omitted, older, recent, and centered windows are included in the autotune grid."
        ),
    )
    parser.add_argument("--max-sector-width-deg", type=int, default=45)
    parser.add_argument("--sector-step-deg", type=int, default=5)
    parser.add_argument(
        "--wind-direction-mode",
        action="append",
        choices=["upwind", "all_directions"],
        help=(
            "Whether tree exposure is restricted to the upwind sector or summed across "
            "all bearings. Repeat to search a subset. If omitted, both modes are included."
        ),
    )
    parser.add_argument("--max-distance-m", type=float, default=5000.0)
    parser.add_argument("--distance-bin-m", type=float, default=250.0)
    parser.add_argument(
        "--distance-shape",
        action="append",
        choices=[
            "monotone_decreasing",
            "monotone_increasing",
            "unrestricted_nonnegative",
            "peaked",
        ],
        help=(
            "Distance coefficient shape to search. Repeat to search a subset. "
            "If omitted, all available distance shapes are included in the autotune grid."
        ),
    )
    parser.add_argument(
        "--wind-speed-mode",
        action="append",
        choices=["none", "multiply", "log_multiply"],
        help=(
            "How wind speed modifies upwind tree-count exposure. Repeat to search a subset. "
            "If omitted, none, multiply, and log_multiply are included in the autotune grid."
        ),
    )
    parser.add_argument("--bearing-bin-deg", type=float, default=1.0)
    parser.add_argument(
        "--target",
        choices=["concentration", "log_concentration", "pcount", "log_pcount"],
        default="log_concentration",
        help="Response variable used during fitting.",
    )
    parser.add_argument("--max-iter", type=int, default=800)
    parser.add_argument(
        "--surface-smooth-lambda",
        type=float,
        default=0.1,
        help="Smoothing penalty for neighboring lag/distance weights in lag_distance_surface mode.",
    )
    parser.add_argument("--tol", type=float, default=1e-8)
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def load_env(env_file):
    env_path = Path(env_file)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect(dbname):
    if psycopg2 is None:
        return {"dbname": dbname}

    return psycopg2.connect(
        dbname=dbname,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PW"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, pd.Timestamp):
        value = value.isoformat()
    escaped = str(value).replace("'", "''")
    return "'%s'" % escaped


def sql_array_literal(values):
    return "ARRAY[%s]::varchar[]" % ",".join(sql_literal(value) for value in values)


def read_database_query(conn, query):
    if psycopg2 is not None and not isinstance(conn, dict):
        return pd.read_sql_query(query, conn)

    env = os.environ.copy()
    env["PGPASSWORD"] = os.getenv("DB_PW", "")
    copy_sql = "COPY (%s) TO STDOUT WITH CSV HEADER" % query.strip().rstrip(";")
    cmd = [
        "psql",
        "-h",
        os.getenv("DB_HOST", "localhost"),
        "-p",
        str(os.getenv("DB_PORT", "5432")),
        "-U",
        os.getenv("DB_USER", ""),
        "-d",
        conn["dbname"],
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        copy_sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
    if not result.stdout.strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(result.stdout))


def load_observations(conn, site_ids, min_pcount):
    query = """
    WITH tree_category AS (
        SELECT category_id
        FROM category
        WHERE name = 'TRE'
    )
    SELECT
        f.site_id,
        COALESCE(s.name, f.site_id) AS site_name,
        f.sensor_id,
        f.moment,
        m.pcount,
        f.cubic_meters,
        m.pcount / NULLIF(f.cubic_meters, 0) AS concentration
    FROM hourly_metrics m
    JOIN tree_category tc
      ON tc.category_id = m.category_id
    JOIN hourly_flow f
      ON f.sensor_id = m.sensor_id
     AND f.moment = m.moment
    JOIN site s
      ON s.site_id = f.site_id
    WHERE f.site_id = ANY(%s)
      AND f.cubic_meters > 0
      AND m.pcount >= %s
    ORDER BY f.site_id, f.moment;
    """ % (sql_array_literal(site_ids), float(min_pcount))
    df = read_database_query(conn, query)
    if not df.empty:
        df["moment"] = pd.to_datetime(df["moment"], utc=True)
    return df


def load_meteorology(conn, site_ids, min_moment, max_moment, max_lag_hours):
    query = """
    SELECT
        site_id,
        moment,
        wind_from_deg,
        wind_speed_10m_ms,
        temperature_2m_k,
        precipitation_m
    FROM era5_hourly_site
    WHERE site_id = ANY(%s)
      AND moment >= %s
      AND moment <= %s
      AND wind_from_deg IS NOT NULL
    ORDER BY site_id, moment;
    """ % (
        sql_array_literal(site_ids),
        sql_literal(min_moment - pd.Timedelta(hours=max_lag_hours)),
        sql_literal(max_moment),
    )
    df = read_database_query(conn, query)
    if not df.empty:
        df["moment"] = pd.to_datetime(df["moment"], utc=True)
    return df


def load_tree_polar(conn, site_ids, source_version_id, max_distance_m):
    if source_version_id is None:
        query = """
        WITH latest AS (
            SELECT site_id, MAX(source_version_id) AS source_version_id
            FROM monitor_tree_polar
            WHERE site_id = ANY(%s)
            GROUP BY site_id
        )
        SELECT
            p.site_id,
            p.source_version_id,
            p.distance_m,
            p.bearing_from_monitor_deg
        FROM monitor_tree_polar p
        JOIN latest l
          ON l.site_id = p.site_id
         AND l.source_version_id = p.source_version_id
        WHERE p.distance_m <= %s
        ORDER BY p.site_id, p.distance_m;
        """ % (sql_array_literal(site_ids), float(max_distance_m))
    else:
        query = """
        SELECT
            site_id,
            source_version_id,
            distance_m,
            bearing_from_monitor_deg
        FROM monitor_tree_polar
        WHERE site_id = ANY(%s)
          AND source_version_id = %s
          AND distance_m <= %s
        ORDER BY site_id, distance_m;
        """ % (sql_array_literal(site_ids), int(source_version_id), float(max_distance_m))

    return read_database_query(conn, query)


def make_target(observations, target):
    if target == "concentration":
        return observations["concentration"].to_numpy(dtype=float)
    if target == "log_concentration":
        return np.log1p(observations["concentration"].to_numpy(dtype=float))
    if target == "pcount":
        return observations["pcount"].to_numpy(dtype=float)
    if target == "log_pcount":
        return np.log1p(observations["pcount"].to_numpy(dtype=float))
    raise ValueError("Unsupported target: %s" % target)


def target_to_concentration(yhat, target):
    if target == "concentration":
        return yhat
    if target == "log_concentration":
        return np.expm1(yhat)
    return np.full_like(yhat, np.nan, dtype=float)


def bearing_bin_count(bearing_bin_deg):
    n_bins = 360.0 / bearing_bin_deg
    rounded = round(n_bins)
    if not math.isclose(n_bins, rounded, rel_tol=0, abs_tol=1e-9):
        raise ValueError("--bearing-bin-deg must divide 360 evenly.")
    return int(rounded)


def build_tree_histograms(polar, site_ids, max_distance_m, distance_bin_m, bearing_bin_deg):
    n_bearing = bearing_bin_count(bearing_bin_deg)
    n_distance = int(math.ceil(max_distance_m / distance_bin_m))
    histograms = {}
    source_versions = {}

    for site_id in site_ids:
        site_trees = polar[polar["site_id"] == site_id]
        if site_trees.empty:
            continue

        bearing_idx = np.floor(
            site_trees["bearing_from_monitor_deg"].to_numpy(dtype=float) / bearing_bin_deg
        ).astype(int) % n_bearing
        distance_idx = np.floor(
            site_trees["distance_m"].to_numpy(dtype=float) / distance_bin_m
        ).astype(int)
        distance_idx = np.clip(distance_idx, 0, n_distance - 1)

        matrix = np.zeros((n_bearing, n_distance), dtype=float)
        np.add.at(matrix, (bearing_idx, distance_idx), 1.0)
        histograms[site_id] = matrix
        source_versions[site_id] = sorted(site_trees["source_version_id"].dropna().unique().tolist())

    edges = np.arange(n_distance + 1, dtype=float) * distance_bin_m
    edges[-1] = max(edges[-1], max_distance_m)
    mids = (edges[:-1] + edges[1:]) / 2.0
    return histograms, source_versions, edges, mids


def angular_bin_indices(wind_from_deg, sector_width_deg, n_bearing, bearing_bin_deg):
    centers = (np.arange(n_bearing, dtype=float) + 0.5) * bearing_bin_deg
    delta = np.abs((centers - wind_from_deg + 180.0) % 360.0 - 180.0)
    return np.flatnonzero(delta <= sector_width_deg / 2.0)


def wind_speed_multiplier(wind_speed, wind_speed_mode):
    if wind_speed_mode == "none":
        return 1.0
    if not np.isfinite(wind_speed):
        return 0.0
    wind_speed = max(float(wind_speed), 0.0)
    if wind_speed_mode == "multiply":
        return wind_speed
    if wind_speed_mode == "log_multiply":
        return math.log1p(wind_speed)
    raise ValueError("Unsupported wind speed mode: %s" % wind_speed_mode)


def build_feature_matrix(
    rows,
    histograms,
    sector_width_deg,
    bearing_bin_deg,
    n_distance,
    wind_speed_mode,
    wind_direction_mode,
):
    n_bearing = bearing_bin_count(bearing_bin_deg)
    x = np.zeros((len(rows), n_distance), dtype=float)
    cache = {}

    for row_index, row in enumerate(rows.itertuples(index=False)):
        hist = histograms.get(row.site_id)
        if hist is None:
            continue

        if wind_direction_mode == "all_directions":
            cache_key = (row.site_id, wind_direction_mode)
            if cache_key not in cache:
                cache[cache_key] = hist.sum(axis=0)
        elif wind_direction_mode == "upwind":
            if not np.isfinite(row.wind_from_deg):
                continue
            wind_key = int(math.floor((row.wind_from_deg % 360.0) / bearing_bin_deg))
            cache_key = (row.site_id, wind_direction_mode, sector_width_deg, wind_key)
            if cache_key not in cache:
                wind_center = (wind_key + 0.5) * bearing_bin_deg
                idx = angular_bin_indices(wind_center, sector_width_deg, n_bearing, bearing_bin_deg)
                cache[cache_key] = hist[idx, :].sum(axis=0)
        else:
            raise ValueError("Unsupported wind direction mode: %s" % wind_direction_mode)

        x[row_index, :] = cache[cache_key] * wind_speed_multiplier(
            row.wind_speed_10m_ms,
            wind_speed_mode,
        )

    return x


def project_nonincreasing_nonnegative(values):
    block_values = []
    block_weights = []

    for value in values:
        block_values.append(float(value))
        block_weights.append(1.0)
        while len(block_values) >= 2 and block_values[-2] < block_values[-1]:
            weight = block_weights[-2] + block_weights[-1]
            merged = (
                block_values[-2] * block_weights[-2]
                + block_values[-1] * block_weights[-1]
            ) / weight
            block_values[-2:] = [merged]
            block_weights[-2:] = [weight]

    projected = np.repeat(block_values, np.array(block_weights, dtype=int))
    return np.maximum(projected, 0.0)


def project_nondecreasing_nonnegative(values):
    return project_nonincreasing_nonnegative(values[::-1])[::-1]


def project_unrestricted_nonnegative(values):
    return np.maximum(values, 0.0)


def project_peaked_nonnegative(values):
    values = np.asarray(values, dtype=float)
    best = None
    best_error = np.inf

    for peak_index in range(len(values)):
        left = project_nondecreasing_nonnegative(values[: peak_index + 1])
        right = project_nonincreasing_nonnegative(values[peak_index:])
        peak_value = (left[-1] + right[0]) / 2.0
        left[-1] = peak_value
        right[0] = peak_value
        candidate = np.concatenate([left[:-1], right])
        error = float(np.sum((candidate - values) ** 2))
        if error < best_error:
            best_error = error
            best = candidate

    return np.maximum(best, 0.0)


def project_distance_coefficients(values, distance_shape):
    if distance_shape == "monotone_decreasing":
        return project_nonincreasing_nonnegative(values)
    if distance_shape == "monotone_increasing":
        return project_nondecreasing_nonnegative(values)
    if distance_shape == "unrestricted_nonnegative":
        return project_unrestricted_nonnegative(values)
    if distance_shape == "peaked":
        return project_peaked_nonnegative(values)
    raise ValueError("Unsupported distance shape: %s" % distance_shape)


def site_intercepts(y, x, beta, site_codes, n_sites):
    residual_without_intercepts = y - x @ beta
    intercepts = np.zeros(n_sites, dtype=float)
    for code in range(n_sites):
        mask = site_codes == code
        if np.any(mask):
            intercepts[code] = residual_without_intercepts[mask].mean()
    return intercepts


def largest_lipschitz(x):
    if x.size == 0 or np.all(x == 0):
        return 1.0
    xtx = (x.T @ x) / max(len(x), 1)
    eigvals = np.linalg.eigvalsh(xtx)
    return max(float(eigvals[-1]), 1e-9)


def fit_distance_model(x, y, site_ids, distance_shape, max_iter, tol):
    site_codes, site_names = pd.factorize(site_ids, sort=True)
    n_sites = len(site_names)
    n_features = x.shape[1]
    beta = np.zeros(n_features, dtype=float)
    step = 1.0 / largest_lipschitz(x)
    previous_loss = np.inf

    for iteration in range(1, max_iter + 1):
        intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
        pred = intercepts[site_codes] + x @ beta
        residual = y - pred
        gradient = -(x.T @ residual) / max(len(y), 1)
        beta = project_distance_coefficients(beta - step * gradient, distance_shape)

        if iteration % 10 == 0 or iteration == max_iter:
            intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
            pred = intercepts[site_codes] + x @ beta
            loss = float(np.mean((y - pred) ** 2))
            if not np.isfinite(previous_loss):
                previous_loss = loss
                continue
            if abs(previous_loss - loss) <= tol * max(previous_loss, 1.0):
                break
            previous_loss = loss

    intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
    pred = intercepts[site_codes] + x @ beta
    return {
        "beta": beta,
        "site_intercepts": dict(zip(site_names.tolist(), intercepts.tolist())),
        "pred": pred,
        "iterations": iteration,
    }


def surface_smooth_penalty_and_gradient(beta_matrix, smooth_lambda):
    if smooth_lambda <= 0:
        return 0.0, np.zeros_like(beta_matrix)

    gradient = np.zeros_like(beta_matrix)
    penalty = 0.0

    if beta_matrix.shape[0] > 1:
        lag_diff = beta_matrix[1:, :] - beta_matrix[:-1, :]
        penalty += float(np.sum(lag_diff ** 2))
        gradient[1:, :] += 2.0 * lag_diff
        gradient[:-1, :] -= 2.0 * lag_diff

    if beta_matrix.shape[1] > 1:
        distance_diff = beta_matrix[:, 1:] - beta_matrix[:, :-1]
        penalty += float(np.sum(distance_diff ** 2))
        gradient[:, 1:] += 2.0 * distance_diff
        gradient[:, :-1] -= 2.0 * distance_diff

    return smooth_lambda * penalty, smooth_lambda * gradient


def project_surface_distance_coefficients(beta_matrix, distance_shape):
    projected = np.zeros_like(beta_matrix)
    for row_index in range(beta_matrix.shape[0]):
        projected[row_index, :] = project_distance_coefficients(beta_matrix[row_index, :], distance_shape)
    return projected


def fit_lag_distance_surface_model(
    x,
    y,
    site_ids,
    n_lags,
    n_distance,
    distance_shape,
    smooth_lambda,
    max_iter,
    tol,
):
    site_codes, site_names = pd.factorize(site_ids, sort=True)
    n_sites = len(site_names)
    beta = np.zeros(n_lags * n_distance, dtype=float)
    step = 1.0 / (largest_lipschitz(x) + 8.0 * max(smooth_lambda, 0.0))
    previous_loss = np.inf

    for iteration in range(1, max_iter + 1):
        intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
        pred = intercepts[site_codes] + x @ beta
        residual = y - pred
        data_gradient = -(x.T @ residual) / max(len(y), 1)
        beta_matrix = beta.reshape(n_lags, n_distance)
        _, smooth_gradient = surface_smooth_penalty_and_gradient(beta_matrix, smooth_lambda)
        gradient = data_gradient + smooth_gradient.reshape(-1)
        beta = beta - step * gradient
        beta = project_surface_distance_coefficients(
            beta.reshape(n_lags, n_distance),
            distance_shape,
        ).reshape(-1)

        if iteration % 10 == 0 or iteration == max_iter:
            intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
            pred = intercepts[site_codes] + x @ beta
            data_loss = float(np.mean((y - pred) ** 2))
            penalty, _ = surface_smooth_penalty_and_gradient(
                beta.reshape(n_lags, n_distance),
                smooth_lambda,
            )
            loss = data_loss + penalty
            if not np.isfinite(previous_loss):
                previous_loss = loss
                continue
            if abs(previous_loss - loss) <= tol * max(previous_loss, 1.0):
                break
            previous_loss = loss

    intercepts = site_intercepts(y, x, beta, site_codes, n_sites)
    pred = intercepts[site_codes] + x @ beta
    return {
        "beta": beta,
        "site_intercepts": dict(zip(site_names.tolist(), intercepts.tolist())),
        "pred": pred,
        "iterations": iteration,
    }


def score_model(y, pred):
    residual = y - pred
    sse = float(np.sum(residual ** 2))
    sst = float(np.sum((y - y.mean()) ** 2))
    return {
        "rmse": float(np.sqrt(np.mean(residual ** 2))),
        "mae": float(np.mean(np.abs(residual))),
        "r2": float(1.0 - sse / sst) if sst > 0 else np.nan,
    }


def lagged_rows(observations, met, lag_hours):
    rows = observations.copy()
    rows["observation_id"] = rows.index
    rows["lag_moment"] = rows["moment"] - pd.Timedelta(hours=lag_hours)
    met_lag = met.rename(columns={"moment": "lag_moment"})
    rows = rows.merge(
        met_lag[["site_id", "lag_moment", "wind_from_deg", "wind_speed_10m_ms"]],
        on=["site_id", "lag_moment"],
        how="left",
    )
    rows = rows.dropna(subset=["wind_from_deg"]).copy()
    return rows


def lag_offsets(lag_window_hours, lag_window_direction):
    window = int(lag_window_hours)
    if window <= 0:
        return [0]
    if lag_window_direction == "older":
        return list(range(0, window + 1))
    if lag_window_direction == "recent":
        return list(range(0, -window - 1, -1))
    if lag_window_direction == "centered":
        return list(range(-window, window + 1))
    raise ValueError("Unsupported lag window direction: %s" % lag_window_direction)


def lag_kernel_components(
    center_lag_hours,
    lag_kernel,
    lag_window_hours,
    lag_window_direction,
    max_lag_hours,
):
    if lag_kernel == "exact":
        return [(center_lag_hours, 1.0)]

    window = int(lag_window_hours)
    if window <= 0:
        return [(center_lag_hours, 1.0)]

    components = []
    offsets = lag_offsets(window, lag_window_direction)
    if lag_kernel == "gaussian":
        sigma = max(window / 2.0, 0.5)
        for offset in offsets:
            lag = center_lag_hours + offset
            if lag < 0 or lag > max_lag_hours:
                continue
            weight = math.exp(-0.5 * (offset / sigma) ** 2)
            components.append((lag, weight))
    elif lag_kernel == "monotone_decreasing":
        for offset_index, offset in enumerate(offsets):
            lag = center_lag_hours + offset
            if lag < 0 or lag > max_lag_hours:
                continue
            weight = len(offsets) - offset_index
            components.append((lag, weight))
    elif lag_kernel == "monotone_increasing":
        for offset_index, offset in enumerate(offsets):
            lag = center_lag_hours + offset
            if lag < 0 or lag > max_lag_hours:
                continue
            weight = offset_index + 1
            components.append((lag, weight))
    else:
        raise ValueError("Unsupported lag kernel: %s" % lag_kernel)

    weight_sum = sum(weight for _, weight in components)
    if weight_sum <= 0:
        return [(center_lag_hours, 1.0)]
    return [(lag, weight / weight_sum) for lag, weight in components]


def aggregate_daily_rows(rows, x):
    daily = rows.copy()
    daily["metric_date"] = daily["moment"].dt.date
    daily["row_order"] = np.arange(len(daily), dtype=int)

    grouped_rows = []
    grouped_x = []
    group_columns = ["site_id", "metric_date"]
    for _, group in daily.groupby(group_columns, sort=True):
        row_indices = group["row_order"].to_numpy(dtype=int)
        first = group.iloc[0]
        grouped_rows.append(
            {
                "site_id": first["site_id"],
                "site_name": first["site_name"],
                "sensor_id": first["sensor_id"],
                "metric_date": first["metric_date"],
                "moment": pd.Timestamp(first["metric_date"]).tz_localize("UTC"),
                "lag_moment": group["lag_moment"].min() if "lag_moment" in group.columns else pd.NaT,
                "wind_from_deg": np.nan,
                "wind_speed_10m_ms": (
                    group["wind_speed_10m_ms"].mean()
                    if "wind_speed_10m_ms" in group.columns
                    else np.nan
                ),
                "pcount": group["pcount"].mean(),
                "concentration": group["concentration"].mean(),
                "n_hourly_measurements": int(len(group)),
            }
        )
        grouped_x.append(x[row_indices, :].sum(axis=0))

    if not grouped_rows:
        return pd.DataFrame(), np.zeros((0, x.shape[1]), dtype=float)

    return pd.DataFrame(grouped_rows), np.vstack(grouped_x)


def prepare_candidate_design(
    observations,
    met,
    histograms,
    args,
    lag_hours,
    lag_kernel,
    lag_window_direction,
    sector_width_deg,
    wind_speed_mode,
    wind_direction_mode,
    n_distance,
):
    accumulated_x = {}
    row_by_observation = {}
    components = lag_kernel_components(
        lag_hours,
        lag_kernel,
        args.lag_window_hours,
        lag_window_direction,
        args.max_lag_hours,
    )

    for component_lag, component_weight in components:
        rows = lagged_rows(observations, met, component_lag)
        rows = rows[rows["site_id"].isin(histograms.keys())].copy()
        if rows.empty:
            continue

        x_component = build_feature_matrix(
            rows,
            histograms=histograms,
            sector_width_deg=sector_width_deg,
            bearing_bin_deg=args.bearing_bin_deg,
            n_distance=n_distance,
            wind_speed_mode=wind_speed_mode,
            wind_direction_mode=wind_direction_mode,
        )

        for row_index, row in enumerate(rows.itertuples(index=False)):
            observation_id = int(row.observation_id)
            if observation_id not in accumulated_x:
                accumulated_x[observation_id] = np.zeros(n_distance, dtype=float)
                row_by_observation[observation_id] = rows.iloc[row_index].copy()
            accumulated_x[observation_id] += component_weight * x_component[row_index, :]

    if not accumulated_x:
        return pd.DataFrame(), np.zeros((0, n_distance), dtype=float)

    observation_ids = sorted(accumulated_x)
    rows = pd.DataFrame([row_by_observation[observation_id] for observation_id in observation_ids])
    x = np.vstack([accumulated_x[observation_id] for observation_id in observation_ids])
    rows["lag_kernel"] = lag_kernel
    rows["lag_window_hours"] = args.lag_window_hours if lag_kernel != "exact" else 0
    rows["lag_window_direction"] = lag_window_direction if lag_kernel != "exact" else "none"
    rows["kernel_lag_hours"] = ",".join("%i:%.4f" % item for item in components)

    if args.temporal_resolution == "daily":
        rows, x = aggregate_daily_rows(rows, x)
        if not rows.empty:
            rows["lag_kernel"] = lag_kernel
            rows["lag_window_hours"] = args.lag_window_hours if lag_kernel != "exact" else 0
            rows["lag_window_direction"] = lag_window_direction if lag_kernel != "exact" else "none"
            rows["kernel_lag_hours"] = ",".join("%i:%.4f" % item for item in components)

    return rows, x


def prepare_surface_design(
    observations,
    met,
    histograms,
    args,
    lag_values,
    sector_width_deg,
    wind_speed_mode,
    wind_direction_mode,
    n_distance,
):
    observation_ids = observations.index.to_numpy(dtype=int)
    observation_position = {observation_id: i for i, observation_id in enumerate(observation_ids)}
    x = np.zeros((len(observations), len(lag_values) * n_distance), dtype=float)

    for lag_index, lag_hours in enumerate(lag_values):
        rows = lagged_rows(observations, met, lag_hours)
        rows = rows[rows["site_id"].isin(histograms.keys())].copy()
        if rows.empty:
            continue
        x_component = build_feature_matrix(
            rows,
            histograms=histograms,
            sector_width_deg=sector_width_deg,
            bearing_bin_deg=args.bearing_bin_deg,
            n_distance=n_distance,
            wind_speed_mode=wind_speed_mode,
            wind_direction_mode=wind_direction_mode,
        )
        column_start = lag_index * n_distance
        column_stop = column_start + n_distance
        for row_index, row in enumerate(rows.itertuples(index=False)):
            position = observation_position.get(int(row.observation_id))
            if position is not None:
                x[position, column_start:column_stop] = x_component[row_index, :]

    rows = observations[observations["site_id"].isin(histograms.keys())].copy()
    kept_positions = [observation_position[int(index)] for index in rows.index]
    x = x[kept_positions, :]

    if args.temporal_resolution == "daily":
        rows, x = aggregate_daily_rows(rows, x)

    return rows, x


def fit_grid(observations, met, histograms, args, distance_edges):
    n_distance = len(distance_edges) - 1
    candidate_rows = []
    best = None

    sector_values = list(range(args.sector_step_deg, args.max_sector_width_deg + 1, args.sector_step_deg))
    lag_values = sorted(set(args.lag_hours)) if args.lag_hours else list(range(0, args.max_lag_hours + 1))
    lag_kernels = args.lag_kernel or [
        "exact",
        "gaussian",
        "monotone_decreasing",
        "monotone_increasing",
    ]
    lag_window_directions = args.lag_window_direction or ["older", "recent", "centered"]
    distance_shapes = args.distance_shape or [
        "monotone_decreasing",
        "monotone_increasing",
        "unrestricted_nonnegative",
        "peaked",
    ]
    wind_speed_modes = args.wind_speed_mode or ["none", "multiply", "log_multiply"]
    wind_direction_modes = args.wind_direction_mode or ["upwind", "all_directions"]

    for lag_hours in lag_values:
        for lag_kernel in lag_kernels:
            directions = ["none"] if lag_kernel == "exact" else lag_window_directions
            for lag_window_direction in directions:
                for wind_direction_mode in wind_direction_modes:
                    direction_sector_values = [360] if wind_direction_mode == "all_directions" else sector_values
                    for sector_width_deg in direction_sector_values:
                        for wind_speed_mode in wind_speed_modes:
                            rows, x = prepare_candidate_design(
                                observations=observations,
                                met=met,
                                histograms=histograms,
                                args=args,
                                lag_hours=lag_hours,
                                lag_kernel=lag_kernel,
                                lag_window_direction=lag_window_direction,
                                sector_width_deg=sector_width_deg,
                                wind_speed_mode=wind_speed_mode,
                                wind_direction_mode=wind_direction_mode,
                                n_distance=n_distance,
                            )
                            if rows.empty:
                                continue
                            if np.all(x == 0):
                                continue

                            y = make_target(rows, args.target)
                            for distance_shape in distance_shapes:
                                fit = fit_distance_model(
                                    x=x,
                                    y=y,
                                    site_ids=rows["site_id"].to_numpy(),
                                    distance_shape=distance_shape,
                                    max_iter=args.max_iter,
                                    tol=args.tol,
                                )
                                scores = score_model(y, fit["pred"])
                                row = {
                                    "lag_hours": lag_hours,
                                    "lag_kernel": lag_kernel,
                                    "lag_window_hours": args.lag_window_hours if lag_kernel != "exact" else 0,
                                    "lag_window_direction": lag_window_direction if lag_kernel != "exact" else "none",
                                    "sector_width_deg": sector_width_deg,
                                    "wind_direction_mode": wind_direction_mode,
                                    "wind_speed_mode": wind_speed_mode,
                                    "distance_shape": distance_shape,
                                    "n_observations": int(len(rows)),
                                    "n_sites": int(rows["site_id"].nunique()),
                                    "temporal_resolution": args.temporal_resolution,
                                    "nonzero_distance_bins": int(np.sum(fit["beta"] > 0)),
                                    "iterations": int(fit["iterations"]),
                                    **scores,
                                }
                                candidate_rows.append(row)

                                if best is None or row["r2"] > best["summary"]["r2"]:
                                    best = {
                                        "summary": row,
                                        "fit": fit,
                                        "rows": rows,
                                        "x": x,
                                        "y": y,
                                    }

    return pd.DataFrame(candidate_rows), best


def fit_lag_distance_surface_grid(observations, met, histograms, args, distance_edges):
    n_distance = len(distance_edges) - 1
    candidate_rows = []
    best = None

    lag_values = sorted(set(args.lag_hours)) if args.lag_hours else list(range(0, args.max_lag_hours + 1))
    sector_values = list(range(args.sector_step_deg, args.max_sector_width_deg + 1, args.sector_step_deg))
    wind_direction_modes = args.wind_direction_mode or ["upwind", "all_directions"]
    wind_speed_modes = args.wind_speed_mode or ["none", "multiply", "log_multiply"]
    distance_shapes = args.distance_shape or ["monotone_decreasing"]

    for wind_direction_mode in wind_direction_modes:
        direction_sector_values = [360] if wind_direction_mode == "all_directions" else sector_values
        for sector_width_deg in direction_sector_values:
            for wind_speed_mode in wind_speed_modes:
                rows, x = prepare_surface_design(
                    observations=observations,
                    met=met,
                    histograms=histograms,
                    args=args,
                    lag_values=lag_values,
                    sector_width_deg=sector_width_deg,
                    wind_speed_mode=wind_speed_mode,
                    wind_direction_mode=wind_direction_mode,
                    n_distance=n_distance,
                )
                if rows.empty or np.all(x == 0):
                    continue

                y = make_target(rows, args.target)
                for distance_shape in distance_shapes:
                    fit = fit_lag_distance_surface_model(
                        x=x,
                        y=y,
                        site_ids=rows["site_id"].to_numpy(),
                        n_lags=len(lag_values),
                        n_distance=n_distance,
                        distance_shape=distance_shape,
                        smooth_lambda=args.surface_smooth_lambda,
                        max_iter=args.max_iter,
                        tol=args.tol,
                    )
                    scores = score_model(y, fit["pred"])
                    row = {
                        "interaction_mode": "lag_distance_surface",
                        "lag_hours": ",".join(str(value) for value in lag_values),
                        "n_lag_surface_hours": len(lag_values),
                        "lag_kernel": "surface",
                        "lag_window_hours": 0,
                        "lag_window_direction": "surface",
                        "sector_width_deg": sector_width_deg,
                        "wind_direction_mode": wind_direction_mode,
                        "wind_speed_mode": wind_speed_mode,
                        "distance_shape": distance_shape,
                        "surface_smooth_lambda": args.surface_smooth_lambda,
                        "n_observations": int(len(rows)),
                        "n_sites": int(rows["site_id"].nunique()),
                        "temporal_resolution": args.temporal_resolution,
                        "nonzero_distance_bins": int(np.sum(fit["beta"] > 0)),
                        "iterations": int(fit["iterations"]),
                        **scores,
                    }
                    candidate_rows.append(row)

                    if best is None or row["r2"] > best["summary"]["r2"]:
                        best = {
                            "summary": row,
                            "fit": fit,
                            "rows": rows,
                            "x": x,
                            "y": y,
                            "surface_lag_values": lag_values,
                        }

    return pd.DataFrame(candidate_rows), best


def write_outputs(output_dir, candidates, best, distance_edges, distance_mids, args, source_versions):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    candidates_path = output_path / "upwind_tree_fit_candidates.csv"
    candidates.sort_values(["r2", "rmse"], ascending=[False, True]).to_csv(candidates_path, index=False)

    beta = best["fit"]["beta"]
    if args.interaction_mode == "lag_distance_surface":
        surface_lag_values = best.get("surface_lag_values", [])
        n_distance = len(distance_edges) - 1
        beta_matrix = beta.reshape(len(surface_lag_values), n_distance)
        weight_rows = []
        for lag_index, lag_hours in enumerate(surface_lag_values):
            for distance_index in range(n_distance):
                weight_rows.append(
                    {
                        "lag_hours": lag_hours,
                        "distance_bin_start_m": distance_edges[distance_index],
                        "distance_bin_end_m": distance_edges[distance_index + 1],
                        "distance_bin_mid_m": distance_mids[distance_index],
                        "tree_count_coefficient": beta_matrix[lag_index, distance_index],
                    }
                )
        weights = pd.DataFrame(weight_rows)
    else:
        weights = pd.DataFrame(
            {
                "distance_bin_start_m": distance_edges[:-1],
                "distance_bin_end_m": distance_edges[1:],
                "distance_bin_mid_m": distance_mids,
                "tree_count_coefficient": beta,
            }
        )
    weights_path = output_path / "upwind_tree_fit_best_distance_weights.csv"
    weights.to_csv(weights_path, index=False)

    pred_rows = best["rows"].copy()
    pred_rows["target_observed"] = best["y"]
    pred_rows["target_predicted"] = best["fit"]["pred"]
    pred_rows["predicted_concentration"] = target_to_concentration(best["fit"]["pred"], args.target)
    pred_rows["observed_concentration"] = pred_rows["concentration"]
    pred_rows["observed_pcount"] = pred_rows["pcount"]
    output_columns = [
        "site_id",
        "site_name",
        "sensor_id",
        "moment",
        "metric_date",
        "lag_moment",
        "lag_kernel",
        "lag_window_hours",
        "lag_window_direction",
        "kernel_lag_hours",
        "wind_from_deg",
        "wind_speed_10m_ms",
        "n_hourly_measurements",
        "observed_pcount",
        "observed_concentration",
        "target_observed",
        "target_predicted",
        "predicted_concentration",
    ]
    pred_rows = pred_rows[[column for column in output_columns if column in pred_rows.columns]]
    predictions_path = output_path / "upwind_tree_fit_best_predictions.csv"
    pred_rows.to_csv(predictions_path, index=False)

    best_summary = {
        "target": args.target,
        "interaction_mode": args.interaction_mode,
        "temporal_resolution": args.temporal_resolution,
        "searched_lag_kernels": args.lag_kernel or [
            "exact",
            "gaussian",
            "monotone_decreasing",
            "monotone_increasing",
        ],
        "searched_lag_window_directions": args.lag_window_direction or [
            "older",
            "recent",
            "centered",
        ],
        "searched_distance_shapes": args.distance_shape or [
            "monotone_decreasing",
            "monotone_increasing",
            "unrestricted_nonnegative",
            "peaked",
        ],
        "searched_wind_speed_modes": args.wind_speed_mode or [
            "none",
            "multiply",
            "log_multiply",
        ],
        "searched_wind_direction_modes": args.wind_direction_mode or [
            "upwind",
            "all_directions",
        ],
        "minimum_tree_pollen_pcount": args.min_pcount,
        "best_candidate": best["summary"],
        "site_intercepts": best["fit"]["site_intercepts"],
        "source_versions_by_site": source_versions,
        "equation": (
            "target = site_intercept[site] + sum_j tree_count_coefficient[j] "
            "* upwind_tree_count_in_distance_bin[j]"
            if args.interaction_mode == "separable"
            else "target = site_intercept[site] + sum_lag sum_distance weight[lag,distance] "
            "* upwind_tree_count[lag,distance]"
        ),
        "notes": [
            "Tree pollen is the direct TRE category row.",
            "Distance coefficients are constrained according to the selected distance_shape.",
            "Upwind sectors are centered on ERA5 wind_from_deg at moment minus lag_hours.",
            "Wind direction mode controls whether exposure uses only upwind tree sectors or all tree bearings.",
            "Lag kernels are normalized so kernel choice changes timing shape rather than total weight.",
            "Lag window direction controls whether kernel components use older, recent, or centered hours around lag_hours.",
            "Wind speed mode controls whether upwind tree-count exposure is unweighted, multiplied by wind speed, or multiplied by log1p(wind speed).",
            "Tree locations are binned by bearing and distance before fitting.",
            "Daily mode uses mean daily tree pollen and summed hourly upwind tree-count bins.",
        ],
        "outputs": {
            "candidate_grid": str(candidates_path),
            "best_distance_weights": str(weights_path),
            "best_predictions": str(predictions_path),
        },
    }
    summary_path = output_path / "upwind_tree_fit_summary.json"
    summary_path.write_text(json.dumps(best_summary, indent=2, default=str))
    return best_summary


def main():
    args = parse_args()
    load_env(args.env_file)
    site_ids = args.site_ids or ANN_ARBOR_SITES

    if args.max_lag_hours > 36:
        raise ValueError("This first-pass fitter is capped at lag <= 36 hours.")
    if args.lag_hours and any(lag < 0 or lag > args.max_lag_hours for lag in args.lag_hours):
        raise ValueError("Every --lag-hour must be between 0 and --max-lag-hours.")
    if args.max_sector_width_deg > 45:
        raise ValueError("This first-pass fitter is capped at sector width <= 45 degrees.")

    conn = connect(args.dbname)
    try:
        observations = load_observations(conn, site_ids, args.min_pcount)
        if observations.empty:
            raise RuntimeError("No hourly direct-TRE pollen observations matched the requested filters.")

        met = load_meteorology(
            conn,
            site_ids=site_ids,
            min_moment=observations["moment"].min(),
            max_moment=observations["moment"].max(),
            max_lag_hours=args.max_lag_hours,
        )
        if met.empty:
            raise RuntimeError("No ERA5 wind rows were found for the requested sites and time range.")

        polar = load_tree_polar(
            conn,
            site_ids=site_ids,
            source_version_id=args.source_version_id,
            max_distance_m=args.max_distance_m,
        )
        if polar.empty:
            raise RuntimeError("No monitor_tree_polar rows were found for the requested sites.")
    finally:
        if hasattr(conn, "close"):
            conn.close()

    histograms, source_versions, distance_edges, distance_mids = build_tree_histograms(
        polar=polar,
        site_ids=site_ids,
        max_distance_m=args.max_distance_m,
        distance_bin_m=args.distance_bin_m,
        bearing_bin_deg=args.bearing_bin_deg,
    )
    if not histograms:
        raise RuntimeError("No tree histograms could be built from monitor_tree_polar.")

    print("Loaded %i direct-TRE hourly observations with pcount >= %.1f." % (len(observations), args.min_pcount))
    print("Interaction mode: %s." % args.interaction_mode)
    print("Temporal resolution: %s." % args.temporal_resolution)
    print("Loaded %i ERA5 hourly wind rows." % len(met))
    print("Loaded %i monitor-tree polar rows across %i sites." % (len(polar), len(histograms)))
    lag_kernels = args.lag_kernel or [
        "exact",
        "gaussian",
        "monotone_decreasing",
        "monotone_increasing",
    ]
    lag_window_directions = args.lag_window_direction or ["older", "recent", "centered"]
    distance_shapes = args.distance_shape or [
        "monotone_decreasing",
        "monotone_increasing",
        "unrestricted_nonnegative",
        "peaked",
    ]
    wind_speed_modes = args.wind_speed_mode or ["none", "multiply", "log_multiply"]
    wind_direction_modes = args.wind_direction_mode or ["upwind", "all_directions"]
    print("Lag kernels: %s." % ", ".join(lag_kernels))
    print("Lag window directions: %s." % ", ".join(lag_window_directions))
    print("Distance shapes: %s." % ", ".join(distance_shapes))
    print("Wind speed modes: %s." % ", ".join(wind_speed_modes))
    print("Wind direction modes: %s." % ", ".join(wind_direction_modes))
    exact_direction_slots = 1 if "exact" in lag_kernels else 0
    nonexact_kernel_count = len([kernel for kernel in lag_kernels if kernel != "exact"])
    n_kernel_direction_slots = exact_direction_slots + nonexact_kernel_count * len(lag_window_directions)
    n_lags = len(set(args.lag_hours)) if args.lag_hours else args.max_lag_hours + 1
    n_direction_sector_slots = 0
    for mode in wind_direction_modes:
        n_direction_sector_slots += 1 if mode == "all_directions" else args.max_sector_width_deg // args.sector_step_deg
    if args.interaction_mode == "lag_distance_surface":
        print("Fitting lag-distance surface with %i lag rows x %i wind direction/sector choices x %i wind speed modes x %i distance shapes..." % (
            n_lags,
            n_direction_sector_slots,
            len(wind_speed_modes),
            len(distance_shapes),
        ))
        candidates, best = fit_lag_distance_surface_grid(
            observations=observations,
            met=met,
            histograms=histograms,
            args=args,
            distance_edges=distance_edges,
        )
    else:
        print("Fitting %i lags x %i lag kernel/direction choices x %i wind direction/sector choices x %i wind speed modes x %i distance shapes..." % (
            n_lags,
            n_kernel_direction_slots,
            n_direction_sector_slots,
            len(wind_speed_modes),
            len(distance_shapes),
        ))
        candidates, best = fit_grid(
            observations=observations,
            met=met,
            histograms=histograms,
            args=args,
            distance_edges=distance_edges,
        )
    if best is None or candidates.empty:
        raise RuntimeError("No candidate model could be fit. Check wind/tree overlap.")

    summary = write_outputs(
        output_dir=args.output_dir,
        candidates=candidates,
        best=best,
        distance_edges=distance_edges,
        distance_mids=distance_mids,
        args=args,
        source_versions=source_versions,
    )

    top = candidates.sort_values(["r2", "rmse"], ascending=[False, True]).head(args.top_n)
    print("\nTop candidate models:")
    print(top.to_string(index=False))
    print("\nBest equation:")
    print(summary["equation"])
    print("Best lag_hours: %s" % summary["best_candidate"]["lag_hours"])
    print("Best lag_kernel: %s" % summary["best_candidate"]["lag_kernel"])
    print("Best lag_window_direction: %s" % summary["best_candidate"]["lag_window_direction"])
    print("Best sector_width_deg: %s" % summary["best_candidate"]["sector_width_deg"])
    print("Best wind_direction_mode: %s" % summary["best_candidate"]["wind_direction_mode"])
    print("Best wind_speed_mode: %s" % summary["best_candidate"]["wind_speed_mode"])
    print("Best distance_shape: %s" % summary["best_candidate"]["distance_shape"])
    print("Best R2 on %s %s: %.4f" % (
        args.temporal_resolution,
        args.target,
        summary["best_candidate"]["r2"],
    ))
    print("Wrote results to %s" % Path(args.output_dir).resolve())


if __name__ == "__main__":
    main()
