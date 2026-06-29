"""Export static data files for the Streamlit Community Cloud dashboard."""

import os
import random
from pathlib import Path

import pandas as pd
import psycopg2


ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
DATA_DIR = ROOT / "data"
DB_NAME = os.getenv("DASHBOARD_DB_NAME", "pollen_dashboard")
SITE_NAME_KEY_FILE = REPO_ROOT / "regional_pollen_dashboard_site_name_key.csv"
RANDOM_NAME_SEED = 20260617
EXCLUDED_SITE_NAMES = {
    "CH1",
    "CH1-out",
    "CH2",
    "CH2-out",
    "CH3",
    "CH3-out",
    "CH4",
    "CH4-out",
}

NAME_ADJECTIVES = [
    "Amber",
    "Blue",
    "Bright",
    "Cedar",
    "Clear",
    "Copper",
    "Crimson",
    "Distant",
    "Emerald",
    "Evening",
    "Golden",
    "Granite",
    "Green",
    "Harbor",
    "Hidden",
    "Ivory",
    "Juniper",
    "Lakeside",
    "Maple",
    "Meadow",
    "Morning",
    "North",
    "Quiet",
    "Red",
    "River",
    "Silver",
    "South",
    "Stone",
    "Sunny",
    "West",
]
NAME_NOUNS = [
    "Bridge",
    "Brook",
    "Center",
    "Circle",
    "Commons",
    "Crossing",
    "Field",
    "Garden",
    "Glen",
    "Grove",
    "Heights",
    "Hill",
    "Landing",
    "Market",
    "Meadow",
    "Park",
    "Place",
    "Point",
    "Ridge",
    "Square",
    "Station",
    "Terrace",
    "Trail",
    "Valley",
    "View",
    "Village",
    "Vista",
    "Walk",
    "Way",
    "Yard",
]


def load_env_file(path):
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect_db():
    load_env_file(REPO_ROOT / ".env")
    return psycopg2.connect(
        dbname=DB_NAME,
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PW"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def anonymize_site_names(sites):
    rng = random.Random(RANDOM_NAME_SEED)
    names = ["%s %s" % (adjective, noun) for adjective in NAME_ADJECTIVES for noun in NAME_NOUNS]
    rng.shuffle(names)

    sites = sites.sort_values(["study_region_name", "name", "site_id"]).copy()
    if len(sites) > len(names):
        raise RuntimeError("Not enough random site names for %i sites." % len(sites))

    sites["random_name"] = names[:len(sites)]
    key = sites[["study_region_name", "site_id", "name", "random_name"]].rename(
        columns={
            "study_region_name": "study_region",
            "name": "real_name",
        }
    )
    sites["name"] = sites["random_name"]
    sites = sites.drop(columns=["random_name"])
    return sites, key


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect_db()
    try:
        sites = pd.read_sql_query(
            """
            WITH latest_site_sensor AS (
                SELECT DISTINCT ON (ssj.site_id)
                    ssj.site_id,
                    ssj.sensor_id,
                    ssj.last_updated,
                    se.status_code,
                    se.status_message,
                    se.status_description,
                    se.mode_description
                FROM site_sensor_join ssj
                LEFT JOIN sensor se
                  ON se.sensor_id = ssj.sensor_id
                ORDER BY ssj.site_id, COALESCE(ssj.last_updated, ssj.since) DESC NULLS LAST
            )
            SELECT
                s.site_id,
                COALESCE(s.name, s.site_id) AS name,
                c.study_region_name,
                ST_Y(s.location) AS latitude,
                ST_X(s.location) AS longitude,
                lss.sensor_id,
                lss.last_updated,
                lss.status_code,
                lss.status_message,
                lss.status_description,
                lss.mode_description
            FROM site s
            JOIN city c
              ON c.city_id = s.city_id
            LEFT JOIN latest_site_sensor lss
              ON lss.site_id = s.site_id
            WHERE c.study_region_name IS NOT NULL
              AND c.study_region_name <> 'Seattle'
            ORDER BY c.study_region_name, name, site_id;
            """,
            conn,
        )
        pollen = pd.read_sql_query(
            """
            WITH category_matches AS (
                SELECT category_map.category, c.category_id
                FROM (
                    VALUES
                        ('Total Pollen', 'POL'),
                        ('Total Tree Pollen', 'TRE'),
                        ('Total Grass Pollen', 'GRA'),
                        ('Total Weed/Shrub Pollen', 'WEE'),
                        ('Total Mold', 'MOL'),
                        ('Other Particulate', 'OTHPAR'),
                        ('Quercus (Oak)', 'QUE'),
                        ('Acer (Maple)', 'ACE'),
                        ('Betula (Birch)', 'BET'),
                        ('Ulmus (Elm)', 'ULM'),
                        ('Fraxinus (Ash)', 'FRA'),
                        ('Populus (Poplar)', 'POP'),
                        ('Pinaceae (Pine)', 'PIN')
                ) AS category_map(category, category_code)
                JOIN category c
                  ON c.name = category_map.category_code
            ),
            flow_hours AS (
                SELECT
                    c.study_region_name,
                    f.site_id,
                    f.sensor_id,
                    f.moment,
                    f.cubic_meters
                FROM hourly_flow f
                JOIN site s
                  ON s.site_id = f.site_id
                JOIN city c
                  ON c.city_id = s.city_id
                WHERE c.study_region_name IS NOT NULL
                  AND c.study_region_name <> 'Seattle'
                  AND f.cubic_meters > 0
            ),
            hourly_allergen AS (
                SELECT
                    m.sensor_id,
                    m.moment,
                    cm.category,
                    SUM(m.pcount) AS pcount
                FROM hourly_metrics m
                JOIN category_matches cm
                  ON cm.category_id = m.category_id
                GROUP BY m.sensor_id, m.moment, cm.category
            )
            SELECT
                fh.study_region_name,
                fh.site_id,
                fh.moment,
                cm.category,
                COALESCE(ha.pcount, 0) AS pcount,
                fh.cubic_meters,
                COALESCE(ha.pcount, 0) / fh.cubic_meters AS concentration
            FROM flow_hours fh
            CROSS JOIN category_matches cm
            LEFT JOIN hourly_allergen ha
              ON ha.sensor_id = fh.sensor_id
             AND ha.moment = fh.moment
             AND ha.category = cm.category
            ORDER BY fh.study_region_name, fh.site_id, fh.moment, cm.category;
            """,
            conn,
        )
    finally:
        conn.close()

    sites, site_name_key = anonymize_site_names(sites)
    excluded_site_ids = site_name_key.loc[
        site_name_key["real_name"].isin(EXCLUDED_SITE_NAMES), "site_id"
    ]
    sites = sites.loc[~sites["site_id"].isin(excluded_site_ids)].copy()
    pollen = pollen.loc[~pollen["site_id"].isin(excluded_site_ids)].copy()
    site_name_key = site_name_key.loc[~site_name_key["site_id"].isin(excluded_site_ids)].copy()

    site_name_key.to_csv(SITE_NAME_KEY_FILE, index=False)
    sites.to_parquet(DATA_DIR / "sites.parquet", index=False, compression="zstd")
    pollen.to_parquet(DATA_DIR / "pollen_hourly.parquet", index=False, compression="zstd")
    print("Wrote %i site rows" % len(sites))
    print("Wrote %i pollen rows" % len(pollen))
    print("Wrote site name key to %s" % SITE_NAME_KEY_FILE)


if __name__ == "__main__":
    main()
