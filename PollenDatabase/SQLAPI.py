### SQLAPI.py
### Author: Andrew Larkin
### Date Created: March 27, 2026
### Summary: Custom wrapper for performing CRUD operations on a PostGIS database

################ Import Libraries ##################

import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
import pandas as ps


class SQLAPI:

    # initialize class
    # INPUTS:
        # db (str) - database name
        # u (str) - user name
        # pw (str) - password
        # h (str) - host
        # p (str) - port

    def __init__(self,db,u,pw,h,p):
        self.conn = psycopg2.connect(
            dbname = db,
            user = u,
            password = pw,
            host = h,
            port = p
        )

        # 🔥 Force UTC for this connection
        with self.conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC';")
        self.conn.commit()

    
    def ensure_utc(self,dt):
        if dt is None:
            return None

        # Handle pandas Timestamp
        if isinstance(dt, ps.Timestamp):
            if dt.tz is None:
                return dt.tz_localize("UTC").to_pydatetime()
            return dt.tz_convert("UTC").to_pydatetime()

        # Handle string
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)

        # Handle datetime
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        raise TypeError(f"Unsupported datetime type: {type(dt)}")


    # test if database connection was sucessful
    # OUTPUTS:
    #    True is sucessful, False otherwsie
    def isConnected(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return True
        except:
            return False

    # insert city record into city table
    # INPUTS:
    #    city_id (int) - unique US Census GEOID 
    #    city_name (str) - commmon name
    #    longtude (float) - centroid longitude
    #    latitude (float) - centroid latitutde
    def addCity(self,city_id, city_name, longitude, latitude):
        query = """
        
        INSERT INTO  city(
            city_id,
            city_name,
            center_location
        )
        VALUES (
            %s,
            %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        ON CONFLICT (city_id) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (city_id, city_name, longitude, latitude) 
            )
        self.conn.commit()
    
    # insert site record into site table
    # INPUTS:
    #    site_id (str) - unique site id
    #    city_id (int) - unique US Census GEOID
    #    longitude (float) - site longitude coord
    #    latitude (float) - site latitude coord
    #    name (str) - name (not systematic, does not follow a nomenclature)
    #    usage_code (str) - monitor usage type (e.g. indoor vs. outdoor)
    def addSite(self, site_id, city_id, longitude, latitude, name, usage_code):
        query = """
        
        INSERT INTO  site(
            site_id,
            city_id,
            location,
            name,
            usage_code
        )
        VALUES (
            %s,
            %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s,
            %s
        )
        ON CONFLICT (site_id) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    site_id, 
                    city_id, 
                    longitude, 
                    latitude,
                    name,
                    usage_code
                ) 
            )
        self.conn.commit()
  
    # insert sensor record in sensor table
    # INPUTS:
    #    sensor_id (int) - unique sensor id
    #    product_model_id (str) - hardware model id
    #    status_code (int) - sensor status code
    #    status_at (TIMESTAMPZ) - local time of last status update
    #    status_message (str) - human readable version of status code
    #    status_description (str) - more details about status code
    #    mode (int) - mode code (e.g. online, offline)
    #    mode_description (stR) - human readable version of mode code 
    def addSensor(self, sensor_id, product_model_id, status_code, status_at, 
                  status_message, status_description, mode, mode_description):
        query = """
        
        INSERT INTO  sensor(
            sensor_id,
            product_model_id,
            status_code,
            status_at,
            status_message,
            status_description,
            mode,
            mode_description
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (sensor_id) 
        DO UPDATE SET
            status_code = EXCLUDED.status_code,
            status_at = EXCLUDED.status_at,
            status_message = EXCLUDED.status_message,
            mode = EXCLUDED.mode,
            mode_description = EXCLUDED.mode_description;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    sensor_id, 
                    product_model_id, 
                    status_code, 
                    self.ensure_utc(status_at),
                    status_message,
                    status_description,
                    mode,
                    mode_description
                )

            )
        self.conn.commit()

    # insert sensor that is not yet active
    # INPUTS:
    #    sensor_id (int) - unique sensor id
    #    product_model_id (str) - hardware model id
    def addSensorPartial(self, sensor_id, product_model_id):
        query = """
        
        INSERT INTO  sensor(
            sensor_id,
            product_model_id
        )
        VALUES (
            %s,
            %s
        )
        ON CONFLICT (sensor_id) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    sensor_id, 
                    product_model_id
                )

            )
        self.conn.commit()
    
    # insert join record into site_sensor_join table
    # INPUTS:
    #    site_id (str) - unique site id
    #    sensor_id (int) - unique sensor id
    #    height (float) - approximate height of sensor above ground, in meters
    #    start_time (TIMESTAMPZ) - UTC time monitor was first activated
    def addSiteSensorJoin(self, site_id, sensor_id, height,start_time):
        # Convert naive datetime to UTC
        print(start_time)
        start_time = self.ensure_utc(start_time)
        print("site_id: %s, start_time: %s" %(site_id,start_time))
        query = """
        
        INSERT INTO  site_sensor_join(
            site_id,
            sensor_id,
            height,
            since
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (site_id, sensor_id) 
        DO UPDATE SET 
            height = EXCLUDED.height,
            since = EXCLUDED.since;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    site_id,
                    sensor_id,
                    height,
                    start_time
                )
            )
        self.conn.commit()
    
    # upsert start_time in site records
    def updateSiteStartTime(self,site_id,start_time):
        query = """
        
        UPDATE site
            SET start_time = %s
            WHERE site_id = %s
        )
        VALUES (
            %s,
            %s
        )
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    self.ensure_utc(start_time),
                    site_id,
                )
            )
        self.conn.commit()

        # upsert start_time in site records
    def upsertLastUpdatedTime(self,site_id,sensor_id,last_updated_time):
        query = """
        UPDATE site_sensor_join
            SET last_updated = %s
            WHERE site_id = %s
            AND sensor_id = %s;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query,
                (self.ensure_utc(last_updated_time), site_id, sensor_id)
            )
        self.conn.commit()

    # insert record into hourly_flow
    # INPUTS:
    #    sensor_id (int) - unique sensor id
    #    site_id (str) - unique site id
    #    moment (TIMESTAMPTZ) - UTC timestamp of hourly measurement
    #    cubic_meters (float) - amount of airflow in cubic meters
    def insertHourlyFlow(self,sensor_id, site_id, moment, cubic_meters):
        query = """
        INSERT INTO hourly_flow(
            sensor_id,
            site_id,
            moment,
            cubic_meters
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (sensor_id, moment) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    sensor_id,
                    site_id,
                    self.ensure_utc(moment),
                    cubic_meters
                )
            )
        self.conn.commit()

    # insert weekly QA record into site_weekly_qa table
    def addSiteWeeklyQA(self):
        return 0

    # insert weekly QA record into city_weekly_qa table
    def addCityWeeklyQA(self):
        return 0
    
    # insert category record into category table
    # INPUTS:
    #    name (str) - Pollen Sense name assigned to category
    #    group_code (str) - parent that code belongs to in hierarchical clustering
    #    description (str) - species (or group) latin name
    #    common_name (str) - species (or group) common name
    #    root_group_code (str) - root parent that code belongs to in hierarchical clustering
    def addCategory(self, name, group_code, description, common_name, root_group_code):
        query = """
        INSERT INTO  category(
            name,
            group_code,
            description,
            common_name,
            root_group_code
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (name) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    name, 
                    group_code, 
                    description, 
                    common_name,
                    root_group_code,
                ) 
            )
        self.conn.commit()

    # update start time for sites
    # INPUTS:
    #    site_id (str) - unique site id
    #    new_start_time (TIMESTAMPTZ) - new start time in UTC time
    def updateStartTimeIfEarlier(self, site_id, new_start_time):
        # Update start_time only if new_start_time is earlier than existing.
        # Does NOT try to insert a new row.
    
        query = """
            UPDATE site
            SET start_time = %s
            WHERE site_id = %s
            AND (%s < start_time OR start_time IS NULL);
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (self.ensure_utc(new_start_time),site_id, self.ensure_utc(new_start_time))
            )
        self.conn.commit()

    # update last updated time for site_sensor combination
    # INPUTS:
    #    site_id (str) - unique site id
    #    sensor_id (int) - unique sensor id
    #    new_update_time (TIMESTAMPTZ) - new update time in UTC time
    def updateSensorLastUpdated(self, site_id, sensor_id, new_update_time):
        # Update update time only if new_update_time is later than existing.
        # Does NOT try to insert a new row.
    
        query = """
            UPDATE site_sensor_join
            SET last_updated = %s
            WHERE site_id = %s
            AND sensor_id = %s
            AND (%s >= last_updated OR last_updated IS NULL);
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (self.ensure_utc(new_update_time),site_id, sensor_id, self.ensure_utc(new_update_time))
            )
        self.conn.commit()

    # insert hourly metric into hourly_metrics table
    # INPUTS:
    #    moment (TIMESTAMPTZ) - UTC time of hourly metric
    #    category_id (int) - id key for measurement category (e.g. mold, maple)
    #    value (float) - pollen count
    #    sensor_id (int) - unique sensor id
    def addHourlyMetric(self,moment,category_id,value,sensor_id):
        query = """
        INSERT INTO hourly_metrics(
            moment,category_id,pcount,sensor_id
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (moment,category_id,sensor_id) DO NOTHING;
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (
                    self.ensure_utc(moment), 
                    category_id, 
                    value, 
                    sensor_id,
                ) 
            )
        self.conn.commit()

    # update end date for site_sensor_join record
    # INPUTS:
    #    siteId (str) - unique site id
    #    sensorId (int) - unique sensor id
    #    endDate (TIMESTAMPTZ) - date sensor was unprovisioned from site
    def updateSiteSensorEndDates(self,siteId,sensorId,endDate):
        query = """
            UPDATE site_sensor_join
            SET stop_time = %s
            WHERE site_id = %s
            AND sensor_id = %s
            AND (%s >= stop_time or stop_time IS NULL);
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (self.ensure_utc(endDate),siteId, sensorId, self.ensure_utc(endDate))
            )
        self.conn.commit()

    
    # given a set of lat/lon coordinates, identify
    # the nearest city in the city table
    # INPUTS:
    #    longitude (float) - coordinate longitudde
    #    latitude (float) - coordinate latittude
    # OUTPUTS:
    #    city_id (INT) - US Census GEOID for the nearest city
    def getNearestCityId(self, longitude, latitude):
        query = """
        SELECT city_id
        FROM city
        ORDER BY center_location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1;
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (longitude, latitude))
            row = cur.fetchone()
            if row:
                return row[0]
            return None
    
    # get start date of sensor provisioned to site. 
    # if multiple sensors are provisioned, returns earliest provision date
    # only works prospectively, 
    # OUTPUTS:
    #    tuples of
                # site_id (str) - unique site id
                # since (TIMESTAMPTZ) - earliest provision date in local time
    def getSiteStarts(self):
        query = """
        SELECT site_id, MIN(since) AS earliest_since
        FROM site_sensor_join
        GROUP BY site_id;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()  # list of tuples
        return results
    
    # get active sensor_site combinations for querying Pollen Sense API
    # OUTPUTs:
    #    tuples of
                # site_id (str) - unique site id
                # sensor_id (int) - unique sensor id
                # since (TIMESTAMPTZ) - when sensor was provisioned to site
                # last_updated (TIMESTAMPTZ) - most recent hourly record from
                #                              site_sensor combo in SQL database
    def getActiveSensorSites(self):
        query = """
        SELECT site_id, sensor_id, since, last_updated 
        FROM site_sensor_join 
        WHERE stop_time is null;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()  # list of tuples
        return results
  
    
    # get historical sensor_site combinations for querying Pollen Sense API
    # OUTPUTs:
    #    tuples of
                # site_id (str) - unique site id
                # sensor_id (int) - unique sensor id
                # since (TIMESTAMPTZ) - when sensor was provisioned to site
                # last_updated (TIMESTAMPTZ) - most recent hourly record from
                #                              site_sensor combo in SQL database
    def getHistoricalSensorSites(self):
        query = """
        SELECT site_id, sensor_id, since, last_updated 
        FROM site_sensor_join 
        WHERE stop_time is not null;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()  # list of tuples
        return results
    

    # create a category lookup table 
    # OUTPUTS:
    #    lookup (dict) - lookup table of {category name: category_id} values
    def getCategoryLookup(self):
        query = """
            SELECT name, category_id
            FROM category;
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        # Convert to dictionary
        lookup = {name: category_id for name, category_id in rows}
        
        return lookup

    def getNullPPMs(self):
        query = """
            SELECT *
            FROM hourly_flow f
            JOIN hourly_metrics m
            ON f.sensor_id = m.sensor_id
            AND f.moment = m.moment
            WHERE m.ppm IS NULL;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()  # list of tuples
        return results

    def ensureDailyMetricsTable(self):
        query = """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            site_id VARCHAR(100) NOT NULL REFERENCES site(site_id),
            metric_date DATE NOT NULL,
            allergen_type VARCHAR(50) NOT NULL,
            pcount DOUBLE PRECISION NOT NULL CHECK (pcount >= 0),
            cubic_meters DOUBLE PRECISION NOT NULL CHECK (cubic_meters > 0),
            concentration DOUBLE PRECISION NOT NULL CHECK (concentration >= 0),
            n_hours INTEGER NOT NULL CHECK (n_hours >= 1 AND n_hours <= 24),
            n_flow_measurements INTEGER NOT NULL CHECK (n_flow_measurements >= 1),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (site_id, metric_date, allergen_type)
        );

        ALTER TABLE daily_metrics
            ADD COLUMN IF NOT EXISTS n_flow_measurements INTEGER;

        UPDATE daily_metrics
            SET n_flow_measurements = n_hours
            WHERE n_flow_measurements IS NULL;

        ALTER TABLE daily_metrics
            ALTER COLUMN n_flow_measurements SET NOT NULL;

        ALTER TABLE daily_metrics
            DROP CONSTRAINT IF EXISTS daily_metrics_n_flow_measurements_check;

        ALTER TABLE daily_metrics
            ADD CONSTRAINT daily_metrics_n_flow_measurements_check
            CHECK (n_flow_measurements >= 1);

        CREATE INDEX IF NOT EXISTS idx_daily_metrics_site_date
            ON daily_metrics (site_id, metric_date);

        CREATE INDEX IF NOT EXISTS idx_daily_metrics_allergen
            ON daily_metrics (allergen_type);
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

    # calculate daily concentrations for monitor/allergen combinations
    # INPUTS:
    #    start_date (date-like) - first daily date to calculate, inclusive
    #    end_date (date-like) - last daily date to calculate, inclusive
    #    day_timezone (str) - timezone used to assign hourly records to dates
    #    min_cubic_meters (float) - minimum daily flow required for output
    # OUTPUTS:
    #    int - number of upserted daily metric rows
    def upsertDailyConcentrations(
        self,
        start_date=None,
        end_date=None,
        day_timezone="UTC",
        min_cubic_meters=0.0,
    ):
        self.ensureDailyMetricsTable()
        query = """
        WITH category_matches AS (
            SELECT category_map.allergen_type, c.category_id
            FROM (
                VALUES
                    ('Total Pollen', 'POL'),
                    ('Total Tree Pollen', 'TRE'),
                    ('Quercus (Oak)', 'QUE'),
                    ('Cupressaceae (Cypress)', 'CUP'),
                    ('Morus (Mulberry)', 'MOR'),
                    ('Ulmus (Elm)', 'ULM'),
                    ('Fraxinus (Ash)', 'FRA'),
                    ('Betula (Birch)', 'BET'),
                    ('Acer (Maple)', 'ACE'),
                    ('Populus (Poplar)', 'POP'),
                    ('Pinaceae (Pine)', 'PIN'),
                    ('Total Grass Pollen', 'GRA'),
                    ('Ambrosia (Ragweed)', 'AMB-IVA'),
                    ('Poaceae (Grasses)', 'POA'),
                    ('Total Mold', 'MOL')
            ) AS category_map(allergen_type, category_code)
            JOIN category c
              ON c.name = category_map.category_code
        ),
        allergen_types AS (
            SELECT DISTINCT allergen_type
            FROM category_matches
        ),
        flow_hours AS (
            SELECT
                f.site_id,
                f.sensor_id,
                f.moment,
                (f.moment AT TIME ZONE %s)::date AS metric_date,
                f.cubic_meters
            FROM hourly_flow f
            WHERE f.cubic_meters > 0
              AND (%s::date IS NULL OR (f.moment AT TIME ZONE %s)::date >= %s::date)
              AND (%s::date IS NULL OR (f.moment AT TIME ZONE %s)::date <= %s::date)
        ),
        hourly_allergen AS (
            SELECT
                m.sensor_id,
                m.moment,
                cm.allergen_type,
                SUM(m.pcount) AS pcount
            FROM hourly_metrics m
            JOIN category_matches cm
              ON cm.category_id = m.category_id
            GROUP BY m.sensor_id, m.moment, cm.allergen_type
        ),
        daily AS (
            SELECT
                fh.site_id,
                fh.metric_date,
                at.allergen_type,
                SUM(COALESCE(ha.pcount, 0)) AS pcount,
                SUM(fh.cubic_meters) AS cubic_meters,
                SUM(COALESCE(ha.pcount, 0)) / SUM(fh.cubic_meters) AS concentration,
                COUNT(DISTINCT fh.moment) AS n_hours,
                COUNT(*) AS n_flow_measurements
            FROM flow_hours fh
            CROSS JOIN allergen_types at
            LEFT JOIN hourly_allergen ha
              ON ha.sensor_id = fh.sensor_id
             AND ha.moment = fh.moment
             AND ha.allergen_type = at.allergen_type
            GROUP BY fh.site_id, fh.metric_date, at.allergen_type
            HAVING SUM(fh.cubic_meters) > %s
        )
        INSERT INTO daily_metrics (
            site_id,
            metric_date,
            allergen_type,
            pcount,
            cubic_meters,
            concentration,
            n_hours,
            n_flow_measurements
        )
        SELECT
            site_id,
            metric_date,
            allergen_type,
            pcount,
            cubic_meters,
            concentration,
            n_hours,
            n_flow_measurements
        FROM daily
        ON CONFLICT (site_id, metric_date, allergen_type)
        DO UPDATE SET
            pcount = EXCLUDED.pcount,
            cubic_meters = EXCLUDED.cubic_meters,
            concentration = EXCLUDED.concentration,
            n_hours = EXCLUDED.n_hours,
            n_flow_measurements = EXCLUDED.n_flow_measurements,
            updated_at = NOW();
        """

        params = (
            day_timezone,
            start_date,
            day_timezone,
            start_date,
            end_date,
            day_timezone,
            end_date,
            min_cubic_meters,
        )

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            upserted = cur.rowcount
        self.conn.commit()
        return upserted

    # retrieve daily concentrations already calculated in daily_metrics
    def getDailyConcentrations(self, start_date=None, end_date=None, allergen_types=None):
        query = """
            SELECT
                site_id,
                metric_date,
                allergen_type,
                pcount,
                cubic_meters,
                concentration,
                n_hours,
                n_flow_measurements
            FROM daily_metrics
            WHERE (%s::date IS NULL OR metric_date >= %s::date)
              AND (%s::date IS NULL OR metric_date <= %s::date)
              AND (%s::text[] IS NULL OR allergen_type = ANY(%s::text[]))
            ORDER BY site_id, metric_date, allergen_type;
        """

        params = (
            start_date,
            start_date,
            end_date,
            end_date,
            allergen_types,
            allergen_types,
        )

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            results = cur.fetchall()
        return results
    
    def getAllSiteIds(self):
        query = """
        SELECT DISTINCT site_id
        FROM site_sensor_join;
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()  # list of tuples
        return results

   ### SQL API isn't needed yet for retrieval operations.

    # # get properties for a city from the city table
    # def getCity(self,cityId):
    #     return 0
    
    # # get properties for a site from the site table
    # def getSite(self, siteId):
    #     return 0
    
    # # get properties for a sensor from the sensor table
    # def getSensor(self, sensorId):
    #     return 0
    
    # # get properties for a category from the category table
    # def getCategory(self, categoryId):
    #     return 0

# end of SQLAPI.py
