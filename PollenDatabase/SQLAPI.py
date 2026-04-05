### SQLAPI.py
### Author: Andrew Larkin
### Date Created: March 27, 2026
### Summary: Custom wrapper for performing CRUD operations on a PostGIS database

################ Import Libraries ##################

import psycopg2
from psycopg2 import sql
import pytz
from datetime import datetime


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
                    status_at,
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
        start_time = datetime.fromisoformat(start_time)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=pytz.UTC)
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
        
        UPDATE site(
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
                    start_time,
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
                (last_updated_time, site_id, sensor_id)
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
                    moment,
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
    #    new_start_time (TIMESTAMPTZ) - new start time in UCT time
    def updateStartTimeIfEarlier(self, site_id, new_start_time):
        # Update start_time only if new_start_time is earlier than existing.
        # Does NOT try to insert a new row.
    
        query = """
            UPDATE site
            SET start_time = %s
            WHERE site_id = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(
                query, 
                (new_start_time,site_id, new_start_time)
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
                (new_start_time,site_id, new_start_time)
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
                (new_update_time,site_id, sensor_id, new_update_time)
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
                    moment, 
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
                (endDate,siteId, sensorId, endDate)
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