### SQLAPI.py
### Author: Andrew Larkin
### Date Created: March 27, 2026
### Summary: Custom wrapper for performing CRUD operations on a PostGIS database

################ Import Libraries ##################

import psycopg2
from psycopg2 import sql

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
                    usage_code) 
            )
        self.conn.commit()
  
    # insert sensor record in sensor table
    def addSensor(self):
        return 0
    
    # insert join record into site_sensor_join table
    def addSiteSensorJoind(self):
        return 0
    
    # insert weekly QA record into site_weekly_qa table
    def addSiteWeeklyQA(self):
        return 0

    # insert weekly QA record into city_weekly_qa table
    def addCityWeeklyQA(self):
        return 0
    
    # insert category record into category table
    def addCategory(self):
        return 0
    
    # insert hourly metric into hourly_metrics table
    def addHourlyMetric(self):
        return 0
    
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