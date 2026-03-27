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
    def addCity(self):
        return 0
    
    # insert site record into site table
    def addSite(self):
        return 0
    
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

    # get properties for a city from the city table
    def getCity(cityId):
        return 0
    
    # get properties for a site from the site table
    def getSite(siteId):
        return 0
    
    # get properties for a sensor from the sensor table
    def getSensor(sensorId):
        return 0
    
    # get properties for a category from the category table
    def getCategory(categoryId):
        return 0

# end of SQLAPI.py