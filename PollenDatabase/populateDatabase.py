### populateDatabase.py
### Author: Andrew Larkin
### Date created: April 2, 2026
### Summary: pouplate SQL database of pollen sense measurements

# import libraries
import requests
import pandas as ps
import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from datetime import timedelta

# import custom classes and environments
GIT_PATH = "C:/Users/larki/Documents/GitHub/PollenModeling/PollenDatabase/"
sys.path.append(GIT_PATH)

from PollenSenseAPI import PollenAPI
from SQLAPI import SQLAPI
load_dotenv(dotenv_path=GIT_PATH + ".env")

# initialize connection to SQL database
SQL = SQLAPI(
    os.getenv("DB_NAME"),
    os.getenv("DB_USER"),
    os.getenv("DB_PW"),
    os.getenv("DB_HOST"),
    os.getenv("DB_PORT")
)
SQL.isConnected()

# initialize connection to Pollen Sense API using the developer key
pollen = PollenAPI(os.getenv("DEVELOPER_API"),"https://sensors.pollensense.com/api")

# populate the city table
# INPUTS:
#    cityCSV (str) - absolute filepath to csv containing city data
#    SQL (SQLAPI class) - custom SQL API object
def populateCities(cityCSV,SQL):
    cityData = ps.read_csv(cityCSV)
    cityData = cityData[['NAME','INTPTLAT','INTPTLON','GEOID']]
    nPoints = cityData.count().iloc[0]
    print("number of city records: %i" %(nPoints))
    for row in range(nPoints):
        curCity = cityData.iloc[row]
        try:
            SQL.addCity(int(curCity['GEOID']),curCity['NAME'],float(curCity['INTPTLON']),float(curCity['INTPTLAT']))
        except Exception as e:
            print(str(e))

# populate/update the site table
# INPUTS:
#    pollen (PollenAPI class) - custom Pollen API object
#    SQL (SQLAPI class) - custom SQLAPI object
def populateSites(pollen,SQL):
    sites = pollen.getSites()
    nSites = sites.count().iloc[0]
    for siteNum in range(nSites):
        curSite = sites.iloc[siteNum]
        nearestCityId = SQL.getNearestCityId(curSite['Longitude'],curSite['Latitude'])
        SQL.addSite(
            curSite['SiteId'],
            nearestCityId,
            curSite['Longitude'],
            curSite['Latitude'],
            curSite['Name'],
            curSite['UsageCode']
        )

# populate/update the category table
# INPUTS:
#    pollen (PollenAPI class) - custom Pollen API object
#    SQL (SQLAPI class) - custom SQLAPI object
def populateCategories(pollen,SQL):
    categories = pollen.getCategories()
    nCategories = categories.count().iloc[0]
    for categoryNum in range(nCategories):
        curCategory = categories.iloc[categoryNum]
        SQL.addCategory(curCategory['Code'],curCategory['GroupCode'],curCategory['Description'],
                          curCategory['CommonName'],curCategory['RootGroupCode'])
        
# populate/update the sensors table
# INPUTS:
#    pollen (PollenAPI class) - custom Pollen API object
#    SQL (SQLAPI class) - custom SQLAPI object
def populateSensors(pollen,SQL):
    sensors = pollen.getSensors()
    #sensors = sensors[sensors['StatusCode']>-999]
    nSensors = sensors.count().iloc[0]
    print("number of sensors with a status code: %i" %(nSensors))
    for sensorNum in range(nSensors):
        curSensor = sensors.iloc[sensorNum]
        if ps.isna(curSensor['StatusCode']):
            SQL.addSensorPartial(int(curSensor['SensorId']),curSensor['ProductModelId'])
        else:
            SQL.addSensor(
                int(curSensor['SensorId']),curSensor['ProductModelId'],int(curSensor['StatusCode']),
                curSensor['StatusAt'],curSensor['StatusMessage'],curSensor['StatusDescription'],
                int(curSensor['Mode']),curSensor['ModeDescription']
            )

# populate/update the site_sensor_join table
# INPUTS:
#    pollen (PollenAPI class) - custom Pollen API object
#    SQL (SQLAPI class) - custom SQLAPI object
def populateSiteSensorJoin(pollen,SQL):
    sensors = pollen.getSensors()
    #sensors = sensors[sensors['StatusCode']>-999]
    #sensors = sensors[sensors['Height']>-999]
    nSensors = sensors.count().iloc[0]
    print("number of sensors with a site and status code: %i" %(nSensors))
    for sensorNum in range(nSensors):
        curSensor = sensors.iloc[sensorNum]
        SQL.addSiteSensorJoin(curSensor['SiteId'],int(curSensor['SensorId']),float(curSensor['Height']),
                             curSensor['Since'])
        
# populate/update the hourly_metrics table
# INPUTS:
#    sensorMetrics (pandas dataframe) - metrics for one site
#    SQL (SQLAPI class) - custom SQLAPI object
#    sensorId (int) - unique sensor id
#    categoryLookup (dict) - key value pairs of {lookup name (str) : lookup_id (int)}
def populateHourlyMetricsOneSensorSite(sensorMetrics,SQL,sensorId,categoryLookup):
    validMeasures = sensorMetrics.dropna(subset=["value"])
    nMeasures = validMeasures.count().iloc[0]
    print("sensor %i has %i new measures" %(sensorId,nMeasures))
    for measureNum in range(nMeasures):
        curMeasure = validMeasures.iloc[measureNum]
        category_id = categoryLookup[curMeasure['category']]
        SQL.addHourlyMetric(curMeasure['moment'],int(category_id),float(curMeasure['value']),int(sensorId))

# populate/update the hourly_flow table
# INPUTS:
#    SQL (SQL API class) - custom SQLAPI object
#    siteId (str) - unique site id
#    sensorId (int) - unique sensor id
#    flowMetrics (pandas dataframe) - flow metrics to add to table
def poulateHourlyFlow(SQL,siteId,sensorId,flowMetrics):
    validFlow = flowMetrics.dropna(subset=['cubic_meters'])
    uniqueFlow = validFlow.drop_duplicates(subset=["moment","cubic_meters"])
    nUniques = uniqueFlow.count().iloc[0]
    for uniqueNum in range(nUniques):
        curFlowMeas = uniqueFlow.iloc[uniqueNum]
        SQL.insertHourlyFlow(sensorId,siteId,curFlowMeas['moment'],curFlowMeas['cubic_meters'])
    lastUpdated = uniqueFlow["moment"].max()
    SQL.upsertLastUpdatedTime(siteId,sensorId,lastUpdated)

def updateSiteSensorEndDates(SQL,siteId,sensorId,endDate):
    return 0


# return the most recent datetime. Handles None (NULL) and timezone-aware datetimes
# INPUTS:
#    dt1 (datetime)
#    dt2 (datetime)
def getMostRecent(dt1, dt2):

    # Handle NULLs (None)
    if dt1 is None:
        return dt2
    if dt2 is None:
        return dt1

    # Both exist → safe to compare
    return max(dt1, dt2)

# limit API queries to one week of metrics. Otherwise may get a 400 error code
# INPUTS:
#    startTime (datetime) - start of query window
#    endTime (datetime) - end of query window
def capOneWeek(startTime, endTime):

    oneWeekLater = startTime + relativedelta(weeks=1)

    if endTime > oneWeekLater:
        return oneWeekLater, True
    else:
        return endTime, False
    
# update hourly metrics of active sensors
# INPUTS:
#    pollen (PollenAPI class) - custom Pollen API object
#    SQL (SQLAPI class) - custom SQLAPI object
def updateActiveSensorHourly(SQL,pollen):
    categoryLookup = SQL.getCategoryLookup()
    activeSensors = SQL.getActiveSensorSites()
    nActive = len(activeSensors)
    now = datetime.now(timezone.utc)
    now = now.replace(minute=0, second=0, microsecond=0)
    for activeNum in range(nActive):
        curActive = activeSensors[activeNum]
        mostRecent = getMostRecent(curActive[2],curActive[3])
        dt_utc = mostRecent.astimezone(timezone.utc) + timedelta(hours=1)
        oneWeek = True
        while(oneWeek):
            queryEnd, oneWeek = capOneWeek(dt_utc,now)
            if(dt_utc != queryEnd):
                print("updating sensor %i" %(curActive[1]))
                curMetrics = pollen.getHourlyMetricsSiteSensor(curActive[0],curActive[1],dt_utc,queryEnd)
                if not(curMetrics.empty):
                    populateHourlyMetricsOneSensorSite(curMetrics,SQL,curActive[1],categoryLookup)
                    poulateHourlyFlow(SQL,curActive[0],curActive[1],curMetrics)
            dt_utc = queryEnd


if __name__ == "__main__":
    populateCities(GIT_PATH + "CBSA_Table.csv",SQL)
    populateSites(pollen,SQL)
    populateSensors(pollen,SQL)
    populateSiteSensorJoin(pollen,SQL)
    updateActiveSensorHourly(SQL,pollen)