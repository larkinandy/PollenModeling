### PollenSenseAPI.py 
### Author: Andrew Larkin
### Date Created: March 27, 2026
### Summary: Custom wrapper for querying the Pollen Sense API

################ Import Libraries ##################

import requests
from requests.exceptions import HTTPError
import pandas as ps
import json

class PollenAPI:


    # initialize class
    # INPUTS:
    #    API_KEY (str) - Pollen Sense API key - unique for each app
    #    url (str) - api url
    def __init__(self,API_KEY,url):
        self.API_KEY = API_KEY
        self.url = url

    # get sites and corresponding properties
    # OUTPUTS:
    #    siteData (pandas dataframe) - site ids and metadata
    def getSites(self):
        queryURL = self.url + "/sites"
        headers = {
            "accept": "application/json",
            "x-ps-key": self.API_KEY
        }

        try:
            # Make the GET request
            response = requests.get(queryURL,headers=headers)

            # Check for success
            response.raise_for_status()
        
        except HTTPError as http_err:
            print(f'HTTP error occured: {http_err}')
            return None

        except requests.exceptions.RequestException as err:
            print(f'An error occurred: {err}')
            return None

        # Parse JSON
        siteData = ps.DataFrame(response.json())
        return(siteData)

    # get sensors and corresponding properties
    # OUTPUTS:
    #    sensorData (pandas dataframe) - sensor ids and metadata
    def getSensors(self):
        queryURL = self.url + "/sensors"
        headers = {
            "accept": "application/json",
            "x-ps-key": self.API_KEY
        }

        try:
            # Make the GET request
            response = requests.get(queryURL,headers=headers)

            # Check for success
            response.raise_for_status()
        
        except HTTPError as http_err:
            print(f'HTTP error occured: {http_err}')
            return None

        except requests.exceptions.RequestException as err:
            print(f'An error occurred: {err}')
            return None

        # Parse JSON
        sensorData = ps.DataFrame(response.json())
        return(sensorData)
    
    # get categories and corresponding properties
    # OUTPUTS:
    #    categoryData (pandas dataframe) - categories and properties
    def getCategories(self):
        queryURL = self.url + "/v2/categories"
        headers = {
            "accept": "application/json",
            "x-ps-key": self.API_KEY
        }

        try:
            # Make the GET request
            response = requests.get(queryURL,headers=headers)

            # Check for success
            response.raise_for_status()
        
        except HTTPError as http_err:
            print(f'HTTP error occured: {http_err}')
            return None

        except requests.exceptions.RequestException as err:
            print(f'An error occurred: {err}')
            return None

        # Parse JSON
        categoryData = ps.DataFrame(response.json())
        return(categoryData)
    
    # get hourly metrics for a single site and specific timeframe
    # INPUTS:
    #    siteId (str) - unique site id 
    #    starttime (datetime) - start time for metric window (inclusive)
    #    endTime (datetime) - end time for metric window (inclusive)
    # OUTPUTS:
    #    siteMetrics (pandas dataframe) - metrics for the corresponding site and time window
    def getHourlyMetricsSiteSensor(self,site_id,sensor_id,startTime,endTime):
        startTimeString = str(startTime.year) + "-" + str(startTime.month).zfill(2) + "-" + str(startTime.day).zfill(2) + "T" + str(startTime.hour).zfill(2) + "%3A00%3A00"
        endTimeString = str(endTime.year) + "-" + str(endTime.month).zfill(2) + "-" + str(endTime.day).zfill(2) + "T" + str(endTime.hour).zfill(2) + "%3A00%3A00"
        queryURL = self.url + "/v2/sites/" + site_id + "/metrics?interval=hour"
        queryURL += "&starting=" + startTimeString + "&ending=" + endTimeString
        queryURL += "&sensorId=" + str(sensor_id)
        headers = {
            "accept": "application/json",
            "x-ps-key": self.API_KEY
        }

        try:
            # Make the GET request
            response = requests.get(queryURL,headers=headers)

            # Check for success
            response.raise_for_status()
        
        except HTTPError as http_err:
            print(f'HTTP error occured: {http_err}')
            return None

        except requests.exceptions.RequestException as err:
            print(f'An error occurred: {err}')
            return None

        df = self.json_to_dataframe(response.json())
        return(df)


    def json_to_dataframe(self,data):
        moments = data["Moments"]
        cubic_meters = data["CubicMeters"]
        layers = data["Layers"]

        rows = []

        for layer in layers:
            layer_id = layer.get("Layer")
            counts = layer.get("Counts", {})

            for category, values in counts.items():
                for i, value in enumerate(values):
                    rows.append({
                        "moment": moments[i],
                        "layer": layer_id,
                        "category": category,
                        "value": value,
                        "cubic_meters": cubic_meters[i]
                    })

        df = ps.DataFrame(rows)
        df["moment"] = ps.to_datetime(df["moment"])

        return df

    

# end of PollenSenseAPI.py