import requests
import pandas as pd

GBFS_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

def fetch_gbfs_feeds():
    """Fetches the available feeds from the API and gives it to a json file."""
    response = requests.get(GBFS_URL)               #send get request to the URL
    response.raise_for_status()                     #check for http errors
    feeds = response.json()["data"]["en"]["feeds"]  #parse the json file and extract the feeds
    return {feed["name"]: feed["url"] for feed in feeds} #return the feeds

def fetch_station_information(station_info_url):
    """Fetches and parses station information."""
    response = requests.get(station_info_url)   #send get request to the URL provided by fetch_gbfs_feeds
    response.raise_for_status()                 #check for http errors 
    data = response.json()["data"]["stations"]  #parse the json file and extract the station information
    df = pd.DataFrame(data)                     #convert the data to a pandas dataframe
    return df

def fetch_station_status(station_status_url):
    """Fetches and parses station status."""
    response = requests.get(station_status_url)  #send get request to the URL provided by fetch_gbfs_feeds
    response.raise_for_status()                  #check for http errors
    data = response.json()["data"]["stations"]   #parse the json file and extract the station status data
    df = pd.DataFrame(data)                      #convert the data to a pandas dataframe
    return df

def main():
    feeds = fetch_gbfs_feeds()
    
    #fetch station information
    station_info_url = feeds["station_information"]                 #get the url for station information
    station_info_df = fetch_station_information(station_info_url)   #fetch the station information
    print("Station Information:") 
    print(station_info_df.head())                                   #print the first 5 rows of the dataframe
    
    #fetch station status
    station_status_url = feeds["station_status"]                    #get the url for station status
    station_status_df = fetch_station_status(station_status_url)    #fetch the station status
    print("\nStation Status:") 
    print(station_status_df.head())                                 #print the first 5 rows of the dataframe

if __name__ == "__main__":
    main()
