import flask
from flask import jsonify
import requests
import pandas as pd
import logging
import time
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

#configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#initialize Flask application
app = flask.Flask(__name__)

#bike Share API client
class BikeShareAPIClient:
    def __init__(self, base_url="https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"):
        #base URL for GBFS (General Bikeshare Feed Specification)
        self.base_url = base_url
        #caching mechanism to reduce unnecessary API calls
        self.cached_urls = {}
        self.cache_expiry = 300  # 5 minutes cache expiration
        self.last_fetch_time = 0
        
        #rate limiting parameters
        self.max_retries = 3
        self.retry_delay = 5  #seconds between retries
        self.rate_limit_delay = 60  #seconds to wait after rate limit error

    def _fetch_feed_urls(self):
        #check if cached URLs are still valid
        current_time = time.time()
        if (self.cached_urls and 
            current_time - self.last_fetch_time < self.cache_expiry):
            return self.cached_urls
        
        try:
            #send GET request with timeout
            response = requests.get(self.base_url, timeout=10)
            
            #check for HTTP errors
            response.raise_for_status()
            
            #extract feeds from the JSON response
            feeds = response.json()["data"]["en"]["feeds"]
            
            #find URLs for station information and status
            self.cached_urls = {
                'station_info': next((feed["url"] for feed in feeds if feed["name"] == "station_information"), None),
                'station_status': next((feed["url"] for feed in feeds if feed["name"] == "station_status"), None)
            }
            
            #update last fetch time
            self.last_fetch_time = current_time
            
            return self.cached_urls
        
        except Timeout:
            #handle request timeout
            logger.warning("Timeout fetching feed URLs. Retrying...")
            time.sleep(self.retry_delay)
        
        except ConnectionError:
            #handle network connection issues
            logger.error("Network connection error. Check internet connectivity.")
        
        except HTTPError as e:
            if e.response.status_code == 429:
                #handle rate limiting
                logger.warning("Rate limit exceeded. Waiting before retry.")
                time.sleep(self.rate_limit_delay)
            else:
                logger.error(f"HTTP error occurred: {e}")
        
        except RequestException as e:
            #catch any other request-related exceptions
            logger.error(f"Request error: {e}")
        
        return None

    def get_station_information(self):
        #fetch feed URLs
        urls = self._fetch_feed_urls()
        
        if not urls or not urls['station_info']:
            logger.error("Could not retrieve station information URL")
            return None
        
        try:
            #send GET request to station information URL
            response = requests.get(urls['station_info'], timeout=10)
            response.raise_for_status()
            
            #extract stations data from JSON response
            stations = response.json()["data"]["stations"]
            
            #create DataFrame with specific columns
            df = pd.DataFrame(stations)[['station_id', 'name', 'lat', 'lon', 'capacity']]
            
            return df
        
        except Exception as e:
            #handle any errors during data retrieval or processing
            logger.error(f"Error fetching station information: {e}")
            return None

    def get_station_status(self):
        #fetch feed URLs
        urls = self._fetch_feed_urls()
        
        if not urls or not urls['station_status']:
            logger.error("Could not retrieve station status URL")
            return None
        
        try:
            #send GET request to station status URL
            response = requests.get(urls['station_status'], timeout=10)
            response.raise_for_status()
            
            #extract stations data from JSON response
            stations = response.json()["data"]["stations"]
            
            #create DataFrame with specific columns
            df = pd.DataFrame(stations)[['station_id', 'num_bikes_available', 'num_docks_available']]
            
            return df
        
        except Exception as e:
            #handle any errors during data retrieval or processing
            logger.error(f"Error fetching station status: {e}")
            return None

#initialize Bike Share API client
bike_share_client = BikeShareAPIClient()

#route to retrieve station information
@app.route('/station/information', methods=['GET'])
def get_station_information():
    #fetch station information
    stations_df = bike_share_client.get_station_information()
    
    if stations_df is not None:
        #convert DataFrame to JSON for API response
        return stations_df.to_json(orient='records')
    else:
        #return error response
        return jsonify({
            "error": "Unable to retrieve station information",
            "status": "500 Internal Server Error"
        }), 500

#route to retrieve station status
@app.route('/station/status', methods=['GET'])
def get_station_status():
    #fetch station status
    stations_df = bike_share_client.get_station_status()
    
    if stations_df is not None:
        #convert DataFrame to JSON for API response
        return stations_df.to_json(orient='records')
    else:
        #return error response
        return jsonify({
            "error": "Unable to retrieve station status",
            "status": "500 Internal Server Error"
        }), 500

#run the Flask application if script is executed directly
if __name__ == '__main__':
    app.run(port=5001)