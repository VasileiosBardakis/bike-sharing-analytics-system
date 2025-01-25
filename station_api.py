import flask
from flask import jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import pandas as pd
import logging
import time
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError
from confluent_kafka import Producer
import json


#configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#initialize Flask application
app = flask.Flask(__name__)



#configure rate limiter
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["100 per day", "30 per hour"],
    storage_uri="memory://"
)

# Lazy initialization of Kafka producer
producer = None

def get_producer():
    global producer
    if producer is None:
        try:
            producer_config = {
                'bootstrap.servers': 'localhost:9092',
                'socket.timeout.ms': 10000,
                'retries': 3,
            }
            producer = Producer(producer_config)
            logger.info("Kafka producer initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            producer = None  # Ensure producer is None if initialization fails
    return producer


#Function to send the message to kafka
def send_to_kafka(topic, data):
    kafka_producer = get_producer()
    if kafka_producer is None:
        logger.warning("Kafka producer is not available. Skipping message production.")
        return
    try:
        # Send each record in the data as a newline-delimited JSON message
        for record in data:
            message = json.dumps(record)  # Convert each record to JSON string
            kafka_producer.produce(topic, value=message.encode('utf-8'))  # Convert JSON string to bytes
        
        # Ensure all messages are sent
        kafka_producer.flush()
        logger.info(f"Successfully sent data to Kafka topic: {topic}")
    except Exception as e:
        # Log any errors that occur while sending to Kafka
        logger.error(f"Error sending data to Kafka topic '{topic}': {e}")

def send_status_to_kafka(topic, data):
    kafka_producer = get_producer()
    if kafka_producer is None:
        logger.warning("Kafka producer is not available. Skipping message production.")
        return
    try:
        records = json.loads(data)
        
        # Send each record as an individual Kafka message
        for record in records:
            message = json.dumps(record)  # Serialize the record to JSON
            kafka_producer.produce(topic, value=message.encode('utf-8'))  # Send to Kafka
        
        # Ensure all messages are sent
        kafka_producer.flush()
        logger.info(f"Successfully sent station status to Kafka topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending station status to Kafka topic '{topic}': {e}")



#bike Share API client
class BikeShareAPIClient:
    def __init__(self, base_url="https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"):
        #base URL for GBFS (General Bikeshare Feed Specification)
        self.base_url = base_url
        #caching mechanism to reduce unnecessary API calls
        self.cached_urls = {}
        self.cached_station_info = None
        self.cached_station_status = None
        self.cache_expiry = 300  # 5 minutes cache expiration
        self.last_fetch_time = 0
        self.last_station_info_fetch = 0
        self.last_station_status_fetch = 0

    def _fetch_feed_urls(self):
        #check if cached URLs are still valid, if they are return them if not fetch new URLs
        current_time = time.time()
        if (self.cached_urls and current_time - self.last_fetch_time < self.cache_expiry):
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
            logger.error("Timeout fetching bike share feed URLs")
            return None
        
        except ConnectionError:
            #handle network connection issues
            logger.error("Network connection error. Check internet connectivity.")
            return None
        
        except HTTPError as e:
            #catch specific HTTP errors
            logger.error(f"HTTP error occurred while fetching feed URLs: {e}")
            return None
        
        except RequestException as e:
            #catch any other request-related exceptions
            logger.error(f"Unexpected request error: {e}")
            return None


    def get_station_information(self):
        #check current time
        current_time = time.time()
        
        #check if cached data is still valid
        if (self.cached_station_info is not None and 
            current_time - self.last_station_info_fetch < self.cache_expiry):
            return self.cached_station_info
        
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
            
            #cache the result and update fetch time
            self.cached_station_info = df
            self.last_station_info_fetch = current_time
            
            return df
        
        except Exception as e:
            #handle any errors during data retrieval or processing
            logger.error(f"Error fetching station information: {e}")
            return None


    def get_station_status(self):
        #check current time
        current_time = time.time()
        
        #check if cached data is still valid
        if (self.cached_station_status is not None and 
            current_time - self.last_station_status_fetch < self.cache_expiry):
            return self.cached_station_status
        
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
            
            #cache the result and update fetch time
            self.cached_station_status = df
            self.last_station_status_fetch = current_time
            
            return df
        
        except Exception as e:
            #handle any errors during data retrieval or processing
            logger.error(f"Error fetching station status: {e}")
            return None

#initialize Bike Share API client
bike_share_client = BikeShareAPIClient()

#route to retrieve station information
@app.route('/station/information', methods=['GET'])
@limiter.limit("15 per minute")
def get_station_information():
    #fetch station information
    stations_df = bike_share_client.get_station_information()
    
    if stations_df is not None:
        try:
            #Send station information to Kafka using the send_to_kafka function
            send_to_kafka('station_information', stations_df.to_dict(orient='records'))
            #Convert DataFrame to JSON for API response
            return stations_df.to_json(orient='records')
        except Exception as e:
            # Log any errors during Kafka message production
            logger.error(f"Error sending station information to Kafka: {e}")
            return jsonify({
                "error": "Failed to send station information to Kafka",
                "status": "500 Internal Server Error"
            }), 500
    else:
        #return error response
        logger.error("Failed to retrieve station information")
        return jsonify({
            "error": "Unable to retrieve station information",
            "status": "500 Internal Server Error"
        }), 500

#route to retrieve station status
@app.route('/station/status', methods=['GET'])
@limiter.limit("15 per minute")
def get_station_status():
    #fetch station status
    stations_df = bike_share_client.get_station_status()
    
    if stations_df is not None:
        try:
            # Convert the DataFrame to a JSON string for Kafka
            send_status_to_kafka('station_status', stations_df.to_json(orient='records'))
            #convert DataFrame to JSON for API response
            return stations_df.to_json(orient='records')
        except  Exception as e:
            # Log any errors during Kafka message production
            logger.error(f"Error sending station status to Kafka: {e}")
            return jsonify({
                "error": "Failed to send station status to Kafka",
                "status": "500 Internal Server Error"
            }), 500
    else:
        #return error response
        logger.error("Failed to retrieve station status")
        return jsonify({
            "error": "Unable to retrieve station status",
            "status": "500 Internal Server Error"
        }), 500

# Error handler for rate limiting
@app.errorhandler(429)
def ratelimit_handler(e):
    # Log rate limit violations with client IP
    logger.warning(f"Rate limit exceeded for IP: {get_remote_address()}")
    return jsonify({
        "error": "Too many requests. Please slow down.",
        "status": "429 Too Many Requests"
    }), 429

#run the Flask application if script is executed directly
if __name__ == '__main__':
    app.run(port=5001)