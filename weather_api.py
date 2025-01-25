import flask
from flask import request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import time
import logging
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

def send_weather_to_kafka(topic, data):
    kafka_producer = get_producer()
    if kafka_producer is None:
        logger.warning("Kafka producer is not available. Skipping message production.")
        return
    try:
        # Convert data (dictionary) to JSON string
        message = json.dumps(data)  # Ensure data is serializable
        kafka_producer.produce(topic, value=message.encode('utf-8'))  # Send JSON as bytes
        kafka_producer.flush()  # Ensure the message is sent immediately
        logger.info(f"Successfully sent weather data to Kafka topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending weather data to Kafka topic '{topic}': {e}")

#openWeather API configuration
class OpenWeatherAPIClient:
    def __init__(self, api_key, base_url='http://api.openweathermap.org/data/2.5/weather'):
        self.api_key = api_key
        self.base_url = base_url
        
        # Caching mechanism
        self.cached_weather_data = {}
        self.cache_expiry = 300  # 5 minutes cache expiration

    def get_weather(self, latitude, longitude):
        # Create a unique cache key based on coordinates
        cache_key = f"{latitude},{longitude}"
        current_time = time.time()
        
        # Check if cached data exists and is still valid
        if (cache_key in self.cached_weather_data and 
            current_time - self.cached_weather_data[cache_key]['timestamp'] < self.cache_expiry):
            return self.cached_weather_data[cache_key]
        
        # Construct API request URL
        url = f'{self.base_url}?lat={latitude}&lon={longitude}&appid={self.api_key}&units=metric'
        
        try:
            # Send GET request with timeout
            response = requests.get(url, timeout=10)
            
            # Check for specific HTTP errors
            response.raise_for_status()
            
            # Parse and extract weather data
            data = response.json()
            weather_info = {
                'temperature': data['main']['temp'],
                'precipitation': data.get('rain', {}).get('1h', 0),
                'wind_speed': data['wind']['speed'],
                'clouds': data['clouds']['all'],
                'timestamp': current_time
            }
            
            # Cache the weather data
            self.cached_weather_data[cache_key] = weather_info
            
            return weather_info
        
        except Timeout:
            # Handle request timeout
            logger.error("Request timed out while fetching weather data")
            return None
        
        except ConnectionError:
            # Handle network connection issues
            logger.error("Network connection error. Check internet connectivity.")
            return None
        
        except HTTPError as e:
            if e.response.status_code == 401:
                # Handle API key error
                logger.critical("Invalid OpenWeather API key. Authentication failed.")
                return None
            elif e.response.status_code == 404:
                # Handle location not found
                logger.error(f"Invalid coordinates: {latitude}, {longitude}")
                return None
            else:
                # Handle other HTTP errors
                logger.error(f"HTTP error occurred: {e}")
                return None
        
        except RequestException as e:
            # Catch any other request-related exceptions
            logger.error(f"Unexpected request error: {e}")
            return None

#initialize OpenWeather API client
openweather_client = OpenWeatherAPIClient('823fcfa09aff1ce3b730145a32b575d3')

#route to handle weather data retrieval
@app.route('/weather', methods=['GET'])
@limiter.limit("10 per minute")
def get_weather():
    #extract and validate latitude and longitude
    latitude = request.args.get('latitude')
    longitude = request.args.get('longitude')
    
    if not latitude or not longitude:
        logger.warning("Weather request missing latitude or longitude")
        return jsonify({
            "error": "Latitude and longitude are required",
            "status": "400 Bad Request"
        }), 400
    
    try:
        #convert coordinates to float for validation
        float(latitude)
        float(longitude)
    except ValueError:
        #handle invalid coordinate format
        logger.warning(f"Invalid coordinate format: lat={latitude}, lon={longitude}")
        return jsonify({
            "error": "Invalid latitude or longitude format",
            "status": "400 Bad Request"
        }), 400
    
    #fetch weather data
    weather_data = openweather_client.get_weather(latitude, longitude)
    
    #check if weather data was successfully retrieved
    if weather_data:
        try:
            send_weather_to_kafka('weather_data', weather_data)
            return jsonify(weather_data)
        except Exception as e:
            # Log any errors during Kafka message production
            logger.error(f"Error sending weather data to Kafka: {e}")
            return jsonify({
                "error": "Failed to send weather data to Kafka",
                "status": "500 Internal Server Error"
            }), 500
    else:
        #return error if data retrieval failed
        logger.error(f"Failed to retrieve weather data for coordinates: {latitude}, {longitude}")
        return jsonify({
            "error": "Unable to retrieve weather data",
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
    app.run(port=5000)