import flask
from flask import request, jsonify
import requests
import time
import logging
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

#configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#initialize Flask application
app = flask.Flask(__name__)

#openWeather API configuration
class OpenWeatherAPIClient:
    def __init__(self, api_key, base_url='http://api.openweathermap.org/data/2.5/weather'):
        self.api_key = api_key
        self.base_url = base_url
        #rate limiting parameters
        self.max_retries = 3
        self.retry_delay = 5  #seconds between retries
        self.rate_limit_delay = 60  #seconds to wait after rate limit error

    def get_weather(self, latitude, longitude):
        #construct API request URL
        url = f'{self.base_url}?lat={latitude}&lon={longitude}&appid={self.api_key}&units=metric'
        
        for attempt in range(self.max_retries):
            try:
                #send GET request with timeout
                response = requests.get(url, timeout=10)
                
                #check for specific HTTP errors
                response.raise_for_status()
                
                #parse and extract weather data
                data = response.json()
                return {
                    'temperature': data['main']['temp'],
                    'precipitation': data.get('rain', {}).get('1h', 0),
                    'wind_speed': data['wind']['speed'],
                    'clouds': data['clouds']['all'],
                    'timestamp': time.time()
                }
            
            except Timeout:
                #handle request timeout
                logger.warning(f"Timeout on attempt {attempt + 1}. Retrying...")
                time.sleep(self.retry_delay)
            
            except ConnectionError:
                #handle network connection issues
                logger.error("Network connection error. Check internet connectivity.")
                return None
            
            except HTTPError as e:
                if e.response.status_code == 429:
                    #handle rate limiting (429 Too Many Requests)
                    logger.warning("Rate limit exceeded. Waiting before retry.")
                    time.sleep(self.rate_limit_delay)
                elif e.response.status_code == 401:
                    #handle api key error
                    logger.error("Invalid API key.")
                    return None
                elif e.response.status_code == 404:
                    #handle location not found
                    logger.error("Invalid coordinates.")
                    return None
                else:
                    #handle other HTTP errors
                    logger.error(f"HTTP error occurred: {e}")
                    return None
            
            except RequestException as e:
                #catch any other request-related exceptions
                logger.error(f"Request error: {e}")
                return None
        
        #if all retries fail
        logger.error("Max retries exceeded. Unable to fetch weather data.")
        return None

#initialize OpenWeather API client
openweather_client = OpenWeatherAPIClient('823fcfa09aff1ce3b730145a32b575d3')

#route to handle weather data retrieval
@app.route('/weather', methods=['GET'])
def get_weather():
    #extract and validate latitude and longitude
    latitude = request.args.get('latitude')
    longitude = request.args.get('longitude')
    

    if not latitude or not longitude:
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
        return jsonify({
            "error": "Invalid latitude or longitude format",
            "status": "400 Bad Request"
        }), 400
    
    #fetch weather data
    weather_data = openweather_client.get_weather(latitude, longitude)
    
    #check if weather data was successfully retrieved
    if weather_data:
        return jsonify(weather_data)
    else:
        #return error if data retrieval failed
        return jsonify({
            "error": "Unable to retrieve weather data",
            "status": "500 Internal Server Error"
        }), 500

#run the Flask application if script is executed directly
if __name__ == '__main__':
    app.run(port=5000)