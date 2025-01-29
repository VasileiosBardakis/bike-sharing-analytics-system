import requests
import logging
import time
import json
from typing import Dict, List, Union, Optional

#logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass

def validate_coordinates(latitude: str, longitude: str) -> None:
    """Validate latitude and longitude inputs
    Args:latitude (str): Latitude value
         longitude (str): Longitude value
    Raises:ValueError: If coordinates are invalid
           DataValidationError: If coordinates are out of acceptable range
    """
    try:
        lat = float(latitude)
        lon = float(longitude)
    except ValueError:
        raise ValueError("Coordinates must be numeric values")
    
    #validate coordinate ranges
    if not (-90 <= lat <= 90):
        raise DataValidationError(f"Latitude must be between -90 and 90. Received: {lat}")
    
    if not (-180 <= lon <= 180):
        raise DataValidationError(f"Longitude must be between -180 and 180. Received: {lon}")

def validate_weather_data(weather_data: Dict[str, Union[float, int]]) -> Dict[str, Union[float, int]]:
    """
    Validate weather data against expected schema and ranges
    Args:weather_data (Dict): Raw weather data from API
    Returns:Dict: Validated weather data
    Raises:DataValidationError: If data fails validation
    """
    #define expected keys and their validation rules
    validation_rules = {
        'temperature': (-100, 100),  #extreme temperature range
        'precipitation': (0, 1000),  #max reasonable precipitation in mm
        'wind_speed': (0, 200),      #max reasonable wind speed
        'clouds': (0, 100),          #cloud coverage percentage
        'timestamp': (time.time() - 3600, time.time() + 3600)  #within 1 hour of current time
    }
    
    validated_data = {}
    for key, (min_val, max_val) in validation_rules.items(): #this is the key defined above
        if key not in weather_data:
            raise DataValidationError(f"Missing required key: {key}")
        
        value = weather_data[key]
        
        #type checking
        if key == 'timestamp':
            if not isinstance(value, (int, float)):
                raise DataValidationError(f"Invalid {key} type. Expected number.")
        else:
            if not isinstance(value, (int, float)):
                raise DataValidationError(f"Invalid {key} type. Expected number.")
        
        #range validation
        if value < min_val or value > max_val:
            raise DataValidationError(f"{key} out of acceptable range. Value: {value}")
        
        validated_data[key] = value
    
    return validated_data

def validate_station_information(stations_info: List[Dict[str, Union[str, int, float]]]) -> List[Dict[str, Union[str, int, float]]]:
    """
    Validate station information data
    Args: stations_info (List[Dict]): Raw station information data
    Returns: List[Dict]: Validated station information
    Raises: DataValidationError: If data fails validation
    """
    validated_stations = []
    
    for station in stations_info:
        #check for required keys
        required_keys = ['station_id', 'name', 'lat', 'lon', 'capacity']
        for key in required_keys:
            if key not in station:
                raise DataValidationError(f"Missing required station key: {key}")
        
        #validate station ID (assuming it's a non-empty string)
        if not station['station_id'] or not isinstance(station['station_id'], (str, int)):
            raise DataValidationError(f"Invalid station ID: {station['station_id']}")
        
        #validate name (non-empty string)
        if not station['name'] or not isinstance(station['name'], str):
            raise DataValidationError(f"Invalid station name: {station['name']}")
        
        #validate geographic coordinates
        try:
            lat = float(station['lat'])
            lon = float(station['lon'])
            
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                raise ValueError("Coordinates out of range")
        except (ValueError, TypeError):
            raise DataValidationError(f"Invalid coordinates: lat={station['lat']}, lon={station['lon']}")
        
        #validate capacity (positive integer)
        if not isinstance(station['capacity'], int) or station['capacity'] < 0:
            raise DataValidationError(f"Invalid capacity: {station['capacity']}")
        
        validated_stations.append(station)
    
    return validated_stations

def validate_station_status(stations_status: List[Dict[str, Union[str, int]]]) -> List[Dict[str, Union[str, int]]]:
    """
    Validate station status data
    Args: stations_status (List[Dict]): Raw station status data
    Returns: List[Dict]: Validated station status
    Raises: DataValidationError: If data fails validation
    """
    validated_statuses = []
    
    for station in stations_status:
        #check for required keys
        required_keys = ['station_id', 'num_bikes_available', 'num_docks_available']
        for key in required_keys:
            if key not in station:
                raise DataValidationError(f"Missing required station status key: {key}")
        
        #validate station ID
        if not station['station_id'] or not isinstance(station['station_id'], (str, int)):
            raise DataValidationError(f"Invalid station ID: {station['station_id']}")
        
        #validate bikes and docks available (non-negative integers)
        for key in ['num_bikes_available', 'num_docks_available']:
            if not isinstance(station[key], int) or station[key] < 0:
                raise DataValidationError(f"Invalid {key}: {station[key]}")
        
        validated_statuses.append(station)
    
    return validated_statuses

def fetch_weather_data(latitude: str, longitude: str) -> Optional[Dict[str, Union[float, int]]]:
    """
    Fetch and validate weather data with comprehensive error handling
    Args: latitude (str): Latitude coordinate
          longitude (str): Longitude coordinate
    Returns: [Dict]: Validated weather data or None
    """
    try:
        validate_coordinates(latitude, longitude)
        
        #URL for weather_api weather data
        url = f'http://localhost:5000/weather?latitude={latitude}&longitude={longitude}'
        
        #send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        #parse and validate weather data
        weather_data = response.json()
        return validate_weather_data(weather_data)
    
    except (ValueError, DataValidationError) as e:
        logger.error(f"Weather data validation error: {e}")
        return None
    except requests.RequestException as e:
        logger.error(f"Weather data retrieval error: {e}")
        return None

def fetch_station_information() -> Optional[List[Dict]]:
    """
    Fetch and validate station information with comprehensive error handling
    """
    try:
        #URL for station_api station info
        url = 'http://localhost:5001/station/information'
        
        #send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        #parse and validate station information
        stations_info = response.json()
        return validate_station_information(stations_info)
    
    except DataValidationError as e:
        logger.error(f"Station information validation error: {e}")
        return None
    except requests.RequestException as e:
        logger.error(f"Station information retrieval error: {e}")
        return None

def fetch_station_status() -> Optional[List[Dict]]:
    """
    Fetch and validate station status with comprehensive error handling
    """
    try:
        #URL for station_api station status
        url = 'http://localhost:5001/station/status'
        
        #send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        #parse and validate station status
        stations_status = response.json()
        return validate_station_status(stations_status)
    
    except DataValidationError as e:
        logger.error(f"Station status validation error: {e}")
        return None
    except requests.RequestException as e:
        logger.error(f"Station status retrieval error: {e}")
        return None

def main():
    """
    Main function to demonstrate data pipeline with validation
    """
    while True:
        try:
            latitude = input("Enter Latitude (or 'q' to quit): ")
            
            #allow user to quit
            if latitude.lower() == 'q':
                break
            
            longitude = input("Enter Longitude: ")
            
            #fetch and validate weather data
            start_time = time.time()  #measure time it takes for the api to fetch the data
            weather_data = fetch_weather_data(latitude, longitude)
            weather_time = time.time() - start_time
            if weather_data:
                print("\nWeather Information:")
                print(f"Temperature: {weather_data['temperature']}°C")
                print(f"Precipitation: {weather_data['precipitation']} mm")
                print(f"Wind Speed: {weather_data['wind_speed']} m/s")
                print(f"Clouds: {weather_data['clouds']}%")
            
            #fetch and validate station information
            start_time = time.time()   #measure time it takes for the api to fetch the data
            stations_info = fetch_station_information()
            station_info_time = time.time() - start_time
            if stations_info:
                print("\nStation Information (first 5 stations):")
                for station in stations_info[:5]:
                    print(f"Station ID: {station['station_id']}")
                    print(f"Name: {station['name']}")
                    print(f"Latitude: {station['lat']}")
                    print(f"Longitude: {station['lon']}")
                    print(f"Capacity: {station['capacity']}\n")
            
            #fetch and validate station status
            start_time = time.time()
            stations_status = fetch_station_status()    #measure time it takes for the api to fetch the data
            station_status_time = time.time() - start_time
            if stations_status:
                print("\nStation Status (first 5 stations):")
                for station in stations_status[:5]:
                    print(f"Station ID: {station['station_id']}")
                    print(f"Bikes Available: {station['num_bikes_available']}")
                    print(f"Docks Available: {station['num_docks_available']}\n")
        
            print(f"\nResponse times:")
            print(f"Weather API: {weather_time:.2f} seconds")
            print(f"Station Info API: {station_info_time:.2f} seconds")
            print(f"Station Status API: {station_status_time:.2f} seconds")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    main()