import requests
import logging
import time
import json
import math
from typing import Dict, List, Union, Optional
import pandas as pd
import os

#logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#directory to store Parquet files
PARQUET_DIR = "data_parquet"
os.makedirs(PARQUET_DIR, exist_ok=True)  #Create if not exist

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
        # Check for required keys
        required_keys = ['station_id', 'num_bikes_available', 'num_docks_available', 'last_reported']
        for key in required_keys:
            if key not in station:
                raise DataValidationError(f"Missing required station status key: {key}")
        
        # Validate station ID
        if not station['station_id'] or not isinstance(station['station_id'], (str, int)):
            raise DataValidationError(f"Invalid station ID: {station['station_id']}")
        
        # Validate bikes and docks available (non-negative integers)
        for key in ['num_bikes_available', 'num_docks_available']:
            if not isinstance(station[key], int) or station[key] < 0:
                raise DataValidationError(f"Invalid {key}: {station[key]}")
        
        validated_statuses.append(station)
    
    return validated_statuses

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the distance between two points using the Haversine formula
    Args:
        lat1, lon1: Coordinates of first point
        lat2, lon2: Coordinates of second point
    Returns:
        Distance in kilometers
    """
    # Convert decimal degrees to radians
    lat1, lon1 = math.radians(lat1), math.radians(lon1)
    lat2, lon2 = math.radians(lat2), math.radians(lon2)
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    
    return c * r

def get_nearest_station(latitude: float, longitude: float, stations_info: List[Dict]) -> Dict:
    """
    Find the nearest station to given coordinates
    Args:
        latitude, longitude: Reference coordinates
        stations_info: List of station information dictionaries
    Returns:
        Dictionary containing nearest station information
    """
    nearest_station = None
    min_distance = float('inf')
    
    for station in stations_info:
        distance = calculate_distance(
            float(latitude), 
            float(longitude),
            float(station['lat']), 
            float(station['lon'])
        )
        
        if distance < min_distance:
            min_distance = distance
            nearest_station = station
    
    return nearest_station

# Modify the fetch_weather_data function to update coordinates with nearest station
def fetch_weather_data(latitude: str, longitude: str) -> Optional[Dict[str, Union[float, int]]]:
    """
    Fetch and validate weather data with comprehensive error handling
    Args: latitude (str): Latitude coordinate
          longitude (str): Longitude coordinate
    Returns: [Dict]: Validated weather data or None
    """
    try:
        # Validate coordinates first
        validate_coordinates(latitude, longitude)
        
        # Fetch station information first to find nearest station
        stations_info = fetch_station_information()
        if not stations_info:
            raise DataValidationError("Could not fetch station information")
        
        # Find nearest station
        nearest_station = get_nearest_station(float(latitude), float(longitude), stations_info)
        if not nearest_station:
            raise DataValidationError("Could not find nearest station")
        
        # URL for weather_api weather data
        url = f'http://localhost:5000/weather?latitude={latitude}&longitude={longitude}'
        
        # Send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse and validate weather data
        weather_data = response.json()
        validated_data = validate_weather_data(weather_data)
        
        # Use the nearest station's coordinates instead of the input coordinates
        validated_data['lat'] = float(nearest_station['lat'])
        validated_data['lon'] = float(nearest_station['lon'])
        
        return validated_data
    
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
        # URL for station_api station info
        url = 'http://localhost:5001/station/information'
        
        # Send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse and validate station information
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
        # URL for station_api station status
        url = 'http://localhost:5001/station/status'
        
        # Send GET request with timeout
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse and validate station status
        stations_status = response.json()
        return validate_station_status(stations_status)
    
    except DataValidationError as e:
        logger.error(f"Station status validation error: {e}")
        return None
    except requests.RequestException as e:
        logger.error(f"Station status retrieval error: {e}")
        return None

def save_to_parquet(data: Union[Dict, List[Dict]], file_name: str) -> None:
    """
    Save data to a Parquet file.
    If the file already exists, append the new data.
    Skips stations that already exist in the file.
    Removes duplicates based on latitude and longitude for weather data.
    """
    try:
        # Convert data to a DataFrame
        new_data_df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        
        # Define the Parquet file path
        parquet_path = os.path.join(PARQUET_DIR, f"{file_name}.parquet")
        
        # Check if the Parquet file already exists
        if os.path.exists(parquet_path):
            # Load existing data
            existing_data_df = pd.read_parquet(parquet_path)
            
            # If this is station data (has station_id column), filter out existing stations
            if 'station_id' in new_data_df.columns:
                existing_station_ids = set(existing_data_df['station_id'])
                new_data_df = new_data_df[~new_data_df['station_id'].isin(existing_station_ids)]
                
                # If all stations already exist, return early
                if len(new_data_df) == 0:
                    logger.info("All stations already exist in the file. No new data to add.")
                    return
            
            # Ensure both DataFrames have the same columns
            missing_cols = set(existing_data_df.columns) - set(new_data_df.columns)
            for col in missing_cols:
                new_data_df[col] = None
            
            missing_cols = set(new_data_df.columns) - set(existing_data_df.columns)
            for col in missing_cols:
                existing_data_df[col] = None
            
            # Ensure column order matches
            new_data_df = new_data_df[existing_data_df.columns]
            
            # Append new data to existing data
            combined_df = pd.concat([existing_data_df, new_data_df], ignore_index=True)
        else:
            # If the file doesn't exist, use the new data as is
            combined_df = new_data_df
        
        # Drop duplicates based on lat and lon for weather data
        if all(col in combined_df.columns for col in ['lat', 'lon']):
            combined_df = combined_df.drop_duplicates(
                subset=['lat', 'lon'],
                keep='last'
            )
        
        # Save the deduplicated DataFrame to Parquet
        combined_df.to_parquet(parquet_path, engine='pyarrow', index=False)
        
    except Exception as e:
        logger.error(f"Error saving data to Parquet file: {e}")

def main():
    """
    Main function to demonstrate data pipeline with validation
    """
    while True:
        try:
            latitude = input("Enter Latitude (or 'q' to quit): ")
            
            # Allow user to quit
            if latitude.lower() == 'q':
                break
            
            longitude = input("Enter Longitude: ")
            
            # Fetch and validate weather data
            weather_data = fetch_weather_data(latitude, longitude)
            if weather_data:
                print("\nWeather Information:")
                print(f"Temperature: {weather_data['temperature']}°C")
                print(f"Precipitation: {weather_data['precipitation']} mm")
                print(f"Wind Speed: {weather_data['wind_speed']} m/s")
                print(f"Clouds: {weather_data['clouds']}%")
                save_to_parquet(weather_data, "weather_data")
            
            # Fetch and validate station information
            stations_info = fetch_station_information()
            if stations_info:
                print("\nStation Information (first 5 stations):")
                for station in stations_info:
                    print(f"Station ID: {station['station_id']}")
                    print(f"Name: {station['name']}")
                    print(f"Latitude: {station['lat']}")
                    print(f"Longitude: {station['lon']}")
                    print(f"Capacity: {station['capacity']}\n")

                save_to_parquet(stations_info, "stations_info")
            
            # Fetch and validate station status
            stations_status = fetch_station_status()
            if stations_status:
                print("\nStation Status (first 5 stations):")
                for station in stations_status:
                    print(f"Station ID: {station['station_id']}")
                    print(f"Bikes Available: {station['num_bikes_available']}")
                    print(f"Docks Available: {station['num_docks_available']}\n")
                    print(f"Last reported: {station['last_reported']}")

                save_to_parquet(stations_status, "stations_status") 
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

if __name__ == '__main__':
    main()

    #hi