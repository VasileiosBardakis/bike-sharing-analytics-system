import requests
import logging
import time

#configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def fetch_weather_data(latitude, longitude):
    #URL for weather_api weather data (have to give latitude and longitude)
    url = f'http://localhost:5000/weather?latitude={latitude}&longitude={longitude}'
    
    #send GET request with timeout
    response = requests.get(url, timeout=10)
            
    #raise an exception for bad status codes
    response.raise_for_status()
            
    #return JSON data if successful
    return response.json()

def fetch_station_information():
    #URL for station_api station info
    url = 'http://localhost:5001/station/information'
    
    #send GET request with timeout
    response = requests.get(url, timeout=10)
            
    #raise an exception for bad status codes
    response.raise_for_status()
            
    #return JSON data if successful
    return response.json()

def fetch_station_status():
    #URL for station_api station status
    url = 'http://localhost:5001/station/status'
    
    #send GET request with timeout
    response = requests.get(url, timeout=10)
            
    #raise an exception for bad status codes
    response.raise_for_status()
            
    #return JSON data if successful
    return response.json()

def main():
    #ask the user for latitude and longitude input
    while True:
        try:
            latitude = input("Enter Latitude (or 'q' to quit): ")
            
            #alow user to quit
            if latitude.lower() == 'q':
                break
            
            longitude = input("Enter Longitude: ")
            
            #validate coordinate inputs
            float(latitude)
            float(longitude)
        
        except ValueError:
            logger.error("Invalid coordinates. Please enter numeric values.")
            continue
        
        #fetch and display weather data
        weather_data = fetch_weather_data(latitude, longitude)
        if weather_data:
            print("\nWeather Information:")
            print(f"Temperature: {weather_data.get('temperature', 'N/A')}°C")
            print(f"Precipitation: {weather_data.get('precipitation', 'N/A')} mm")
            print(f"Wind Speed: {weather_data.get('wind_speed', 'N/A')} m/s")
            print(f"Clouds: {weather_data.get('clouds', 'N/A')}%")
        else:
            logger.error("Failed to retrieve weather data")
        
        #fetch and display station information
        stations_info = fetch_station_information()
        if stations_info:
            print("\nStation Information (first 5 stations):")
            for station in stations_info[:5]:
                print(f"Station ID: {station.get('station_id', 'N/A')}")
                print(f"Name: {station.get('name', 'N/A')}")
                print(f"Latitude: {station.get('lat', 'N/A')}")
                print(f"Longitude: {station.get('lon', 'N/A')}")
                print(f"Capacity: {station.get('capacity', 'N/A')}\n")
        else:
            logger.error("Failed to retrieve station information")
        
        #fetch and display station status
        stations_status = fetch_station_status()
        if stations_status:
            print("\nStation Status (first 5 stations):")
            for station in stations_status[:5]:
                print(f"Station ID: {station.get('station_id', 'N/A')}")
                print(f"Bikes Available: {station.get('num_bikes_available', 'N/A')}")
                print(f"Docks Available: {station.get('num_docks_available', 'N/A')}\n")

                #run the main function if script is executed directly
if __name__ == '__main__':
    main()