import requests

#ask the user for the latitude and longitude
latitude = input("Enter Latitude:")
longitude = input("Enter Longitude:")

#get the url using the latitude and longitude, api key is '823fcfa09aff1ce3b730145a32b575d3'
url = 'http://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid=823fcfa09aff1ce3b730145a32b575d3&units=metric'.format(latitude, longitude)

#send a get request to the url
res = requests.get(url) 
data = res.json()

#extract the temperature, precipitation, wind speed and clouds from the json data
temp = data['main']['temp']
precipitation = data.get('rain', {}).get('1h', 0)
wind_speed = data['wind']['speed']
clouds = data['clouds']['all']

print('Temperature:', temp, '°C')
print('Precipitation:', precipitation, 'mm')
print('Wind Speed:', wind_speed, 'm/s')
print('Clouds:', clouds, '%')