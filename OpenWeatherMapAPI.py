import requests
import json


class OpenWeatherMapAPI:
    def __init__(self, api_key):
        self.__api_key = api_key
        self.__base_url = "https://api.openweathermap.org/data/2.5"

    def get_weather_data(self, latitude=37.648544, longitude=-118.972076, units=None, mode=None):
        # Ref: https://openweathermap.org/current
        # Example URL Construction: https://api.openweathermap.org/data/2.5/weather?lat=36.0&lon=-122.08&appid=XXXXX
        request_url = self.__base_url + "/" + "weather"
        # fields (REQUIRED)
        request_url += "?lat=" + str(latitude) + "&lon=" + str(longitude) + "&appid=" + str(self.__api_key)
        # units (optional)
        if units is not None:
            request_url += "&units=" + str(units)
        # mode (optional)
        if mode is not None:
            request_url += "&mode=" + str(mode)

        print(request_url)

        request = requests.get(request_url)
        if request.status_code != 200:
            raise ValueError(request.text)
        else:
            # deserialization
            return json.loads(request.text)
