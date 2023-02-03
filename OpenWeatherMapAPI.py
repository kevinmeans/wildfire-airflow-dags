import configparser

import requests
import json
import configparser


class OpenWeatherMapAPI:
    """
    OpenWeatherMapAPI class provides a simple way to interact with OpenWeatherMap API.

    Example:
        api = OpenWeatherMapAPI(api_key)
        data = api.get_weather_data(latitude, longitude, units, mode)
    """
    config = configparser.ConfigParser()
    config.read('config/openweathermap_api.ini')

    def __init__(self, api_key: str):
        """
        Initializes the OpenWeatherMapAPI class with the given api_key.

        Args:
            api_key: API key for OpenWeatherMap API.
        """
        self.__api_key = api_key
        self.__base_url = self.config['OPEN_WEATHER_MAP']['url']

    def get_weather_data(self, latitude: float = 37.648544, longitude: float = -118.972076, units: str = None,
                         mode: str = None):
        """
        Gets the weather data for the given location.  Defaults to the always windy Mammoth Lakes, CA :)

        Args:
            latitude: Latitude of the location. Default is 37.648544
            longitude: Longitude of the location. Default is -118.972076
            units: Units in which to return the data. Options are 'metric' or 'imperial'.
            mode: Data format, options are 'json', 'xml', or 'html'.

        Returns:
            The weather data for the given location in JSON format.
        """
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

        request = requests.get(request_url)
        if request.status_code != 200:
            raise ValueError(f"Request to API failed with status code: {request.status_code}. Response: {request.text}")
        else:
            # deserialization
            return json.loads(request.text)
