import requests
import json
import configparser


class PurpleAirAPI:
    """
    Class for making API calls to Purple Air sensors data.
    """
    config = configparser.ConfigParser()
    config.read('config/purple_air_api.ini')

    def __init__(self, api_key):
        """
        Initializes the API client with the API key.

        :param api_key: API key to access the data
        """

        self.__api_key = api_key
        self.__base_url = self.config['PURPLE_AIR']['url']
        self.__api_version = self.config['PURPLE_AIR']['api_version']

    def get_sensors_data(self, fields=config['PURPLE_AIR']['api_return_fields'], location_type=0, read_keys=None,
                         show_only=None, modified_since=None, max_age=None,
                         nwlng=config['PURPLE_AIR']['nwlng'],
                         nwlat=config['PURPLE_AIR']['nwlat'],
                         selng=config['PURPLE_AIR']['selng'],
                         selat=config['PURPLE_AIR']['selat']):
        """
        Retrieves the sensors data from Purple Air API.

        :param fields: (optional) fields to retrieve in the response (default: PURPLE_AIR_FIELDS)
        :param location_type: (optional) location_type filter (default: 0)
        :param read_keys: (optional) read_keys filter
        :param show_only: (optional) show_only filter
        :param modified_since: (optional) modified_since filter
        :param max_age: (optional) max_age filter
        :param nwlng: (optional) nwlng filter (default: PURPLE_AIR_US_COORDINATES["nwlng"])
        :param nwlat: (optional) nwlat filter (default: PURPLE_AIR_US_COORDINATES["nwlat"])
        :param selng: (optional) selng filter (default: PURPLE_AIR_US_COORDINATES["selng"])
        :param selat: (optional) selat filter (default: PURPLE_AIR_US_COORDINATES["selat"])
        :return: deserialized response from the API call
        :raises ValueError: If the request is not successful
        """
        request_url = self.__base_url + "/" + self.__api_version + "/sensors"
        # fields (REQUIRED)
        request_url += "?fields=" + fields
        # location_type (optional)
        if location_type is not None:
            request_url += "&location_type=" + str(location_type)
        # read_keys (optional)
        if read_keys is not None:
            request_url += "&read_keys=" + read_keys
        # show_only (optional)
        if show_only is not None:
            request_url += "&read_keys=" + show_only
        # modified_since (optional)
        if modified_since is not None:
            request_url += "&modified_since=" + modified_since
        # max_age (optional)
        if max_age is not None:
            request_url += "&max_age=" + max_age
        # nwlat (optional)
        if nwlat is not None:
            request_url += "&nwlat=" + nwlat
        # nwlng (optional)
        if nwlng is not None:
            request_url += "&nwlng=" + nwlng
        # selat (optional)
        if selat is not None:
            request_url += "&selat=" + selat
        # selng (optional)
        if selng is not None:
            request_url += "&selng=" + selng

        print(request_url)

        request = requests.get(request_url, headers={"X-API-Key": str(self.__api_key)})
        if request.status_code != 200:
            raise ValueError(request.text)
        else:
            # deserialization
            return json.loads(request.text)
