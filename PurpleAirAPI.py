import requests
import json

PURPLE_AIR_FIELDS = "last_seen,name,latitude,longitude,pm2.5,pm2.5_a,pm2.5_b,pm10.0,pm10.0_a,pm10.0_b,confidence,channel_flags"
PURPLE_AIR_US_COORDINATES = {
    "nwlat": "68.68",
    "nwlng": "-171.20",
    "selat": "22.55",
    "selng": "-62.48"
}

class PurpleAirAPI:
    def __init__(self, api_key):
        self.__api_key = api_key
        self.__base_url = "https://api.purpleair.com"
        self.__api_version = "v1"

    def get_sensors_data(self, fields=PURPLE_AIR_FIELDS, location_type=0, read_keys=None, show_only=None, modified_since=None,
                         max_age=None, nwlng=PURPLE_AIR_US_COORDINATES["nwlng"], nwlat=PURPLE_AIR_US_COORDINATES["nwlat"], selng=PURPLE_AIR_US_COORDINATES["selng"], selat=PURPLE_AIR_US_COORDINATES["selat"]):

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
