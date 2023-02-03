# Wild Fire Locator Airflow Task

These are the task that is ran in Airflow.  Connection Task are used to add connections in Airflow to be used by Authenication Hooks.  
The main workflow of downloading of data can be found in PurpleAirTaskFlowDag.py.  Task are connected via TaskFlow in Airflow.  This allows for contexted to be passed easily between task through the DAG.

Here is a summary of the PurpleAirTaskFlowDag's task:
1. Download data from PurpleAir every 10 minutes (scheduled).  
2. After that it is lightly parsed, converted to an acceptable BigQuery Format (BQ JSON).
3. Push into a GCS folder of an external Bigquery Table
4. Filter results in BigQuery based off of a variety of factors.
5. Grab Sensor's who are past above 200 AQI
6. Filter Sensors to only grab most offending sensor within a radius of each other.
7. Grab weather data from OpenWeatherMap API from filtered list of sensor.
8. Extrapolate where we think the fire is based off of wind direction, AQI, wind speed.
9. Push probable wildfire locations into mysql database for frontend 