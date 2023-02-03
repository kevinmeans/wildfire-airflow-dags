import json
from datetime import datetime, timezone

import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import configparser

CONFIG = configparser.ConfigParser()
CONFIG.read('config/purple_airflow_dag.ini')


def gbq_query(sql):
    """
    Execute the SQL query in BigQuery and return the results in the form of a list of dictionaries.

    :param sql: SQL query string to be executed
    :type sql: str
    :return: Results of the query in a list of dictionaries
    :rtype: str
    """
    bq = BigQueryHook(use_legacy_sql=False)
    conn = bq.get_client()
    results = conn.query(query=sql,
                         project=CONFIG['WILD_FIRE']['big_query_project'],
                         location=CONFIG['WILD_FIRE']['big_query_location'])

    data = []
    # Consume iterator and push to list
    for row in results.result():
        data.append(dict(list(row.items())))
        print(row)
    # Convert to JSON so that XCOM can serialize it correctly.
    # DecimalEndocer class to convert BigQuery Returned values from Decimal type to actual decimals
    from DecimalEncoder import DecimalEncoder
    return json.dumps(data, cls=DecimalEncoder)


def grab_highest_aqi(data):
    """
    Filter the list of AQI Offenders and retain only the ones with highest AQI values within a certain proximity.

    :param data: List of dictionaries containing AQI values
    :type data: str
    :return: Filtered list of dictionaries with highest AQI values
    :rtype: list
    """
    data = json.loads(data)
    to_delete_list = list()
    for cur_row in data:
        for to_check in data:
            if cur_row is to_check or cur_row in to_delete_list or to_check in to_delete_list:
                continue
            if not (abs(cur_row.get("latitude") - to_check.get("latitude")) > 2.00 or abs(
                    cur_row.get("longitude") - to_check.get("longitude")) > 2.00):
                to_delete_list.append(to_check)
    for cur_row in to_delete_list:
        data.remove(cur_row)
    for cur_row in data:
        # Print the finished results to Airflow DAG Console
        print(cur_row)
    return data


def grab_wind_direction_per_offender(data):
    """
    Add wind direction and speed information to each dictionary in the list.

    :param data: List of dictionaries with AQI values
    :type data: list
    :return: None
    :rtype: None
    """
    from airflow.hooks.base_hook import BaseHook
    conn = BaseHook.get_connection(CONFIG['WILD_FIRE']['open_weather_map_airflow_connection_key'])
    from OpenWeatherMapAPI import OpenWeatherMapAPI
    open_weather_request = OpenWeatherMapAPI(conn.get_password().strip())

    import pandas as pd
    df_radii = pd.DataFrame()

    import time
    epoch_time = int(time.time())

    for row in data:
        request = open_weather_request.get_weather_data(latitude=row.get("latitude"), longitude=row.get("longitude"))
        row['wind_direction_degree'] = request['wind']['deg']
        row['wind_speed'] = request['wind']['speed']

        # Only insert a data point if there is more than 2.5 knots of wind.
        if (row['wind_speed']) > 2.5:
            import geopy.distance
            # Linear Regression equation  y = -0.02375x + 24.75 ([AQI,MILES AWAY],[1000,1],[200,20])
            miles = -0.02375 * (float(row.get("pm2_5"))) + 24.75

            # Calculate geo point towards the wind X miles away from the offending sensor
            pt = geopy.distance.distance(miles=miles).destination((row.get("latitude"), row.get("longitude")),
                                                                  bearing=(row['wind_direction_degree']))
            row = {'offend_time': epoch_time, 'latitude': pt.latitude, 'longitude': pt.longitude}

            df_radii = df_radii.append(row, ignore_index=True)

    from sqlalchemy import create_engine
    from airflow.hooks.base_hook import BaseHook
    conn = BaseHook.get_connection(CONFIG['WILD_FIRE']['open_weather_map_airflow_connection_key'])
    db = "mysql+pymysql://{user}:{password}@{ip}:{port}/{database}".format(user=conn.user,
                                                                           password=conn.get_password(),
                                                                           ip=conn.host,
                                                                           port=conn.port,
                                                                           database='purple_air')
    db_connection = create_engine(db).connect()

    try:
        df_radii.to_sql("smoke_radius", db_connection, if_exists='append', index=False);
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    finally:
        db_connection.close()


def bigquery_insert(sql):
    """
    Inserts data into BigQuery.

    Parameters:
        sql (str): The SQL query to execute.

    Returns:
        object: The result of the query execution.
    """
    bq = BigQueryHook(use_legacy_sql=False)
    conn = bq.get_client()
    results = conn.query(query=sql, project=CONFIG['WILD_FIRE']['big_query_project'], location=CONFIG['WILD_FIRE']['big_query_location'])
    return results


def gcs_upload_local_file(response):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
        response (dict): The response data to be uploaded to GCS.

    Returns:
        None
    """
    import pandas as pd
    # Columns cannot have have '.'.  Instead replace with underscore to match Big Query Table
    df = pd.DataFrame(response["data"], columns=[column.replace('.', '_') for column in response["fields"]])

    import time
    epoch_time = int(time.time())
    df['batch_timestamp'] = epoch_time

    import random
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import string

    GCSHook().upload(
        bucket_name=CONFIG['WILD_FIRE']['gcs_purple_air_bucket'],
        object_name=''.join(random.choice(string.ascii_letters) for i in range(16)) + ".json",
        data=df.to_json(orient='records', lines=True)
    )


def purple_air_mysql_insert(response):
    """
    This function inserts the finialized filtered data into a mysql database.
    The database connection details and table name is specified in the code.
    Specifically, it inserts smoke data points.

    Parameters:
    response (dict): A dictionary containing the data to be inserted into the database

    Returns:
    None
    """
    import pandas as pd
    df = pd.DataFrame(response["data"], columns=[column.replace('.', '_') for column in response["fields"]])
    import time
    epoch_time = int(time.time())
    df['batch_timestamp'] = epoch_time

    from sqlalchemy import create_engine
    from airflow.hooks.base_hook import BaseHook
    conn = BaseHook.get_connection(CONFIG['WILD_FIRE']['open_weather_map_airflow_connection_key'])
    db = "mysql+pymysql://{user}:{password}@{ip}:{port}/{database}".format(user=conn.user,
                                                                           password=conn.get_password(),
                                                                           ip=conn.host,
                                                                           port=conn.port,
                                                                           database='purple_air')
    db_connection = create_engine(db).connect()

    try:
        frame = df.to_sql("purple_air_data", db_connection, if_exists='replace');
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    finally:
        db_connection.close()


def construct_bigquery_sql(data, gcp_project, gcp_dataset, gcp_table):
    """
    This function constructs a sql statement for inserting data into a BigQuery table.

    Parameters:
    data (list): A list of data to be inserted into the table.
    gcp_project (str): The name of the GCP project where the BigQuery table resides.
    gcp_dataset (str): The name of the BigQuery dataset where the table resides.
    gcp_table (str): The name of the BigQuery table.

    Returns:
    str: The constructed SQL statement.
    """
    # Ternary Operation, Add quotes if its a str, change it to NULL if its none
    query = "INSERT INTO " + gcp_project + "." + gcp_dataset + "." + gcp_table + " values "
    for i, val in enumerate(data):
        query += "(" + ",".join(
            ["'" + j.replace("'", "") + "'" if type(j) is str else "NULL" if j == None else str(j) for j in val])
        if i == len(data) - 1:
            query += ")\n"
        else:
            query += "),\n"
    return query


def get_purple_air_data():
    """
    This function retrieves data from the PurpleAir API.
    The API connection details are specified in the code.

    Returns:
    dict: A dictionary of data retrieved from the PurpleAir API.
    """
    from airflow.hooks.base_hook import BaseHook
    conn = BaseHook.get_connection(CONFIG['WILD_FIRE']['purple_air_airflow_connection_key'])
    import PurpleAirAPI as purpleAir
    purple_air_request = purpleAir.PurpleAirAPI(conn.get_password().strip())
    raw_data = purple_air_request.get_sensors_data()
    return raw_data


@dag(
    schedule_interval='*/10 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['PurpleAirTest'],
)
def purpleair_taskflow_api_etl(**kwargs):
    @task()
    def extract_purple_air():
        return get_purple_air_data()

    @task()
    def insert_gcs(raw_data):
        gcs_upload_local_file(raw_data)

    @task()
    def insert_mysql_data(raw_data):
        purple_air_mysql_insert(raw_data)

    @task()
    def extract_gbq():
        return gbq_query(CONFIG['WILD_FIRE']['gbq_sql_extraction_query'])

    @task()
    def reduce(data):
        return grab_highest_aqi(data)

    @task()
    def wind_direction(data):
        grab_wind_direction_per_offender(data)

    purple_air_data = extract_purple_air()
    insert = insert_gcs(purple_air_data)
    insert_mysql_data(purple_air_data)

    extract = extract_gbq()
    reduce = reduce(extract)

    insert >> extract >> reduce >> wind_direction(reduce)


etl_dag = purpleair_taskflow_api_etl()
