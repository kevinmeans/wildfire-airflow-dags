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
    bq = BigQueryHook(use_legacy_sql=False)
    conn = bq.get_client()
    results = conn.query(query=sql,
                         project=CONFIG['WILD_FIRE']['big_query_project'], location=CONFIG['WILD_FIRE']['big_query_location'])

    data = []
    # Consume iterator and push to list
    for row in results.result():
        data.append(dict(list(row.items())))
        print(row)
    # Convert to JSON so that XCOM can serialize it correctly.  DecimalEndocer class to convert BigQuery Returned values from Decimal type to actual decimals
    from DecimalEncoder import DecimalEncoder
    return json.dumps(data, cls=DecimalEncoder)


def grab_highest_aqi(data):
    data = json.loads(data)
    to_delete_list = list()
    for cur_row in data:
        for toCheck in data:
            if cur_row is toCheck or cur_row in to_delete_list or toCheck in to_delete_list:
                continue
            if not (abs(cur_row.get("latitude") - toCheck.get("latitude")) > 2.00 or abs(
                    cur_row.get("longitude") - toCheck.get("longitude")) > 2.00):
                to_delete_list.append(toCheck)
    for cur_row in to_delete_list:
        data.remove(cur_row)
    for cur_row in data:
        # Print the finished results to Airflow DAG Console
        print(cur_row)
    return data


def grab_wind_direction_per_offender(data):
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

        if (row['wind_speed']) > 2.5:
            import geopy.distance
            # Linear Regression equation  y = -0.02375x + 24.75 ([AQI,MILES AWAY],[1000,1],[200,20])
            miles = -0.02375 * (float(row.get("pm2_5"))) + 24.75
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
    bq = BigQueryHook(use_legacy_sql=False)
    conn = bq.get_client()
    results = conn.query(query=sql, project=CONFIG['WILD_FIRE']['big_query_project'], location=CONFIG['WILD_FIRE']['big_query_location'])
    return results


def gcs_upload_local_file(response):
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


def purple_air_sql_insert(response):
    import pandas as pd
    # Columns cannot have have '.'.  Instead replace with underscore to match Big Query Table
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
        purple_air_sql_insert(raw_data)

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
