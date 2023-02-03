from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import configparser


def add_openweathermap_connection(**kwargs):
    config = configparser.ConfigParser()
    config.read('config/secret.ini')
    file_path = config['SECRET']['purple_air_api_key_file_path']

    with open('file_path') as f:
        api_key = f.readline()

    new_conn = Connection(
        conn_id="open_weather_map_api_key",
        conn_type='generic',
        password=api_key
    )

    session = settings.Session()

    # checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        session.add(my_connection)
        session.commit()
    else:  # if it doesn't exit create one
        session.add(new_conn)
        session.commit()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 9),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("activate_openweathermap_connection", default_args=default_args, schedule_interval="@once")

with dag:
    activateOpenWeatherMap = PythonOperator(
        task_id='add_openweathermap_connection_python',
        python_callable=add_openweathermap_connection,
        provide_context=True,
    )

    activateOpenWeatherMap
