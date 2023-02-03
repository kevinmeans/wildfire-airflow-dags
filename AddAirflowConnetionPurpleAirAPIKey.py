from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import configparser


def add_purpleair_connection(**kwargs):
    """
    Function to add purple air connection to the Airflow UI

    :param kwargs: Airflow context
    :return: None
    """
    # Load the API key from a secret.ini file
    config = configparser.ConfigParser()
    config.read('config/secret.ini')
    file_path = config['SECRET']['purple_air_api_key_file_path']
    with open(file_path) as f:
        api_key = f.readline()

    # Create the connection object with the API key
    new_conn = Connection(
        conn_id="purple_air_api_key",
        conn_type='generic',
        password=api_key
    )

    session = settings.Session()

    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        session.add(my_connection)
        session.commit()
    else:
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

# Create the DAG
dag = DAG("activate_purpleair_connection", default_args=default_args, schedule_interval="@once")

with dag:
    activatePurpleAir = PythonOperator(
        task_id='add_purpleair_connection_python',
        python_callable=add_purpleair_connection,
        provide_context=True,
        dag_description="Add purple air connection to the Airflow UI"
    )

    activatePurpleAir
