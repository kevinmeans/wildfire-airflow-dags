from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import configparser


def add_mysql_connection_python(**kwargs):
    """
    This function reads the MySQL connection credentials from a file and adds it to the connections
    in the Airflow database. If the connection already exists, it updates it.

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('config/secret.ini')
    file_path = config['SECRET']['mysql_creds_file']

    with open(file_path) as f:
        api_key = f.readline()

    arr = api_key.split(':')  # Split the credentials into separate parts

    new_conn = Connection(
        conn_id="mysql_connection_creds",
        conn_type='generic',
        host=arr[0],
        port=arr[1],
        login=arr[2],
        password=arr[3]
    )

    session = settings.Session()

    # Check if connection already exists
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

dag = DAG("add_mysql_connection_python", default_args=default_args, schedule_interval="@once")

with dag:
    activateMySQL = PythonOperator(
        task_id='add_mysql_connection_python',
        python_callable=add_mysql_connection_python,
        provide_context=True,
    )

    activateMySQL