from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import json
import configparser


def add_gcp_connection(**kwargs):
    """
    This function adds a Google Cloud Platform connection to Airflow, if it does not already exist.

    The connection information is read from the secret.ini file, which should be located in a config folder.
    The file path to the Google Cloud Platform secret file is specified in the secret.ini file.

    The connection id is set as "google_cloud_default" and the type as "google_cloud_platform". The extra fields specify
    the scope as "https://www.googleapis.com/auth/cloud-platform" and the key path to the Google Cloud Platform secret file.

    If a connection with the same connection id already exists in Airflow, the existing connection is updated with the
    new extra field values.
    """
    config = configparser.ConfigParser()
    config.read('config/secret.ini')
    file_path = config['SECRET']['gcp_secret_file_path']
    new_conn = Connection(
        conn_id="google_cloud_default",
        conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__scope": "https://www.googleapis.com/auth/cloud-platform",
        "extra__google_cloud_platform__key_path": file_path
    }

    session = settings.Session()

    # checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else:  # if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 5, 9),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("AddGCPConnection", default_args=default_args, schedule_interval="@once")

with dag:
    activateGCP = PythonOperator(
        task_id='add_gcp_connection_python',
        python_callable=add_gcp_connection,
        provide_context=True,
    )

    activateGCP
