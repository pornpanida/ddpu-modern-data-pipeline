import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from datetime import timedelta

import requests
import pendulum

DAG_FOLDER = "/opt/airflow/dags"

def _get_weather_data():
    API_KEY = "39bd8d44-a9d2-4762-a263-b168c0d88b94"
    url = "https://api.airvisual.com/v2/city"
    params = {
        "city": "Bangkok",
        "state": "Bangkok",
        "country": "Thailand",
        "key": API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    with open(f"{DAG_FOLDER}/data.json", "w") as f:
        json.dump(data, f)

def _validate_data():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    try:
        weather_data = data["data"]["current"]["weather"]
        temp = weather_data["tp"]
    except KeyError as e:
        raise ValueError(f"Missing key in response data: {e}")

    print("Weather Data:", json.dumps(weather_data, indent=2))

def _create_weather_air_quality_table():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS weather_air_quality (
            ts TIMESTAMP NOT NULL,
            temp FLOAT,
            pressure FLOAT,
            humidity INT,
            wind_speed FLOAT,
            wind_dir INT,
            icon VARCHAR(10),
            aqi_us INT,
            main_pollutant_us VARCHAR(10),
            aqi_cn INT,
            main_pollutant_cn VARCHAR(10)
        );
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres():
    pg_hook = PostgresHook(
        postgres_conn_id="weather_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    weather = data["data"]["current"]["weather"]
    pollution = data["data"]["current"]["pollution"]

    ts = weather.get("ts")
    temp = weather.get("tp")
    pressure = weather.get("pr")
    humidity = weather.get("hu")
    wind_speed = weather.get("ws")
    wind_dir = weather.get("wd")
    icon = weather.get("ic")

    aqi_us = pollution.get("aqius")
    main_pollutant_us = pollution.get("mainus")
    aqi_cn = pollution.get("aqicn")
    main_pollutant_cn = pollution.get("maincn")

    # Data quality check
    required_fields = [ts, temp, pressure, humidity, wind_speed, wind_dir, aqi_us, aqi_cn]
    if any(x is None for x in required_fields):
        raise ValueError("Missing critical field(s) in API response")

    # Type check (optional)
    assert isinstance(temp, (int, float)), "temp must be numeric"
    assert isinstance(aqi_us, int), "aqius must be integer"

    sql = """
        INSERT INTO weather_air_quality (
            ts, temp, pressure, humidity, wind_speed, wind_dir, icon,
            aqi_us, main_pollutant_us, aqi_cn, main_pollutant_cn
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (
        ts, temp, pressure, humidity, wind_speed, wind_dir, icon,
        aqi_us, main_pollutant_us, aqi_cn, main_pollutant_cn
    ))
    connection.commit()

def _check_temp_alert():
    with open(f"{DAG_FOLDER}/data.json", "r") as f:
        data = json.load(f)

    temp = data["data"]["current"]["weather"]["tp"]
    print(f"Current temperature = {temp}°C")
    return "send_alert_email" if temp > 35 else "no_alert"

default_args = {
    "email": ["Somyong@odds.team"],
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "start_date": timezone.datetime(2025, 1, 1)
}

with DAG(
    "weather_api_dag_capstone",
    default_args=default_args,
    schedule="0 */1 * * *",
    # start_date=timezone.datetime(2025, 1, 1),
    #data_interval=timedelta(days=90),
    #start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dpu"],
):
    start = EmptyOperator(task_id="start")

    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    create_weather_air_quality_table = PythonOperator(
        task_id="create_weather_air_quality_table",
        python_callable=_create_weather_air_quality_table,
    )

    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    check_temp_alert = BranchPythonOperator(
        task_id="check_temp_alert",
        python_callable=_check_temp_alert,
    )

    send_alert_email = EmailOperator(
        task_id="send_alert_email",
        to=["Somyong@odds.team"],
        subject="🔥 Weather Alert: Temperature Too High!",
        html_content="The temperature in Bangkok exceeds 35°C. Please take precautions!",
    )

    no_alert = EmptyOperator(task_id="no_alert")

    send_email = EmailOperator(
        task_id="send_email",
        to=["Somyong@odds.team"],
        subject="Weather pipeline completed",
        html_content="The weather data pipeline has completed successfully.",
    )

    end = EmptyOperator(task_id="end")

    # DAG Flow
    start >> get_weather_data >> validate_data >> load_data_to_postgres >> check_temp_alert
    start >> create_weather_air_quality_table >> load_data_to_postgres
    check_temp_alert >> [send_alert_email, no_alert]
    [send_alert_email, no_alert] >> send_email >> end