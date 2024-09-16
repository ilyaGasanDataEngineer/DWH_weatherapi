import socket
import json
import requests
import psycopg2
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer, KafkaError

cities = ['Moscow', 'London', 'Paris', 'Berlin', 'Tokyo']


def connect_to_db():
    connection = psycopg2.connect(
        dbname="threadings",  # убедитесь, что это название вашей базы данных
        user="ilyagasan",  # ваше имя пользователя PostgreSQL
        password="29892989",  # ваш пароль для PostgreSQL
        host="localhost",  # сервер базы данных
        port="5432"  # порт PostgreSQL (по умолчанию 5432)
    )
    return connection


def fetch_weather_data():
    for city in cities:
        try:
            url = f'http://api.weatherapi.com/v1/current.json?key=5c95bb9708a94707bab103249241309&q={city}'
            response = requests.get(url)
            data = response.json()
            if 'current' in data:
                yield {
                    'city': city,
                    'temperature': data['current']['temp_c'],
                    'humidity': data['current']['humidity'],
                    'wind_speed': data['current']['wind_kph'],
                }
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")


def send_to_kafka():
    conf = {'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname()}
    print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    try:
        producer = Producer(conf)
        print("Kafka producer created successfully")
    except KafkaError as e:
        print(f"Error creating Kafka producer: {e}")
        return

    for data in fetch_weather_data():
        try:
            print(f"Preparing to send data: {data}")
            producer.produce(
                'asinc_sending',
                key=str(data['city']).encode('utf-8'),
                value=json.dumps(data).encode('utf-8')  # Преобразуем данные в JSON
            )
            print(f"Sent data for {data['city']}: {data}")
        except Exception as e:
            print(f"Failed to send data for {data['city']}: {e}")
        finally:
            producer.flush()  # Дожидаемся отправки сообщений

def test():
    print('okay')

# Аргументы DAG
args = {
    'owner': 'ilyagasan',
    'start_date': datetime(2024, 9, 13, 16, 50)
}

# Определение DAG
with DAG(description='Producer must run before consumer, check start date or schedule interval',
         dag_id="kafka_dag_asinc_producer_2.0",
         schedule_interval='@daily',
         default_args=args,
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id='kafka_producer',
        python_callable=send_to_kafka  # Синхронная функция
    )
    task2 = PythonOperator(
        task_id='okay',
        python_callable=test  # Синхронная функция
    )

    task1 >> task2
