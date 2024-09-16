from airflow import DAG
from confluent_kafka import Consumer, TopicPartition
import psycopg2

def connect_to_db():
    connection = psycopg2.connect(
        dbname="threadings",  # убедитесь, что это название вашей базы данных
        user="ilyagasan",     # ваше имя пользователя PostgreSQL
        password="29892989",  # ваш пароль для PostgreSQL
        host="localhost",     # сервер базы данных
        port="5432"           # порт PostgreSQL (по умолчанию 5432)
    )
    return connection
