import psycopg2
from psycopg2 import sql

def connect_to_db():
    connection = psycopg2.connect(
        dbname="threadings",  # убедитесь, что это название вашей базы данных
        user="ilyagasan",     # ваше имя пользователя PostgreSQL
        password="29892989",  # ваш пароль для PostgreSQL
        host="localhost",     # сервер базы данных
        port="5432"           # порт PostgreSQL (по умолчанию 5432)
    )
    return connection



connect_to_db()


import requests
from concurrent.futures import ThreadPoolExecutor
import threading

# Список городов для запроса
cities = ['Moscow', 'London', 'Paris', 'Berlin', 'Tokyo']

# Лок для синхронизации записи в БД
db_lock = threading.Lock()

def fetch_weather_data(city):
    url = f'http://api.weatherapi.com/v1/current.json?key=5c95bb9708a94707bab103249241309&q={city}'
    try:
        response = requests.get(url)
        data = response.json()
        if 'current' in data:
            return {
                'city': city,
                'temperature': data['current']['temp_c'],
                'humidity': data['current']['humidity'],
                'wind_speed': data['current']['wind_kph'],
            }
    except Exception as e:
        print(f"Error fetching data for {city}: {e}")
    return None

import queue
from datetime import datetime

data_queue = queue.Queue()

def save_to_db(data, connection):
    with db_lock:
        cursor = connection.cursor()
        insert_query = sql.SQL(
            "INSERT INTO weather_data (city, temperature, humidity, wind_speed, recorded_at) VALUES (%s, %s, %s, %s, %s)"
        )
        cursor.execute(insert_query, (
            data['city'], data['temperature'], data['humidity'], data['wind_speed'], datetime.now()
        ))
        connection.commit()
        cursor.close()

def worker(connection):
    while not data_queue.empty():
        data = data_queue.get()
        if data:
            save_to_db(data, connection)
        data_queue.task_done()


def main():
    connection = connect_to_db()

    # С помощью ThreadPoolExecutor выполняем параллельные запросы
    with ThreadPoolExecutor(max_workers=5) as executor:
        for city in cities:
            future = executor.submit(fetch_weather_data, city)
            data = future.result()
            if data:
                data_queue.put(data)

    # Запускаем рабочие потоки для записи данных в базу данных
    workers = []
    for _ in range(3):  # 3 потока для записи данных
        thread = threading.Thread(target=worker, args=(connection,))
        thread.start()
        workers.append(thread)

    # Ожидаем завершения всех задач
    data_queue.join()

    # Закрываем все потоки
    for thread in workers:
        thread.join()

    connection.close()

if __name__ == "__main__":
    main()
