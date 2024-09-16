import json
import psycopg2
from confluent_kafka import Consumer

def connect_to_db():
    connection = psycopg2.connect(
        dbname="threadings",
        user="ilyagasan",
        password="29892989",
        host="localhost",
        port="5432"
    )
    return connection

def read_data_from_broker():
    CONSUMER_CONFIG = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test',
        'auto.offset.reset': 'earliest',
    }
    consumer = Consumer(CONSUMER_CONFIG)
    running = True

    connection = connect_to_db()
    try:
        consumer.subscribe(['asinc_sending'])

        while running:
            msg = consumer.poll(timeout=0)
            if msg is not None:
                if msg.error():
                    print('Consumer error: {}'.format(msg.error()))
                else:
                    data = json.loads(msg.value().decode('utf-8'))
                    running = False
                    cursor = connection.cursor()

                    for city_data in data:
                        city = city_data["city"]
                        temperature = city_data["temperature"]
                        humidity = city_data["humidity"]
                        wind_speed = city_data["wind_speed"]

                        sql_select = """
                            SELECT 1 FROM weather_data
                            WHERE city = %s;
                        """
                        cursor.execute(sql_select, (city,))
                        result = cursor.fetchone()

                        if result:
                            sql_update = """
                                UPDATE weather_data
                                SET temperature = %s, humidity = %s, wind_speed = %s
                                WHERE city = %s;
                            """
                            cursor.execute(sql_update, (temperature, humidity, wind_speed, city))
                            print(f"Запись для города {city} обновлена.")
                        else:
                            sql_insert = """
                                INSERT INTO weather_data (city, temperature, humidity, wind_speed)
                                VALUES (%s, %s, %s, %s);
                            """
                            cursor.execute(sql_insert, (city, temperature, humidity, wind_speed))
                            print(f"Новая запись для города {city} добавлена.")

                    connection.commit()

    except Exception as e:
        print(e)
    finally:
        connection.close()

read_data_from_broker()
