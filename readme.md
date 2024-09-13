

# Weather Data Multithreading with PostgreSQL

## Описание проекта

Этот проект предназначен для многопоточной загрузки данных о погоде из внешнего API и записи этих данных в базу данных PostgreSQL. Для параллельного получения данных с нескольких городов используется библиотека `concurrent.futures.ThreadPoolExecutor`, а для синхронной записи данных в базу данных — библиотека `psycopg2`.

### Основные функции:
- Параллельная загрузка данных о погоде для нескольких городов.
- Запись данных в PostgreSQL с использованием блокировок для синхронизации потоков.
- Обработка ошибок при запросах к API и вставке данных в базу данных.

## Установка

### Шаг 1: Клонирование репозитория

```bash
git clone https://github.com/ilyaGasanDataEngineer/weather-data-postgres.git
cd weather-data-postgres
```

### Шаг 2: Создание виртуального окружения и установка зависимостей

Убедитесь, что у вас установлен Python 3.7 или выше и PostgreSQL.

1. Создайте виртуальное окружение:

```bash
python -m venv venv
source venv/bin/activate   # для Linux/Mac
venv\Scripts\activate      # для Windows
```

2. Установите необходимые зависимости:

```bash
pip install -r requirements.txt
```

### Шаг 3: Настройка базы данных

Создайте базу данных PostgreSQL и таблицу для хранения данных о погоде.

1. Подключитесь к PostgreSQL:
   
```bash
psql -U <your_username>
```

2. Выполните SQL-команды для создания базы данных и таблицы:

```sql
CREATE DATABASE threadings;

\c threadings  -- подключение к созданной базе данных

CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    temperature FLOAT,
    humidity INT,
    wind_speed FLOAT,
    recorded_at TIMESTAMP
);
```

### Шаг 4: Настройка API-ключа

Зарегистрируйтесь на сайте [WeatherAPI](https://www.weatherapi.com/) и получите API-ключ. Вставьте его в код в функцию `fetch_weather_data` вместо `your_api_key`.

```python
url = f'http://api.weatherapi.com/v1/current.json?key=your_api_key&q={city}'
```

### Шаг 5: Запуск проекта

Запустите скрипт:

```bash
python postgress_threading.py
```

## Используемые технологии

- **Python**: Язык программирования для выполнения кода.
- **PostgreSQL**: Система управления базами данных, куда сохраняются данные о погоде.
- **ThreadPoolExecutor**: Многопоточность для параллельной загрузки данных.
- **psycopg2**: Библиотека для взаимодействия с PostgreSQL.

