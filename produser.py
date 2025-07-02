# produser.py
import psycopg2
import json
import time
from kafka import KafkaProducer

# Параметры подключения к PostgreSQL
DB_PARAMS = {
    "dbname": "test_db",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}

# SQL-запрос для выбора данных из таблицы, которые ещё не отправлялись в Kafka
SELECT_QUERY = """
    SELECT id, username, event_type, EXTRACT(EPOCH FROM event_time)::FLOAT AS timestamp 
    FROM user_login
    WHERE sent_to_kafka = FALSE
"""

# SQL-запрос для обновления статуса "отправленной" записи
UPDATE_QUERY = """
    UPDATE user_login
    SET sent_to_kafka = TRUE 
    WHERE id = %s
"""

# Создаем Kafka-продюсер
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Соединение с PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

try:
    # Выполняем SQL-запрос для выборки записей, которые ещё не отправляли в Kafka
    cursor.execute(SELECT_QUERY)

    # Цикл перебора всех  записей
    for row in cursor.fetchall():
        # Собираем данные для отправки в Kafka
        producer.send(
            'user_event',
            value={
            "user": row[1],       # Значение поля "username"
            "event": row[2],      # Значение поля "event_type"
            "timestamp": row[3]   # Значение поля "timestamp"
        })

        # Обновляем поле sent_to_kafka, определяя по полю "id"
        cursor.execute(UPDATE_QUERY, (row[0],))
        conn.commit()  # Сохраняем изменения

        # Ждем 0.5 секунд перед следующей итерацией
        time.sleep(0.5)

except Exception as e:
    # Обработка возможных ошибок
    print(f"Error: {e}")

finally:
    # Закрытие ресурсов (курсор, соединение с базой данных и Kafka-producer)
    cursor.close()
    conn.close()
    producer.close()