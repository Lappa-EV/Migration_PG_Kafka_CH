# consumer.py
from kafka import KafkaConsumer
import json
import clickhouse_connect

# Создаем Kafka-потребителя
consumer = KafkaConsumer(
    "user_event",
    bootstrap_servers="localhost:9092",
    group_id="user-login-consumer",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Подключение к ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='user',
    password='strongpassword'
)

# Создание таблицы в ClickHouse, если её ещё нет
client.command("""
CREATE TABLE IF NOT EXISTS user_login (
    username String,
    event_type String,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY event_time
""")

# Начинаем получать сообщения из темы Kafka
for message in consumer: # Переменная теперь называется consumer
    data = message.value  # Данные из Kafka
    print("Received:", data)

    # Выполняем вставку полученных данных в таблицу ClickHouse
    client.command(
        f"""
        INSERT INTO user_login (username, event_type, event_time) 
        VALUES ('{data['user']}', '{data['event']}', toDateTime('{data['timestamp']}')); """
    )
