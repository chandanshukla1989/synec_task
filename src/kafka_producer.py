import json
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# Sample data
messages = [
    {"name": "Alice", "address": "123 Main St", "dob": "1990-05-14"},
    {"name": "Bob", "address": "456 Park Ave", "dob": "1985-08-21"},
    {"name": "Charlie", "address": "789 Elm St", "dob": "2002-11-30"}
]

# Publish messages to input_topic
for message in messages:
    producer.send('input_topic', value=message)
    print(f"Produced: {message}")

producer.flush()
producer.close()
