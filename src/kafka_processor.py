import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'input_topic'
EVEN_TOPIC = 'EVEN_TOPIC'
ODD_TOPIC = 'ODD_TOPIC'

# Configure Kafka consumer and producers
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer_even = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

producer_odd = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# File paths
EVEN_FILE_PATH = "even_results.txt"
ODD_FILE_PATH = "odd_results.txt"

# Helper function to calculate age from DOB
def calculate_age(dob_str):
    dob = datetime.strptime(dob_str, "%Y-%m-%d")
    return datetime.now().year - dob.year

# Process messages from the Kafka input topic
def process_messages():
    for message in consumer:
        data = message.value
        try:
            name = data['name']
            address = data['address']
            dob = data['dob']
            age = calculate_age(dob)

            # Prepare result message
            result = {
                "name": name,
                "address": address,
                "dob": dob,
                "age": age
            }

            # Publish to appropriate Kafka topic and file
            if age % 2 == 0:
                producer_even.send(EVEN_TOPIC, value=result)
                with open(EVEN_FILE_PATH, "a") as even_file:
                    even_file.write(json.dumps(result) + "\n")
                print(f"Published to EVEN_TOPIC: {result}")
            else:
                producer_odd.send(ODD_TOPIC, value=result)
                with open(ODD_FILE_PATH, "a") as odd_file:
                    odd_file.write(json.dumps(result) + "\n")
                print(f"Published to ODD_TOPIC: {result}")

        except KeyError as e:
            print(f"Error processing message - Missing key: {e}")

if __name__ == "__main__":
    try:
        print("Starting Kafka message processing...")
        process_messages()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        # Close Kafka connections
        consumer.close()
        producer_even.close()
        producer_odd.close()
