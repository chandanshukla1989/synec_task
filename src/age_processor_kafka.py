from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json
from datetime import datetime

def calculate_age(dob):
    dob_date = datetime.strptime(dob, "%Y-%m-%d")
    today = datetime.today()
    age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
    return age

def main():
    # Set up the Flink execution environment with JAR dependencies
    config = Configuration()
    config.set_string(
        "pipeline.jars",
        "file:///home/ubuntu/flink-1.13.2/lib/flink-connector-kafka_2.12-1.13.2.jar,"
        "file:///home/ubuntu/flink-1.13.2/lib/kafka-clients-2.8.0.jar"
    )
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    # Kafka source and producers
    kafka_source = FlinkKafkaConsumer(
        topics='input_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink-group'}
    )

    even_producer = FlinkKafkaProducer(
        topic='EVEN_TOPIC',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    odd_producer = FlinkKafkaProducer(
        topic='ODD_TOPIC',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    # Process and route
    def process_and_route(record_json):
        record = json.loads(record_json)
        age = calculate_age(record['DateOfBirth'])
        record['Age'] = age
        output_json = json.dumps(record)
        if age % 2 == 0:
            even_producer.invoke(output_json, None)
        else:
            odd_producer.invoke(output_json, None)
        return output_json

    input_stream = env.add_source(kafka_source)
    processed_stream = input_stream.map(process_and_route)
    processed_stream.print()

    # Execute the job
    env.execute("Kafka Age Processing Job")

if __name__ == "__main__":
    main()
