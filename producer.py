import json
import csv
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
import os
from dotenv import load_dotenv
load_dotenv()
def delivery_report(err, msg):
    """
    Callback for delivery reports from the producer.
    This method is triggered after a message is successfully sent or fails.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

file_path = 'creditcard.csv'

if __name__ == "__main__":
    with open('transaction.avsc', 'r') as schema_file:
        schema_str = schema_file.read()
    # Load Avro schema from file
    value_schema = avro.loads(schema_str)

    # Configure the AvroProducer
    producer_config = {
        'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
        'schema.registry.url': 'http://localhost:8081'
    }
    producer = AvroProducer(producer_config, default_value_schema=value_schema)

    topic_name = "Transactions"

    print("Producer created successfully!")
    print("Reading data from source file...")

    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    producer.produce(topic=topic_name, value=row, callback=delivery_report)
                    producer.poll(0.7)
                    print("Message sent successfully!")
                except Exception as e:
                    print(f"Error producing message: {e}")
                break  # Exit input loop to process the next row

    except KeyboardInterrupt:
        print("\nProducer closed by user. Exiting...")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        # Ensure resources are released
        print("Closing producer...")
        producer.flush()
        print("Producer closed.")
