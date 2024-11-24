
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaException, KafkaError
import pymysql
import pandas as pd
from sklearn.preprocessing import StandardScaler
import pickle
import xgboost as xgb
import os
from dotenv import load_dotenv
load_dotenv()
db_cofig = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password":os.getenv("DB_USER_PASSWORD"),
    "database":"CreditTransactionsDB",
    "port":3306
}
consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'TransactionConsumer',
        'auto.offset.reset': 'earliest',
        'schema.registry.url': 'http://localhost:8081'
    }

with open('AmountTimeScaler.pkl', 'rb') as scaler_file:
    scaler = pickle.load(scaler_file)
model = xgb.XGBClassifier()
model.load_model('CreditScoringModel.json')
SQL_INSERT_STATEMENT ="""
            INSERT INTO transactions (Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14,V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27,V28, Amount, IsFraud)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);"""
if __name__ == "__main__":

    topic_name = "Transactions"
    db_connection = pymysql.connect(**db_cofig)
    db_cursor = db_connection.cursor()
    # Create an AvroConsumer
    kafka_consumer = AvroConsumer(consumer_config)
    kafka_consumer.subscribe([topic_name])

    print(f"Subscribed to topic: {topic_name}")

    try:
        while True:
            msg = kafka_consumer.poll(1.0)  # Poll for new messages with a 1-second timeout
            if msg is None:
                continue  # No message received within timeout
            if msg.error():
                # Handle Kafka errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Deserialize Avro message
                record = pd.DataFrame([msg.value()]).drop('Class',axis=1)

                record[['Amount','Time']] = scaler.transform(record[['Amount', 'Time']])
                score=model.predict(record)
                record['IsFraud'] = score

                values = tuple(record[col].values[0] for col in record.columns)
                db_cursor.execute(SQL_INSERT_STATEMENT, values)
                db_connection.commit()

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    except KafkaException as e:
        print(f"Kafka exception occurred: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        print("Closing consumer...")
        kafka_consumer.close()
        print("Consumer closed.")
        db_cursor.close()
        print("Cursor closed.")
        db_connection.close()
        print("Connection closed.")

