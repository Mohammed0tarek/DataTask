from confluent_kafka.admin import AdminClient, NewTopic
import pymysql
from pymysql.err import OperationalError, ProgrammingError, MySQLError

def create_kafka_topic(topic_name, kafka_config, num_partitions=1, replication_factor=1):
    """Create a Kafka topic if it doesn't exist."""
    admin_client = AdminClient(kafka_config)
    topics = admin_client.list_topics(timeout=5).topics
    
    if topic_name in topics:
        print(f"Kafka topic '{topic_name}' already exists.")
        return
    
    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    futures = admin_client.create_topics([new_topic])
    
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Kafka topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create Kafka topic '{topic}': {e}")


def create_mysql_table(database_config, table_name, table_schema):
    """Create a MySQL table if it doesn't exist."""
    try:
        connection = pymysql.connect(**database_config)
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if cursor.fetchone():
            print(f"MySQL table '{table_name}' already exists.")
        else:
            # Create the table
            cursor.execute(table_schema)
            print(f"MySQL table '{table_name}' created successfully.")
        
        cursor.close()
        connection.close()
    except OperationalError as e:
        print(f"Operational error: {e}")
    except ProgrammingError as e:
        print(f"Programming error: {e}")
    except MySQLError as e:
        print(f"MySQL error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Kafka Configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    kafka_topic_name = "Transactions"

    # MySQL Configuration
    mysql_config = {
        'user': 'root',
        'password': 'ThisIs7021',
        'host': '127.0.0.1',
        'database': 'CreditTransactionsDB'
    }
    mysql_table_name = "transactions"
    mysql_table_schema = f""" 
CREATE TABLE {mysql_table_name} (
    TransactionId INT AUTO_INCREMENT PRIMARY KEY,
    Time DOUBLE NOT NULL,
    V1 DOUBLE NOT NULL,
    V2 DOUBLE NOT NULL,
    V3 DOUBLE NOT NULL,
    V4 DOUBLE NOT NULL,
    V5 DOUBLE NOT NULL,
    V6 DOUBLE NOT NULL,
    V7 DOUBLE NOT NULL,
    V8 DOUBLE NOT NULL,
    V9 DOUBLE NOT NULL,
    V10 DOUBLE NOT NULL,
    V11 DOUBLE NOT NULL,
    V12 DOUBLE NOT NULL,
    V13 DOUBLE NOT NULL,
    V14 DOUBLE NOT NULL,
    V15 DOUBLE NOT NULL,
    V16 DOUBLE NOT NULL,
    V17 DOUBLE NOT NULL,
    V18 DOUBLE NOT NULL,
    V19 DOUBLE NOT NULL,
    V20 DOUBLE NOT NULL,
    V21 DOUBLE NOT NULL,
    V22 DOUBLE NOT NULL,
    V23 DOUBLE NOT NULL,
    V24 DOUBLE NOT NULL,
    V25 DOUBLE NOT NULL,
    V26 DOUBLE NOT NULL,
    V27 DOUBLE NOT NULL,
    V28 DOUBLE NOT NULL,
    Amount DOUBLE NOT NULL,
    IsFraud BOOLEAN NOT NULL DEFAULT FALSE
    );"""
    # Create Kafka topic
    create_kafka_topic(kafka_topic_name, kafka_config)

    # Create MySQL table
    create_mysql_table(mysql_config, mysql_table_name, mysql_table_schema)
