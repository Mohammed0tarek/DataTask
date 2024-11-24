
# **Project Title**

A comprehensive data pipeline leveraging Kafka, Confluent, and MySQL to ingest, process, and store data. This project demonstrates the implementation of a robust data pipeline using Kafka producers and consumers, schema enforcement with Avro, and seamless integration with MySQL.

---

## **Table of Contents**

- [Overview](#overview)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Setup](#setup)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

---

## **Overview**

This project was a simple task assigned to me during a hiring process for a startup.builds a complete data pipeline that:
1. Ingests data using Kafka producers.
2. Enforces schemas using Confluent's Schema Registry with Avro.
3. Processes the data using Kafka consumers.
4. Stores the processed data in a MySQL database for further analysis and querying.

---

## **Features**
1. Data Ingestion (Kafka): The system reads payment transaction data from an Apache Kafka stream in real time. Kafka serves as a robust, scalable solution for handling highthroughput, real-time data feeds.
2. Transaction Scoring (Machine Learning Model): A pre-trained machine learning model will be used to score each transaction based on various features. The model can be designed for fraud detection, credit scoring, or other relevant evaluations.
3. Data Storage (MySQL): The scored transactions will be written into a MySQL database. This integration ensures that scoring results are immediately available for reporting,analysis, and further action.

---

## **Technologies Used**

- **Kafka**: Distributed event streaming platform for real-time data processing.
- **Confluent Platform**: Kafka management tools and Schema Registry.
- **Avro**: Schema definition and enforcement.
- **MySQL**: Relational database for data storage.
- **Python/Java**: (Specify language if relevant) Programming language used for producer and consumer implementation.

---

## **Setup**

### Prerequisites
- Kafka and Confluent Platform installed.
- MySQL database setup and running.
- Required libraries and dependencies installed.

### Steps
1. **Kafka Setup**:
   - Start Kafka brokers and Zookeeper.
   ```bash
   bash /bin/zookeper-server-start.sh $KAFKA_HOME/config/zookeper.properties
   bash /bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
   ```
   - Configure Kafka topics for data ingestion and processing.
   you can do it like this. we will create a topic called Transactions since this is the name we used.
   ```bash
   bash kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Transactions --partitions 1 --replication-factor 1
   ```

2. **Confluent Setup**:
   - Configure Confluent Schema Registry.
   After downloading the platform tarball use:
   ```bash
   bash tar -xzf confluent-7.7.1.tar.gz
   bash cd confluent-7.7.1
   bash ./bin/schema-registry-start.sh /etc/schema-registry.properties
   ```
   - Set up Confluent API for Kafka topic management.
   ```bash
   bash pip install confluent-kafka
   ```
3. **Avro Schema**:
   - Define and register Avro schemas for data validation.
   we created the avro schema separately in a file. 
4. **MySQL Database**:
   - Create the necessary database and tables for storing processed data.
   open mysql workbench or cli. For me I used the cli
   ```bash 
   bash mysql -u root -p -h 127.0.0.1
   ```
   I used the loopback address instead of localhost because for some reason it kept denying the access. 
   Enter your password.
   If you don't have the database and it is you first time then use the sql script provided.

5. **Run the Pipeline**:
   - Start Kafka producers to publish data.
   You will need to edit the code to change the user, host, user-password, and the bootstrap-servers.
   - Start Kafka consumers to process data and load it into MySQL.

---

## **Usage**
1. **Run the Admin**:
   - This script creates the Database table and Kafka topic if they are not created
   ```bash
   python admin.py
   ```
2. **Run the Producer**:
   - Publish data using the Kafka producer:
     ```bash
     python producer.py
     ```
3. **Run the Consumer**:
   - Consume and process data using the Kafka consumer:
     ```bash
     python consumer.py
     ```

---
## **Contributing**

Contributions are welcome! Please create a pull request or open an issue for feedback and suggestions.

---

## **License**

Use whatever you want from here. But note that we are using confluent so make sure you have the proper permessions if you plan to use it for a commercial purpose.

---
