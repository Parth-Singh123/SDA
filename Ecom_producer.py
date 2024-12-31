import pandas as pd
import time
from kafka import KafkaProducer
import json
from pymongo import MongoClient


def read_Ecom_data_from_csv(file_path):
    return pd.read_csv(file_path)

def send_to_kafka(producer, topic, message):
    producer.send(topic, value=json.dumps(message).encode('utf-8'))
    producer.flush()
    print("Sent to Kafka:", message)

def insert_into_mongo(client, collection, message):
    collection.insert_one(message)

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    ecommerce_data = read_Ecom_data_from_csv('ecommerce_dataset.csv')
    client = MongoClient("mongodb+srv://mongoadmin:mongoadmin@sda2.cz0sc.mongodb.net/")  # replace with your MongoDB connection string
    db = client['ecommerce']  # replace with your database name
    collection = db['ecommerce_data']  # replace with your collection name

    print("Streaming Started")
    for index, row in ecommerce_data.iterrows():
        message = {
            'product_name': row['product_name'],
            'quantity': row['quantity'],
            'price_per_unit': row['price_per_unit'],
            'location': row['location'],
            'timestamp': row['cart_last_updated']
        }
        print("Iteration Started:", message)

        send_to_kafka(producer, 'ecommerce_topic', message)
        insert_into_mongo(client, collection, message)
        time.sleep(1)  # Adjust the sleep time as needed

if __name__ == "__main__":
    main()
