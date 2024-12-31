from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from datetime import datetime, timedelta

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'ecommerce_topic',  # Match the topic used in the producer
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ecommerce-consumer-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Connect to MongoDB
mongo_client = MongoClient('mongodb+srv://mongoadmin:mongoadmin@sda2.cz0sc.mongodb.net/')  # Replace with your MongoDB connection string
db = mongo_client['ecommerce']  # Match the database name in the producer
collection = db['ecommerce_data']  # Match the collection name in the producer

# In-memory storage for alerts
cart_last_updated = {}
cart_prices = {}

print("Kafka Consumer is now running...")
for message in consumer:
    # Retrieve the message value as a dictionary
    data = message.value

    # Insert the data into MongoDB
    collection.insert_one(data)

    # Print a confirmation message
    print(f"Inserted record into MongoDB: {data}")

    # Extract necessary fields
    cart_id = data.get('cart_id')
    user_id = data.get('user_id')
    product_name = data.get('product_name')
    cart_last_update_time = data.get('cart_last_updated')
    price = data.get('price_per_unit')
    stock_status = data.get('product_stock_status')

    # Parse cart_last_updated as datetime
    if cart_last_update_time:
        cart_last_update_time = datetime.strptime(cart_last_update_time, '%d-%m-%Y %H:%M')

    # Alert 1: Cart Abandonment
    if cart_id:
        if cart_id in cart_last_updated:
            last_update = cart_last_updated[cart_id]
            if datetime.now() - last_update > timedelta(minutes=10):
                print(f"Alert! Cart {cart_id} abandoned by user {user_id}.")
        cart_last_updated[cart_id] = datetime.now()

    # Alert 2: Price Drop
    if cart_id and price:
        if cart_id in cart_prices:
            previous_price = cart_prices[cart_id]
            if price < previous_price:
                print(f"Alert! Price drop detected for cart {cart_id} (Product: {product_name}). Previous: {previous_price}, New: {price}.")
        cart_prices[cart_id] = price

    # Alert 3: Stock Status
    if stock_status == 'Low Stock':
        print(f"Alert! Attention: Low inventory for product {product_name}.")
    elif stock_status == 'Out of Stock':
        print(f"Alert! Immediate action required: {product_name} is out of stock.")
