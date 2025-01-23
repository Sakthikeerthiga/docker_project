import pika
import mysql.connector
import logging
from dotenv import load_dotenv
import os


# Set up logging
logging.basicConfig(filename='consumer.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection

MYSQL_HOST = "mysql-container"
MYSQL_USER = "root"
MYSQL_PASSWORD = "password"
MYSQL_DB = "shopping"

# RabbitMQ settings


db = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)

cursor = db.cursor()

# RabbitMQ connection
RABBITMQ_HOST = "rabbitmq-container"

# Callback function for processing messages
def callback(ch, method, properties, body):
    try:
        print(f"Received {body}")
        order_data = eval(body)
        
        # Ensure 'id' exists in order_data before proceeding
        if 'id' in order_data:
            cursor.execute("UPDATE orders SET status = %s WHERE id = %s", ('Processed', order_data['id']))
            db.commit()
            print(f"Order {order_data['id']} status updated to 'Processed'.")
        else:
            logging.error("Order ID is missing in the received message.")
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")

# Start consuming messages
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()
channel.queue_declare(queue='order_queue')
channel.basic_consume(queue='order_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit, press CTRL+C')
channel.start_consuming()
