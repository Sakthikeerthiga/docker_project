import pika
import mysql.connector

db = mysql.connector.connect(
    host="mysql-container",
    user="root",
    password="password",
    database="shopping"
)
cursor = db.cursor()

def callback(ch, method, properties, body):
    print(f"Received {body}")
    order_data = eval(body)
    cursor.execute("UPDATE orders SET status = %s WHERE product_name = %s", ('Processed', order_data['product_name']))
    db.commit()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq-container'))
channel = connection.channel()

channel.queue_declare(queue='order_queue')
channel.basic_consume(queue='order_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit, press CTRL+C')
channel.start_consuming()
