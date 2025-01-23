from flask import Flask, request, jsonify
from flask_cors import CORS
import pika
import mysql.connector
import logging
from email.mime.text import MIMEText
import smtplib
from jsonschema import validate, ValidationError
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(filename='producer.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# App setup
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection
# db = mysql.connector.connect(
#     host=os.getenv("MYSQL_HOST"),
#     user=os.getenv("MYSQL_USER"),
#     password=os.getenv("MYSQL_PASSWORD"),
#     database=os.getenv("MYSQL_DB")
# )

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

# Email settings
EMAIL_SENDER = "mailforwork2026@gmail.com"
EMAIL_PASSWORD = "oaoazhrjhszftkvn"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Input validation schema
order_schema = {
    "type": "object",
    "properties": {
        "product_name": {"type": "string"}
    },
    "required": ["product_name"]
}

# Helper function to send emails
def send_email(to_email, subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = to_email

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, to_email, msg.as_string())
    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")
        raise

# Helper function to send messages to RabbitMQ
def send_to_queue(order):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue='order_queue')
        channel.basic_publish(exchange='', routing_key='order_queue', body=str(order))
        connection.close()
    except Exception as e:
        logging.error(f"Error sending to RabbitMQ: {str(e)}")
        raise

# Route to place an order

@app.route('/place_order', methods=['POST'])
def place_order():
    try:
        order = request.json
        validate(instance=order, schema=order_schema)

        product_name = order['product_name']

        # Insert order into the database and retrieve the order id
        cursor.execute("INSERT INTO orders (product_name, status) VALUES (%s, %s)", (product_name, 'Pending'))
        db.commit()
        order_id = cursor.lastrowid  # Get the last inserted ID from the database

        # Add the order ID to the order data
        order['id'] = order_id

        # Send order details to RabbitMQ
        send_to_queue(order)

        # Send email notification
        send_email("mailforwork2026@gmail.com", "Order Confirmation", f"Your order for {product_name} has been placed.")

        return jsonify({"message": "Order placed successfully!"})
    except ValidationError as ve:
        return jsonify({'error': f"Invalid input: {str(ve)}"}), 400
    except Exception as e:
        logging.error(f"Error in /place_order: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
# @app.route('/place_order', methods=['POST'])
# def place_order():
#     try:
#         order = request.json
#         validate(instance=order, schema=order_schema)

#         product_name = order['product_name']

#         # Insert order into the database
#         cursor.execute("INSERT INTO orders (product_name, status) VALUES (%s, %s)", (product_name, 'Pending'))
#         db.commit()

#         # Send order details to RabbitMQ
#         send_to_queue(order)

#         # Send email notification
#         send_email("mailforwork2026@gmail.com", "Order Confirmation", f"Your order for {product_name} has been placed.")

#         return jsonify({"message": "Order placed successfully!"})
#     except ValidationError as ve:
#         return jsonify({'error': f"Invalid input: {str(ve)}"}), 400
#     except Exception as e:
#         logging.error(f"Error in /place_order: {str(e)}")
#         return jsonify({'error': str(e)}), 500

# Route to check order status
@app.route('/order_status/<int:message_id>', methods=['GET'])
def order_status(message_id):
    try:
        cursor.execute("SELECT product_name, status FROM orders WHERE id = %s", (message_id,))
        result = cursor.fetchone()

        if result:
            return jsonify({"product_name": result[0], "status": result[1]})
        else:
            return jsonify({"error": "Order not found"}), 404
    except Exception as e:
        logging.error(f"Error in /order_status: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Route to retrieve all orders
@app.route('/order_history', methods=['GET'])
def order_history():
    try:
        cursor.execute("SELECT id, product_name, status FROM orders")
        orders = cursor.fetchall()
        return jsonify([{"id": order[0], "product_name": order[1], "status": order[2]} for order in orders])
    except Exception as e:
        logging.error(f"Error in /order_history: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("Producer Flask app is starting...")
    app.run(host='0.0.0.0', port=5000)
