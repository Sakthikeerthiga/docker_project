from flask import Flask, request, jsonify
from flask_cors import CORS
import pika
import mysql.connector

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database connection
db = mysql.connector.connect(
    host="mysql-container",
    user="root",
    password="password",
    database="shopping"
)
cursor = db.cursor()

# Helper function to send messages to RabbitMQ
def send_to_queue(order):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq-container'))
        channel = connection.channel()
        channel.queue_declare(queue='order_queue')
        channel.basic_publish(exchange='', routing_key='order_queue', body=str(order))
        connection.close()
    except Exception as e:
        raise Exception(f"Error sending to queue: {str(e)}")

# Example route for placing an order
@app.route('/place_order', methods=['POST'])
def place_order():
    try:
        # Parse the order details from the request
        order = request.json
        product_name = order.get('product_name')

        if not product_name:
            return jsonify({'error': 'Product name is required'}), 400

        # Insert order into the database
        cursor.execute("INSERT INTO orders (product_name, status) VALUES (%s, %s)", (product_name, 'Pending'))
        db.commit()

        # Send order details to RabbitMQ
        send_to_queue(order)

        return jsonify({"message": "Order placed successfully!"})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
#     # Route to check order status
@app.route('/order_status/<message_id>', methods=['GET'])
def order_status(message_id):
    try:
        # Query the database for the order status
        cursor.execute("SELECT product_name,status FROM orders WHERE message_id = %s", (message_id,))
        result = cursor.fetchone()

        if result:
            return jsonify({"product_name": result[0], "status": result[1]})
        else:
            return jsonify({"error": "Order not found"}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# New endpoint: Check order status
# @app.route('/order_status/<message_id>', methods=['GET'])
# def order_status(message_id):
#     try:
#         cursor.execute("SELECT product_name,status FROM orders WHERE id = %s", (message_id,))
#         result = cursor.fetchone()
#         if result:
#             return jsonify({"product_name": result[0], "status": result[1]})
#         else:
#             return jsonify({"error": "Order not found"}), 404


if __name__ == '__main__':
    print("Producer Flask app is starting...")
    app.run(host='0.0.0.0', port=5000)
