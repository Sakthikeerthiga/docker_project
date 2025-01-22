FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install pika mysql-connector-python
CMD ["python", "consumer.py"]
