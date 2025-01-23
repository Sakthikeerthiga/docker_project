FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install pika mysql-connector-python

RUN pip install -r requirements.txt

CMD ["python", "consumer.py"]
