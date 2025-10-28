import pika

credentials = pika.PlainCredentials("admin", "73p8Wg8ibGZJ")
connection = pika.BlockingConnection(
    pika.ConnectionParameters("77.237.246.25", 5672, credentials=credentials)
)
channel = connection.channel()

channel.queue_declare(queue="relate", durable=True)

channel.basic_publish(
    exchange="",
    routing_key="relate",
    body="relate",
    properties=pika.BasicProperties(delivery_mode=2),
)
connection.close()