from rabbitmq.connection import get_connection
from handlers.handler import process_message
from config.settings import RABBITMQ_QUEUE
from logging_config import logger


def start_consumer():
    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message)

    logger.info(f"📥 Aguardando mensagens na fila '{RABBITMQ_QUEUE}'...")
    channel.start_consuming()