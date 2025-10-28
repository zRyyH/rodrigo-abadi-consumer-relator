from logging_config import logger
from config.settings import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
)
import time
import pika


def get_connection(retry_delay=5):
    """
    Retorna uma conexão RabbitMQ com reconexão automática em caso de falha.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=300,
        blocked_connection_timeout=300,
    )

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            if connection.is_open:
                logger.info(
                    f"✅ Conectado ao RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT})"
                )
                return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"❌ Falha na conexão RabbitMQ: {e}")
            logger.info(f"⏳ Reconectando em {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}")
            time.sleep(retry_delay)