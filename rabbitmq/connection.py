from logging_config import logger, log_start
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

    :param retry_delay: tempo em segundos entre tentativas de reconexão
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
                log_start("✅ Conectado ao RabbitMQ")
                return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"❌ Erro ao conectar ao RabbitMQ: {e}")
            logger.info(f"⏳ Tentando reconectar em {retry_delay}s...")
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}")
            logger.info(f"⏳ Tentando reconectar em {retry_delay}s...")
            time.sleep(retry_delay)