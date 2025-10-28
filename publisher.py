import pika
import logging

# Configuração de logging - silencia Pika
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silencia logs do Pika
logging.getLogger('pika').setLevel(logging.WARNING)

try:
    credentials = pika.PlainCredentials("admin", "73p8Wg8ibGZJ")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("77.237.246.25", 5672, credentials=credentials)
    )
    logger.info("Conexão estabelecida com RabbitMQ")
    
    channel = connection.channel()
    channel.queue_declare(queue="relate", durable=True)
    
    channel.basic_publish(
        exchange="",
        routing_key="relate",
        body="relate",
        properties=pika.BasicProperties(delivery_mode=2),
    )
    logger.info("Mensagem publicada na fila 'relate'")
    
    connection.close()
    logger.info("Conexão encerrada")

except pika.exceptions.AMQPConnectionError as e:
    logger.error(f"Falha na conexão: {e}")
except Exception as e:
    logger.error(f"Erro inesperado: {e}")