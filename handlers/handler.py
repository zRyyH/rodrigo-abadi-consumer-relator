from logging_config import logger
from services.handler_processor import MessageProcessor


def process_message(ch, method, properties, body):
    delivery_tag = method.delivery_tag

    try:
        logger.info(f"📥 Processando mensagem #{delivery_tag}")

        processor = MessageProcessor()
        processor.process_data()

        ch.basic_ack(delivery_tag=delivery_tag)
        logger.info(f"✅ Mensagem #{delivery_tag} processada com sucesso")

    except Exception as e:
        logger.error(f"❌ Erro ao processar mensagem #{delivery_tag}: {str(e)}")
        ch.basic_nack(delivery_tag=delivery_tag, requeue=True)