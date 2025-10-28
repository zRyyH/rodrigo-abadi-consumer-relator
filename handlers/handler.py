from logging_config import logger, log_start, log_end, log_result
from services.handler_processor import MessageProcessor

def process_message(ch, method, properties, body):
    delivery_tag = method.delivery_tag

    try:
        log_start("MENSAGEM RECEBIDA")
        log_result("Delivery Tag", delivery_tag)

        processor = MessageProcessor()
        processor.process_data()

        ch.basic_ack(delivery_tag=delivery_tag)
        log_end("MENSAGEM PROCESSADA", success=True)
        
    except Exception as e:
        logger.error(f"❌ Erro no processamento: {str(e)}", exc_info=True)
        ch.basic_nack(delivery_tag=delivery_tag, requeue=True)
        log_end("MENSAGEM PROCESSADA", success=False)