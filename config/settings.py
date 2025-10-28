from dotenv import load_dotenv
import os

load_dotenv()

# RabbitMQ
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "my_queue")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# Directus API
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))
DIRECTUS_API_URL = os.getenv("DIRECTUS_API_URL", "localhost")
DIRECTUS_STATIC_TOKEN = os.getenv("DIRECTUS_STATIC_TOKEN", "")