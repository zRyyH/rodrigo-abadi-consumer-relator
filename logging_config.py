from logging.handlers import RotatingFileHandler
import logging
import os
import sys

# Caminho da pasta de logs
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Configuração do logger principal
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)

# Handler para arquivo (com rotação)
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding="utf-8",
)

# Handler para terminal
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Formato limpo e estruturado
formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Evitar duplicatas
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)