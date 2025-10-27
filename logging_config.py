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

# --- HANDLER PARA ARQUIVO (com rotação) ---
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding="utf-8",
)

# --- HANDLER PARA TERMINAL ---
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# --- FORMATO LIMPO E ESTRUTURADO ---
formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# --- EVITAR DUPLICATAS ---
if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# --- FUNÇÕES AUXILIARES PARA LOGS ESTRUTURADOS ---
def log_step(step_number: int, total_steps: int, description: str):
    """Log de etapa do processo"""
    logger.info(f"[{step_number}/{total_steps}] {description}")


def log_start(process_name: str):
    """Log de início de processo"""
    logger.info(f"{'='*70}")
    logger.info(f"INICIANDO: {process_name}")
    logger.info(f"{'='*70}")


def log_end(process_name: str, success: bool = True):
    """Log de fim de processo"""
    status = "✅ CONCLUÍDO" if success else "❌ FALHOU"
    logger.info(f"{'='*70}")
    logger.info(f"{status}: {process_name}")
    logger.info(f"{'='*70}")


def log_result(description: str, value):
    """Log de resultado com destaque"""
    logger.info(f"  → {description}: {value}")


def log_error_detail(error: Exception):
    """Log de erro com detalhes"""
    logger.error(f"  ❌ Erro: {type(error).__name__}")
    logger.error(f"  → {str(error)}", exc_info=True)
