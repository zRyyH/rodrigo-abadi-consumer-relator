from logging_config import logger, log_result
from io import BytesIO
import requests


class FileService:
    """Service for downloading, extracting and renaming files from zip archives."""

    def __init__(self, data, buffers):
        self.buffers = buffers
        self.data = data
        
    def download_files(self):
        """Download files from provided URLs and store in buffers."""
        file_count = len(self.data["files"])
        logger.info(f"  • Baixando {file_count} arquivo(s)...")

        for file_key, file_info in self.data["files"].items():
            try:
                response = requests.get(file_info["downloadUrl"], timeout=30)
                response.raise_for_status()
                self.buffers[file_key] = BytesIO(response.content)

            except requests.exceptions.RequestException as e:
                logger.error(f"  ❌ Erro ao baixar {file_key}: {str(e)}")
                raise

        log_result("Downloads concluídos", file_count)