from config.settings import (
    DIRECTUS_API_URL,
    DIRECTUS_STATIC_TOKEN,
)
from logging_config import logger
import requests


class Directus:
    def __init__(self):
        self.url = DIRECTUS_API_URL
        self.headers = {"Authorization": f"Bearer {DIRECTUS_STATIC_TOKEN}"}

    def get(self, collection: str, params: dict = None):
        """Busca por items no directus e retorna um array de items."""
        try:
            response = requests.get(
                f"{self.url}/{collection}",
                headers=self.headers,
                params=params,
                timeout=300,
            )

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f" Directus API Error: {response.json()}")
            logger.error(f"  ❌ Erro ao criar item na collection '{collection}': {str(e)}")
            raise

    def post(self, collection: str, data: dict):
        """Cria um novo item na collection do Directus."""
        try:
            response = requests.post(
                f"{self.url}/{collection}",
                headers=self.headers,
                json=data,
                timeout=300,
            )
            
            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f" Directus API Error: {response.json()}")
            logger.error(f" Data: {data}")
            logger.error(f"  ❌ Erro ao criar item na collection '{collection}' data: {data}: {str(e)}")
            raise

    def patch(self, collection: str, data: dict):
        """Atualiza um item existente na collection do Directus."""
        try:
            response = requests.patch(
                f"{self.url}/{collection}",
                headers=self.headers,
                json=data,
                timeout=300,
            )

            response.raise_for_status()

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f" Directus API Error: {response.json()}")
            logger.error(f" Data: {data}")
            logger.error(f"  ❌ Erro ao criar item na collection '{collection}': {str(e)}")
            raise