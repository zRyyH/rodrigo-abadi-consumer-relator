from integration.directus import Directus
from config.settings import BATCH_SIZE
from logging_config import logger
import math


class ApiService:
    def __init__(self):
        self.directus = Directus()

    def _get_meta(self, collection):
        return self.directus.get(f"{collection}", {"limit": 0, "meta": "*"})["meta"]

    def get_all(self, collection):
        items = []

        try:
            total_count = self._get_meta(collection)["total_count"]
            pages = math.ceil(total_count / BATCH_SIZE) + 1
        except Exception as e:
            logger.error(f"❌ Erro ao obter meta dados: {e}")

        try:
            for page in range(1, pages):
                data = self.directus.get(f"{collection}", params={"page": page})["data"]
                items.extend(data)
                logger.info(
                    f"Collection: {collection}, Lote: {page*BATCH_SIZE}/{total_count}"
                )
        except Exception as e:
            logger.error(f"❌ Erro ao obter items paginados: {e}")

        return items

    def update(self, collection, items_update):
        for i, item_data in enumerate(items_update, 1):
            try:
                self.directus.patch(f"items/{collection}", [item_data])
                logger.info(f"📊 {i}/{len(items_update)} {collection} atualizados")
            except Exception as e:
                logger.error(f"❌ {i} {collection}: {e}")

        logger.info(f"✅ Concluído: {collection} atualizados")
