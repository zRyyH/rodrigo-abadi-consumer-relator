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
            logger.info(f"📦 Buscando {total_count} itens de '{collection}'")
        except Exception as e:
            logger.error(f"❌ Erro ao obter meta dados de '{collection}': {e}")
            return items

        try:
            for page in range(1, pages):
                data = self.directus.get(f"{collection}", params={"page": page})["data"]
                items.extend(data)
        except Exception as e:
            logger.error(f"❌ Erro ao buscar itens de '{collection}': {e}")

        return items

    def update(self, collection, items_update):
        if not items_update:
            return

        total = len(items_update)
        logger.info(f"🔄 Atualizando {total} itens em '{collection}'")

        failed = 0
        for item_data in items_update:
            try:
                self.directus.patch(f"items/{collection}", [item_data])
            except Exception as e:
                failed += 1
                logger.error(f"❌ Erro ao atualizar item em '{collection}': {e}")

        if failed == 0:
            logger.info(f"✅ {total} itens de '{collection}' atualizados")
        else:
            logger.warning(
                f"⚠️  {total - failed}/{total} itens de '{collection}' atualizados ({failed} falhas)"
            )