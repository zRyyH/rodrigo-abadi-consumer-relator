from config.settings import GET_BATCH_SIZE
from integration.directus import Directus
from logging_config import logger
import json


class ApiService:
    def __init__(self):
        self.directus = Directus()

    def check_existing(self, collection, key_name, key_values):
        if not key_values: return []

        existing_items = []
        
        try:
            for i in range(0, len(key_values), GET_BATCH_SIZE):
                batch = key_values[i:i + GET_BATCH_SIZE]
                params = {
                    "filter": json.dumps({key_name: {"_in": batch}}),
                    "fields": ["*"]
                }
                response = self.directus.get(f"items/{collection}", params=params)
                existing_items.extend(response.get("data", []))
                logger.info(f"📋 {i} {collection} verificados")
            
            logger.info(f"✅ {len(existing_items)}/{len(key_values)} {collection} já existem")
            return existing_items
        except Exception as e:
            logger.error(f"❌ Erro ao verificar {collection}: {e}")
            raise

    def create(self, collection, items_data):
        if not items_data: return
        
        for i, item_data in enumerate(items_data, 1):
            try:
                self.directus.post(f"items/{collection}", item_data)
                logger.info(f"📊 {i}/{len(items_data)} {collection} criados")
            except Exception as e:
                logger.error(f"❌ {i} {collection} item {i}: {e}")
        
        logger.info(f"✅ Concluído: {collection} criados")

    def update(self, collection, items_update, key_name):
        if not items_update: return
        
        for i, item_data in enumerate(items_update, 1):
            try:
                key_value = item_data.get(key_name)

                update_data = [{k: v for k, v in item_data.items() if k != key_name}]
                self.directus.patch(f"items/{collection}", update_data)

                logger.info(f"📊 {i}/{len(items_update)} {collection} atualizados")
            except Exception as e:
                logger.error(f"❌ {i} {collection} {key_value}: {e}")

        logger.info(f"✅ Concluído: {collection} atualizados")