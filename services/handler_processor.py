from services.data_relation import DataRelation
from services.api_service import ApiService
from logging_config import logger


class MessageProcessor:
    """
    Main orchestrator for the data processing pipeline.
    Coordinates different stages without implementing business logic.
    """

    def __init__(self):
        self.api = ApiService()
        self.data_relation = DataRelation()

    def _get_related_nfes(self):
        files = self.api.get_all("files")
        nfes = self.api.get_all(f"items/nfes")
        related = self.data_relation.relate_nfes(files, nfes)
        logger.info(f"🔗 {len(related)} NFes relacionadas com arquivos")
        return related

    def _get_relate_sales(self):
        nfes = self.api.get_all("items/nfes")
        products = self.api.get_all("items/products")
        sales = self.api.get_all(f"items/sales")
        related = self.data_relation.relate_sales(nfes, products, sales)
        logger.info(f"🔗 {len(related)} vendas relacionadas")
        return related

    def process_data(self):
        """Main processing pipeline."""
        logger.info("=" * 50)
        logger.info("🚀 Iniciando processamento de dados")
        logger.info("=" * 50)

        related_nfes = self._get_related_nfes()
        related_sales = self._get_relate_sales()

        self.api.update("sales", related_sales)
        self.api.update("nfes", related_nfes)

        logger.info("=" * 50)
        logger.info("✅ Processamento concluído")
        logger.info("=" * 50)