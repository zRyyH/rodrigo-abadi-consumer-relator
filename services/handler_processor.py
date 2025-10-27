from logging_config import logger, log_start, log_end
from services.upsert_service import UpsertCollection
from services.sheet_service import SheetService
from services.file_service import FileService
from services.api_service import ApiService


class MessageProcessor:
    """
    Main orchestrator for the data processing pipeline.
    Coordinates different stages without implementing business logic.
    """

    def __init__(self, data):
        self.data = data
        self.buffers = {}
        self.sheets = {}

        self.api = ApiService()
        
        self.upsert_service = UpsertCollection(self.api)
        self.file_service = FileService(self.data, self.buffers)
        self.sheet_service = SheetService(self.buffers)
    

    def _create_and_update_collection(self, collection, key_name):
        nfes = self.sheets[collection]
        nfes_keys = [nfe[key_name] for nfe in nfes]

        existing_nfes = self.api.check_existing(collection, key_name, nfes_keys)

        self.upsert_service.merge_items_by_key(nfes, existing_nfes, key_name)
        self.upsert_service.upsert(collection)


    def process_data(self):
        """Main data processing pipeline."""
        log_start("PIPELINE DE PROCESSAMENTO")

        try:
            self.file_service.download_files()
            self.sheets = self.sheet_service.execute()

            self._create_and_update_collection("nfes", "nfe_key")
            self._create_and_update_collection("sales", "sale_id")

            log_end("PIPELINE DE PROCESSAMENTO", success=True)
            return True

        except Exception as e:
            logger.error(f"❌ Pipeline falhou: {str(e)}", exc_info=True)
            log_end("PIPELINE DE PROCESSAMENTO", success=False)
            raise