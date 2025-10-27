import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

from constants import sales, nfes
from io import BytesIO
import openpyxl


class SheetService:
    """Service for extracting and processing data from Excel spreadsheets."""

    def __init__(self, buffers):
        self.buffers = buffers

    def execute(self):
        """
        Execute spreadsheet processing for all types.

        Returns:
            Dictionary with processed sales and nfes data
        """
        return {
            "sales": self._process_sheet("sales", sales),
            "nfes": self._process_sheet("nfes", nfes),
        }

    def _process_sheet(self, sheet_type, config):
        """
        Process a single spreadsheet type.

        Args:
            sheet_type: Type identifier (e.g., 'sales', 'nfes')
            config: Configuration object with config and map attributes

        Returns:
            List of processed records
        """

        file_content = self.buffers[f"{sheet_type}_xlsx"].getvalue()
        rows = self._extract_data(file_content, **config.config)
        return self._convert_rows(rows, config.map)
    
    def _extract_data(self, xlsx_file, linha_inicial, planilha):
        """
        Extract raw data from Excel file.

        Args:
            xlsx_file: Binary content of Excel file
            linha_inicial: Starting row number (1-indexed)
            planilha: Sheet name (None for active sheet)

        Returns:
            List of rows with cell values
        """
        workbook = openpyxl.load_workbook(BytesIO(xlsx_file))
        worksheet = workbook[planilha] if planilha else workbook.active

        all_rows = list(worksheet.values)
        return [list(row) for row in all_rows[linha_inicial - 1 :]]
    
    def _convert_rows(self, rows, field_mapping):
        """
        Convert raw rows to dictionary records based on field mapping.

        Args:
            rows: List of raw row data
            field_mapping: Dictionary mapping column indices to field configuration

        Returns:
            List of dictionary records
        """
        result = []

        for row in rows:
            record = {}

            for column_index, field_config in field_mapping.items():
                field_name = field_config["key"]
                transform_func = field_config["transform"]
                cell_value = row[column_index]

                # Skip empty or whitespace-only values
                if cell_value is None or str(cell_value).strip() == "":
                    continue

                record[field_name] = transform_func(cell_value)

            if record:  # Only add non-empty records
                result.append(record)

        return result
