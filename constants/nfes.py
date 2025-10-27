from utils.formatters import brl_to_float, br_datetime_to_iso_utc

config = {"linha_inicial": 3, "planilha": "Invoices"}

map = {
    0: {"key": "status", "transform": str},
    1: {"key": "sale_or_dispatch", "transform": str},
    2: {"key": "invoice_number", "transform": int},
    3: {"key": "series", "transform": str},
    4: {"key": "customer_name", "transform": str},
    5: {"key": "nfe_key", "transform": str},
    6: {"key": "modality", "transform": str},
    7: {"key": "operation", "transform": str},
    8: {"key": "logistic_type", "transform": str},
    9: {"key": "issue_date", "transform": br_datetime_to_iso_utc},
    10: {"key": "amount", "transform": brl_to_float},
    11: {"key": "total_amount", "transform": brl_to_float},
    12: {"key": "freight", "transform": brl_to_float},
    13: {"key": "notes", "transform": str},
    14: {"key": "reference_invoice_date", "transform": str},
    15: {"key": "reference_danfe_key", "transform": str},
}
