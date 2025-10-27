from datetime import datetime, timedelta
import re


def converter_data_completa_para_iso(data_str):
    """
    Converte '1 de setembro de 2025 06:35 hs.' para '2025-09-01T06:35:00Z'.
    """
    # Mapeamento de meses em português
    meses = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }

    try:
        # Remover o 'hs.' se existir
        data_str = data_str.replace(" hs.", "").strip()

        # Regex para extrair dia, mês, ano, hora e minuto
        match = re.match(r"(\d{1,2}) de (\w+) de (\d{4}) (\d{1,2}):(\d{2})", data_str)
        if not match:
            raise ValueError("Formato de data inválido")

        dia, mes_str, ano, hora, minuto = match.groups()
        dia = int(dia)
        mes = meses[mes_str.lower()]
        ano = int(ano)
        hora = int(hora)
        minuto = int(minuto)

        dt = datetime(ano, mes, dia, hora, minuto)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        raise ValueError(f"Erro ao converter data: {e}")


def converter_data_humana_para_iso(data_str):
    """
    Converte uma data no formato '5 de setembro | 08:50' para 'YYYY-MM-DDTHH:MM:SSZ',
    usando o ano atual do servidor.
    """
    # Mapeamento de meses em português para números
    meses = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }

    try:
        # Separar data e hora
        parte_data, parte_hora = data_str.split("|")
        dia, _, mes_str = parte_data.strip().split(" ")
        hora, minuto = parte_hora.strip().split(":")

        # Conversão para inteiros
        dia = int(dia)
        mes = meses[mes_str.lower()]
        hora = int(hora)
        minuto = int(minuto)

        # Pegar o ano atual do servidor
        ano = datetime.now().year

        dt = datetime(ano, mes, dia, hora, minuto)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        raise ValueError(f"Erro ao converter data: {e}")


def brl_to_float(valor_str: str) -> float:
    """
    Converte valor em formato brasileiro 'R$ 1.234,56' para float 1234.56
    """
    # Remove o 'R$', espaços e pontos de milhar
    valor_limpo = valor_str.replace("R$", "").replace(".", "").strip()
    # Substitui a vírgula decimal por ponto
    valor_float = float(valor_limpo.replace(",", "."))
    return valor_float


def br_datetime_to_iso_utc(br_datetime_str: str) -> str:
    """
    Converte data/hora no formato brasileiro 'DD/MM/YYYY HH:MM:SS'
    para ISO 8601 em UTC 'YYYY-MM-DDTHH:MM:SSZ'

    Assumindo que a hora fornecida está no fuso horário local (por exemplo, Brasil -3)
    """
    # Parse da string para datetime
    dt_local = datetime.strptime(br_datetime_str, "%d/%m/%Y %H:%M:%S")

    # Ajuste para UTC (considerando Brasil -3h)
    dt_utc = dt_local - timedelta(hours=3)

    # Formata para ISO 8601 com Z
    iso_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    return iso_utc
