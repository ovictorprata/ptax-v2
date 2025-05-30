from __future__ import annotations
import requests
import pandas as pd
from datetime import datetime

BASE_DAILY_URL_BCB_PTAX = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarDia(dataCotacao=@dataCotacao)"
BASE_RANGE_URL_BCB_PTAX = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?@dataInicial='MM-DD-YYYY'&@dataFinalCotacao='MM-DD-YYYY'&$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"

def format_date_for_api(date_obj: datetime) -> str:
    return date_obj.strftime('%m-%d-%Y')

def fetch_dollar_rate_for_period(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    try:
        api_start_date = format_date_for_api(start_date)
        api_end_date = format_date_for_api(end_date)
    except ValueError:
        print('Invalid date format. The correct format is YYYY-MM-DD.')
        return pd.DataFrame()
    
    try:
        url = (
            f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
            f"@dataInicial='{api_start_date}'&@dataFinalCotacao='{api_end_date}'&"
            f"$format=json&$select=cotacaoCompra,cotacaoVenda,dataHoraCotacao"
        )
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        quotation_data = response.json()
        rates_list = quotation_data.get('value', [])
        return pd.DataFrame(rates_list)
    except Exception as e:
        print(f'An error occurred: {e}')
        return pd.DataFrame()


print(fetch_dollar_rate_for_period(datetime(2025,5,21), datetime(2025,5,29)))