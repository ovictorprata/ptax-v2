from __future__ import annotations
import requests
import pandas as pd
from datetime import datetime

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
        rates_list = response.json().get('value', [])
        df = pd.DataFrame(rates_list)

        if df.empty:
            return df

        # Adiciona a data base (sem hora) derivada do timestamp
        df["data"] = pd.to_datetime(df["dataHoraCotacao"]).dt.date
        df["atualizado_em"] = pd.to_datetime(df["dataHoraCotacao"])
        df.rename(columns={
            "cotacaoCompra": "compra",
            "cotacaoVenda": "venda"
        }, inplace=True)

        return df[["data", "compra", "venda", "atualizado_em"]]

    except Exception as e:
        print(f'An error occurred: {e}')
        return pd.DataFrame()


print(fetch_dollar_rate_for_period(datetime(2025,5,21), datetime(2025,5,29)))