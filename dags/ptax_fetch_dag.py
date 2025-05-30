from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import fetch_dollar_rate_for_period
from scripts.load import load_ptax_to_postgres
from scripts.mesa import update_ptax_mesa_table
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_fetch_and_load(**kwargs):
    # Busca dos últimos 10 dias
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    df = fetch_dollar_rate_for_period(start_date, end_date)

    if df.empty:
        print("Nenhum dado retornado da API.")
        return

    conn_str = "postgresql+psycopg2://airflow:airflow@postgres:5432/ptax"
    load_ptax_to_postgres(df, conn_str)

with DAG(
    dag_id='ptax_fetch_dag',
    default_args=default_args,
    schedule_interval='0 10,15,20 * * *',  # 3x por dia
    catchup=False,
    tags=['ptax'],
) as dag:

    fetch_and_load = PythonOperator(
        task_id='fetch_and_load_ptax',
        python_callable=run_fetch_and_load,
        provide_context=True,
    )

    update_mesa = PythonOperator(
        task_id='update_ptax_mesa',
        python_callable=lambda: update_ptax_mesa_table("postgresql+psycopg2://airflow:airflow@postgres:5432/ptax")
    )

    fetch_and_load >> update_mesa


