import pandas as pd
from sqlalchemy import create_engine

def load_ptax_to_postgres(df: pd.DataFrame, conn_str: str):
    engine = create_engine(conn_str)
    df.to_sql("ptax_raw", engine, if_exists="append", index=False, method="multi")

