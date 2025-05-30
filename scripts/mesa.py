import pandas as pd
from sqlalchemy import create_engine, text

def update_ptax_mesa_table(conn_str: str):
    engine = create_engine(conn_str)

    with engine.begin() as conn:
        # 1. Extrai o mais recente por data
        query = """
        WITH latest AS (
            SELECT DISTINCT ON (data)
                data, compra, venda, atualizado_em
            FROM ptax_raw
            ORDER BY data, atualizado_em DESC
        )
        INSERT INTO ptax_mesa (data, compra, venda, atualizado_em)
        SELECT * FROM latest
        ON CONFLICT (data) DO UPDATE
        SET compra = EXCLUDED.compra,
            venda = EXCLUDED.venda,
            atualizado_em = EXCLUDED.atualizado_em;
        """
        conn.execute(text(query))
