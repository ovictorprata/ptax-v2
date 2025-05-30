CREATE TABLE IF NOT EXISTS ptax_mesa (
  data          date PRIMARY KEY,
  compra        numeric NOT NULL,
  venda         numeric NOT NULL,
  atualizado_em timestamp NOT NULL
);

CREATE TABLE IF NOT EXISTS ptax_receita (
  data          date PRIMARY KEY,
  compra        numeric NOT NULL,
  venda         numeric NOT NULL,
  atualizado_em timestamp NOT NULL,
  congelado_em  date NOT NULL -- data do snapshot (1º dia útil do mês seguinte)
);
