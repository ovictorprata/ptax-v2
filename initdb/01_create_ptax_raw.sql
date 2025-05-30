CREATE TABLE IF NOT EXISTS ptax_raw (
  data          date      NOT NULL,
  compra        numeric   NOT NULL,
  venda         numeric   NOT NULL,
  atualizado_em timestamp NOT NULL,
  PRIMARY KEY (data, atualizado_em)
);
