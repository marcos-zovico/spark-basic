
CREATE KEYSPACE credenciadora WITH  REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

use credenciadora;

CREATE TABLE credenciadora_table (
  idt_uuid uuid,
  referencia_externa uuid,
  data_referencia timestamp,
  cnpj_da_instituicao_credenciadora text,
  data_de_liquidacao timestamp,
  valor_constituido_total bigint,
  PRIMARY KEY (idt_uuid));