-- Lê o tópico CDC como envelope Debezium "cru" (before/after/op) e append-only.
-- Por quê: a regra de fraude (MATCH_RECOGNIZE) só aceita entrada append-only;
-- com 'avro-debezium-registry' o Flink abriria o envelope como changelog (retract).
ALTER TABLE `desafio.public.transactions` SET (
  'value.format'   = 'avro-registry',
  'changelog.mode' = 'append'
);
