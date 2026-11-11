-- RULE_VELOCITY: 3 ou mais transações no MESMO cartão em até 60 s -> 1 alerta.
-- T{3,}? é relutante: fecha o match assim que a 3ª transação chega
-- (Flink não aceita quantificador guloso na última variável do padrão).
-- AFTER MATCH SKIP PAST LAST ROW: a mesma transação não gera dois alertas.
INSERT INTO `desafio.fraud.detected` (
  alert_id, rule_name, card_id, card_last4, tx_count, total_amount,
  first_transaction_id, last_transaction_id, window_start, window_end, detected_at
)
SELECT
  CONCAT('RULE_VELOCITY-', CAST(card_id AS STRING), '-', CAST(first_tx AS STRING)),
  'RULE_VELOCITY',
  card_id,
  card_last4,
  tx_count,
  CAST(total_amount AS DECIMAL(15, 2)),
  first_tx,
  last_tx,
  window_start,
  window_end,
  CURRENT_TIMESTAMP
FROM `desafio.transactions.stream`
MATCH_RECOGNIZE (
  PARTITION BY card_id
  ORDER BY created_at
  MEASURES
    FIRST(T.transaction_id)        AS first_tx,
    LAST(T.transaction_id)         AS last_tx,
    LAST(T.card_last4)             AS card_last4,
    COUNT(T.transaction_id)        AS tx_count,
    SUM(T.amount)                  AS total_amount,
    FIRST(T.created_at)            AS window_start,
    LAST(T.created_at)             AS window_end
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
  DEFINE
    T AS TRUE
);
