-- Lado "probe": fluxo append-only de transações com event time em created_at.
-- Particionado por card_id: todas as transações de um cartão no mesmo bucket.
CREATE TABLE `desafio.transactions.stream` (
  card_id        INT NOT NULL,
  transaction_id BIGINT NOT NULL,
  account_id     INT NOT NULL,
  merchant_id    INT NOT NULL,
  card_last4     STRING,
  amount         DECIMAL(15, 2),
  currency       STRING,
  status         STRING,
  created_at     TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(card_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append'
);
