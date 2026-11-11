-- Camada 4 — dispara a RULE_VELOCITY: 3 transações no MESMO cartão em < 60 s.
-- Uso: psql ... -v card=42 -f db/06-burst-fraude.sql
\set ON_ERROR_STOP 1
\if :{?card}
\else
  \set card 42
\endif

INSERT INTO transactions (card_id, account_id, merchant_id, card_last4, amount)
SELECT card_id, account_id, 11, card_last4, 129.90 FROM cards WHERE card_id = :card;
SELECT pg_sleep(5) \gset
INSERT INTO transactions (card_id, account_id, merchant_id, card_last4, amount)
SELECT card_id, account_id, 23, card_last4, 349.00 FROM cards WHERE card_id = :card;
SELECT pg_sleep(5) \gset
INSERT INTO transactions (card_id, account_id, merchant_id, card_last4, amount)
SELECT card_id, account_id, 37, card_last4, 1899.99 FROM cards WHERE card_id = :card;

-- "Empurra" o watermark: o MATCH_RECOGNIZE em event time só fecha o match quando
-- o watermark passa da 3ª transação. Sem um evento posterior no tópico, o
-- watermark para em (última - 5 s) e o alerta ficaria preso no buffer.
SELECT pg_sleep(15) \gset
INSERT INTO transactions (card_id, account_id, merchant_id, card_last4, amount)
SELECT card_id, account_id, 5, card_last4, 9.90 FROM cards WHERE card_id = (:card % 320) + 1;

SELECT transaction_id, card_id, card_last4, amount, created_at
FROM transactions
WHERE card_id = :card AND created_at > (now() AT TIME ZONE 'utc') - interval '2 minutes'
ORDER BY created_at;
