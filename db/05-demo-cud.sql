-- Camada 3 — gera os três tipos de evento CDC numa transação de demonstração.
-- op=c (INSERT) -> op=u (UPDATE) -> op=d (DELETE) + tombstone
\set ON_ERROR_STOP 1
INSERT INTO transactions (card_id, account_id, merchant_id, card_last4, amount, status)
SELECT c.card_id, c.account_id, 1, c.card_last4, 42.00, 'PENDENTE'
FROM cards c WHERE c.card_id = 7
RETURNING transaction_id AS demo_tx \gset

SELECT pg_sleep(2) \gset
UPDATE transactions SET status = 'APROVADA', amount = 43.50 WHERE transaction_id = :demo_tx;

SELECT pg_sleep(2) \gset
DELETE FROM transactions WHERE transaction_id = :demo_tx;

-- UPDATE de conta: gera nova versão para o temporal join (updated_at via trigger)
UPDATE accounts SET credit_limit = credit_limit + 500 WHERE account_id = 7;

SELECT :demo_tx AS transacao_demo_insert_update_delete;
