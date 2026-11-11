-- Só nascimentos de transação: snapshot (r) e INSERT (c).
-- UPDATE/DELETE de transação não são "novas compras" e não entram na regra.
INSERT INTO `desafio.transactions.stream`
SELECT
  `after`.card_id,
  `after`.transaction_id,
  `after`.account_id,
  `after`.merchant_id,
  `after`.card_last4,
  `after`.amount,
  `after`.currency,
  `after`.status,
  CAST(`after`.created_at AS TIMESTAMP_LTZ(3))
FROM `desafio.public.transactions`
WHERE `op` IN ('r', 'c') AND `after` IS NOT NULL;
