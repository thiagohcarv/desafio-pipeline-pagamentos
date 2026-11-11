-- Enriquecimento: transação ⋈ versão da conta VIGENTE no instante da transação.
-- FOR SYSTEM_TIME AS OF t.created_at = event-time temporal join
-- (não usa o "agora": reprocessar dá o mesmo resultado).
INSERT INTO `desafio.transactions.enriched` (
  transaction_id, card_id, account_id, merchant_id, card_last4, amount, currency, tx_status,
  customer_id, holder_name, account_type, account_status, credit_limit, account_version_at, created_at
)
SELECT
  t.transaction_id,
  t.card_id,
  t.account_id,
  t.merchant_id,
  t.card_last4,
  t.amount,
  t.currency,
  t.status,
  a.customer_id,
  a.holder_name,
  a.account_type,
  a.status,
  a.credit_limit,
  a.updated_at,
  t.created_at
FROM `desafio.transactions.stream` AS t
JOIN `desafio.accounts.versioned` FOR SYSTEM_TIME AS OF t.created_at AS a
  ON t.account_id = a.account_id;
