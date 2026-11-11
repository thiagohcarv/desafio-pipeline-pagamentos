-- Cada c/r/u da conta vira uma nova versão (upsert pela PK).
INSERT INTO `desafio.accounts.versioned`
SELECT
  `after`.account_id,
  `after`.customer_id,
  `after`.holder_name,
  `after`.account_type,
  `after`.status,
  `after`.credit_limit,
  CAST(`after`.updated_at AS TIMESTAMP_LTZ(3))
FROM `desafio.public.accounts`
WHERE `op` IN ('r', 'c', 'u') AND `after` IS NOT NULL;
