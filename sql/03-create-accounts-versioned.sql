-- Lado "build" do temporal join: tabela VERSIONADA de contas.
-- Precisa de PRIMARY KEY + watermark na coluna de versão (updated_at).
CREATE TABLE `desafio.accounts.versioned` (
  account_id   INT NOT NULL,
  customer_id  INT,
  holder_name  STRING,
  account_type STRING,
  status       STRING,
  credit_limit DECIMAL(15, 2),
  updated_at   TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (account_id) NOT ENFORCED,
  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
)
DISTRIBUTED BY HASH(account_id) INTO 1 BUCKETS
WITH (
  'changelog.mode'       = 'upsert',
  'kafka.cleanup-policy' = 'compact'
);
