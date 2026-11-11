-- =====================================================================
-- Camada 3 — Modelo transacional de origem (Neon / Postgres 15+)
-- Executado com o usuário DONO do banco (NEON_OWNER_USER).
-- Idempotente: pode rodar de novo sem quebrar.
-- =====================================================================

-- Todas as datas em UTC, timestamp(3) sem fuso: com time.precision.mode=connect
-- o Debezium emite Avro timestamp-millis, que o Flink lê como TIMESTAMP_LTZ(3).

CREATE TABLE IF NOT EXISTS customers (
    customer_id  serial PRIMARY KEY,
    full_name    text         NOT NULL,
    email        text         NOT NULL,
    document     text         NOT NULL,          -- CPF sintético (PII)
    city         text         NOT NULL,
    created_at   timestamp(3) NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id   serial PRIMARY KEY,
    customer_id  int           NOT NULL REFERENCES customers (customer_id),
    holder_name  text          NOT NULL,         -- PII
    account_type text          NOT NULL CHECK (account_type IN ('CORRENTE', 'POUPANCA', 'PAGAMENTO')),
    status       text          NOT NULL DEFAULT 'ATIVA' CHECK (status IN ('ATIVA', 'BLOQUEADA', 'ENCERRADA')),
    credit_limit numeric(15,2) NOT NULL,
    created_at   timestamp(3)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    updated_at   timestamp(3)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc')  -- versão (temporal join)
);

CREATE TABLE IF NOT EXISTS cards (
    card_id      serial PRIMARY KEY,
    account_id   int          NOT NULL REFERENCES accounts (account_id),
    card_last4   text         NOT NULL CHECK (card_last4 ~ '^[0-9]{4}$'),  -- PCI (PAN mascarado)
    brand        text         NOT NULL,
    status       text         NOT NULL DEFAULT 'ATIVO',
    created_at   timestamp(3) NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id  serial PRIMARY KEY,
    name         text         NOT NULL,
    mcc          text         NOT NULL,
    city         text         NOT NULL,
    created_at   timestamp(3) NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id bigserial PRIMARY KEY,
    card_id        int           NOT NULL REFERENCES cards (card_id),
    account_id     int           NOT NULL REFERENCES accounts (account_id),
    merchant_id    int           NOT NULL REFERENCES merchants (merchant_id),
    card_last4     text          NOT NULL,       -- PCI
    amount         numeric(15,2) NOT NULL CHECK (amount > 0),
    currency       text          NOT NULL DEFAULT 'BRL',
    status         text          NOT NULL DEFAULT 'APROVADA',
    created_at     timestamp(3)  NOT NULL DEFAULT (now() AT TIME ZONE 'utc')  -- event time
);

CREATE INDEX IF NOT EXISTS ix_transactions_card_time ON transactions (card_id, created_at);

-- updated_at sempre avança em UPDATE de conta: é a coluna de versão do temporal join.
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now() AT TIME ZONE 'utc';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_accounts_touch ON accounts;
CREATE TRIGGER trg_accounts_touch BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- REPLICA IDENTITY FULL: o WAL passa a carregar a linha antiga inteira,
-- então o evento op=u vem com "before" completo (e op=d também).
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE cards        REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
