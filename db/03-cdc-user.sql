-- =====================================================================
-- Camada 3 — Usuário de CDC com menor privilégio + publication
-- Executado com o usuário DONO do banco:
--   psql ... -v cdc_password="$CDC_USER_PASSWORD" -f db/03-cdc-user.sql
--
-- O cdc_user NÃO é dono de nada e NÃO escreve em nada. Ele só pode:
--   * logar e abrir conexão de replicação (LOGIN REPLICATION)
--   * ler as 5 tabelas capturadas (SELECT — necessário para o snapshot)
-- A publication é criada aqui pelo dono, e o conector roda com
-- publication.autocreate.mode=disabled: ele não precisa de CREATE no banco.
-- =====================================================================

SELECT 'CREATE ROLE cdc_user LOGIN REPLICATION'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user')
\gexec

-- No Neon, se este ALTER falhar por falta de permissão para REPLICATION,
-- crie o papel cdc_user pelo console (Roles) com Logical Replication ativada
-- e rode o restante do arquivo.
ALTER ROLE cdc_user WITH LOGIN REPLICATION PASSWORD :'cdc_password';

SELECT format('GRANT CONNECT ON DATABASE %I TO cdc_user', current_database())
\gexec

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;

SELECT 'CREATE PUBLICATION desafio_pub FOR TABLE customers, accounts, cards, merchants, transactions'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'desafio_pub')
\gexec
