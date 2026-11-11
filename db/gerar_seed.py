#!/usr/bin/env python3
"""Gera db/02-seed.sql com dados sintéticos determinísticos (seed=42).

Só biblioteca padrão. O arquivo gerado já vem versionado; rode este script
apenas se quiser mudar volumes ou regenerar:

    python3 db/gerar_seed.py > db/02-seed.sql

Volumes: 200 customers | 240 accounts | 320 cards | 60 merchants | 2000 transactions

Os horários são relativos ao momento da carga (now() - N segundos), com N
determinístico. Assim o mesmo seed produz sempre os mesmos dados, mas
"recentes" em qualquer dia em que o pipeline for subido.

Garantias para a demo:
  * as transações históricas ficam entre 2 min e 30 dias atrás, inseridas em
    ordem cronológica (o snapshot do CDC chega em ordem de event time);
  * nenhum cartão do seed tem 3 transações em 60 s: o único alerta de fraude
    da demo é o que você dispara com scripts/disparar-fraude.sh.
"""
import random

SEED = 42
N_CUSTOMERS, N_ACCOUNTS, N_CARDS, N_MERCHANTS, N_TX = 200, 240, 320, 60, 2000

FIRST = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela", "Heitor",
         "Isabela", "João", "Karina", "Lucas", "Mariana", "Nicolas", "Olívia",
         "Paulo", "Rafaela", "Samuel", "Tatiane", "Vinícius"]
LAST = ["Silva", "Souza", "Oliveira", "Santos", "Lima", "Pereira", "Costa",
        "Almeida", "Ferreira", "Rodrigues", "Gomes", "Martins", "Araújo", "Ribeiro"]
CITIES = ["São Paulo", "Guarulhos", "Campinas", "Rio de Janeiro", "Belo Horizonte",
          "Curitiba", "Porto Alegre", "Recife", "Salvador", "Fortaleza"]
MERCHANT_KINDS = [("Supermercado", "5411"), ("Posto", "5541"), ("Farmácia", "5912"),
                  ("Restaurante", "5812"), ("Loja de Eletrônicos", "5732"),
                  ("Livraria", "5942"), ("Padaria", "5462"), ("App de Transporte", "4121")]
BRANDS = ["VISA", "MASTERCARD", "ELO"]

DAY = 86_400


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def ago(seconds: int) -> str:
    return f"(now() AT TIME ZONE 'utc') - interval '{seconds} seconds'"


def main() -> None:
    rnd = random.Random(SEED)
    out = ["-- GERADO por db/gerar_seed.py (seed=42). Não edite à mão.",
           "BEGIN;",
           "TRUNCATE transactions, cards, accounts, merchants, customers RESTART IDENTITY CASCADE;"]

    # customers
    rows, customer_names = [], {}
    for cid in range(1, N_CUSTOMERS + 1):
        name = f"{rnd.choice(FIRST)} {rnd.choice(LAST)}"
        customer_names[cid] = name
        login = name.lower().replace(" ", ".").encode("ascii", "ignore").decode()
        doc = "".join(str(rnd.randint(0, 9)) for _ in range(11))
        rows.append(f"({cid}, {q(name)}, {q(f'{login}.{cid}@exemplo.com.br')}, "
                    f"{q(doc)}, {q(rnd.choice(CITIES))}, {ago(90 * DAY + rnd.randint(0, 30 * DAY))})")
    out.append("INSERT INTO customers (customer_id, full_name, email, document, city, created_at) VALUES\n  "
               + ",\n  ".join(rows) + ";")

    # accounts: 1 por cliente + 40 extras
    rows, account_owner = [], {}
    for aid in range(1, N_ACCOUNTS + 1):
        cid = aid if aid <= N_CUSTOMERS else rnd.randint(1, N_CUSTOMERS)
        account_owner[aid] = cid
        created = 60 * DAY + rnd.randint(0, 20 * DAY)
        limit = rnd.choice([500, 1000, 2500, 5000, 10000, 20000])
        rows.append(f"({aid}, {cid}, {q(customer_names[cid])}, "
                    f"{q(rnd.choice(['CORRENTE', 'CORRENTE', 'POUPANCA', 'PAGAMENTO']))}, "
                    f"'ATIVA', {limit}.00, {ago(created)}, {ago(created)})")
    out.append("INSERT INTO accounts (account_id, customer_id, holder_name, account_type, status, "
               "credit_limit, created_at, updated_at) VALUES\n  " + ",\n  ".join(rows) + ";")

    # cards: 1 por conta + 80 extras
    rows, card_account, card_last4 = [], {}, {}
    for kid in range(1, N_CARDS + 1):
        aid = kid if kid <= N_ACCOUNTS else rnd.randint(1, N_ACCOUNTS)
        card_account[kid] = aid
        card_last4[kid] = f"{rnd.randint(0, 9999):04d}"
        rows.append(f"({kid}, {aid}, {q(card_last4[kid])}, {q(rnd.choice(BRANDS))}, 'ATIVO', "
                    f"{ago(45 * DAY + rnd.randint(0, 10 * DAY))})")
    out.append("INSERT INTO cards (card_id, account_id, card_last4, brand, status, created_at) VALUES\n  "
               + ",\n  ".join(rows) + ";")

    # merchants
    rows = []
    for mid in range(1, N_MERCHANTS + 1):
        kind, mcc = rnd.choice(MERCHANT_KINDS)
        city = rnd.choice(CITIES)
        rows.append(f"({mid}, {q(f'{kind} {mid:02d} {city}')}, {q(mcc)}, {q(city)}, {ago(100 * DAY)})")
    out.append("INSERT INTO merchants (merchant_id, name, mcc, city, created_at) VALUES\n  "
               + ",\n  ".join(rows) + ";")

    # transactions — offsets em segundos atrás; garante <3 por cartão em 60 s
    by_card: dict[int, list[int]] = {}
    txs = []
    while len(txs) < N_TX:
        kid = rnd.randint(1, N_CARDS)
        offset = rnd.randint(120, 30 * DAY)
        window = [t for t in by_card.get(kid, []) if abs(t - offset) < 60]
        if len(window) >= 1:          # já há outra no mesmo minuto: descarta (folga extra)
            continue
        by_card.setdefault(kid, []).append(offset)
        amount = round(rnd.lognormvariate(4.2, 0.9), 2)
        amount = min(max(amount, 1.00), 9999.99)
        txs.append((offset, kid, rnd.randint(1, N_MERCHANTS), amount))

    txs.sort(key=lambda t: -t[0])     # mais antiga primeiro
    rows = []
    for tid, (offset, kid, mid, amount) in enumerate(txs, start=1):
        rows.append(f"({tid}, {kid}, {card_account[kid]}, {mid}, {q(card_last4[kid])}, "
                    f"{amount:.2f}, 'BRL', 'APROVADA', {ago(offset)})")
    out.append("INSERT INTO transactions (transaction_id, card_id, account_id, merchant_id, card_last4, "
               "amount, currency, status, created_at) VALUES\n  " + ",\n  ".join(rows) + ";")

    # sequences continuam após os ids explícitos
    for table, col in [("customers", "customer_id"), ("accounts", "account_id"), ("cards", "card_id"),
                       ("merchants", "merchant_id"), ("transactions", "transaction_id")]:
        out.append(f"SELECT setval(pg_get_serial_sequence('{table}', '{col}'), (SELECT max({col}) FROM {table}));")

    out.append("COMMIT;")
    print("\n".join(out))


if __name__ == "__main__":
    main()
