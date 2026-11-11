#!/usr/bin/env bash
# Camada 3 — Ingestão: modelo + seed no Neon, cdc_user/publication e conector CDC.
source "$(dirname "$0")/lib.sh"
need confluent jq psql

[ -n "${SA_PRODUCER:-}" ] || die "rode scripts/01-fundacao.sh antes"

# ---------------------------------------------------------------- banco
log "Neon: replicação lógica habilitada?"
WAL="$(psql_owner -tAc "SHOW wal_level")"
[ "$WAL" = "logical" ] || die "wal_level=$WAL. No console do Neon: Settings > Logical Replication > Enable."
ok "wal_level=logical"

log "Neon: tabelas + REPLICA IDENTITY FULL"
psql_owner -q -f "$ROOT/db/01-schema.sql"

if [ "$(psql_owner -tAc 'SELECT count(*) FROM transactions')" = "0" ]; then
  log "Neon: carregando seed determinístico (seed=42)"
  psql_owner -q -f "$ROOT/db/02-seed.sql" >/dev/null
else
  warn "seed já carregado — mantendo os dados (rode db/02-seed.sql à mão para recarregar)"
fi
{ echo "\$ psql -f db/04-contagem.sql   # seed=42"; psql_owner -f "$ROOT/db/04-contagem.sql"; } \
  | tee "$EVID/03-seed-contagem.txt"

log "Neon: cdc_user (LOGIN REPLICATION + SELECT) e publication desafio_pub"
psql_owner -q -v cdc_password="$CDC_USER_PASSWORD" -f "$ROOT/db/03-cdc-user.sql" >/dev/null
ok "cdc_user e desafio_pub prontos"

# ---------------------------------------------------------------- conector
log "Conector PostgresCdcSourceV2"
RENDERED="$STATE_DIR/cdc.rendered.json"      # contém senha: fica em .state/ (gitignored)
( umask 077
  jq 'walk(if type=="string" then gsub("\\$\\{(?<v>[A-Z_]+)\\}"; env[.v] // error("variável \(.v) vazia")) else . end)' \
     "$ROOT/connectors/cdc.json" > "$RENDERED" )

if [ -z "${CONNECTOR_ID:-}" ]; then
  save CONNECTOR_ID "$(confluent connect cluster create --config-file "$RENDERED" \
      --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json | jq -r '.id')"
fi
rm -f "$RENDERED"

for _ in $(seq 1 60); do
  ST="$(confluent connect cluster describe "$CONNECTOR_ID" --cluster "$CLUSTER_ID" -o json \
        | jq -r '.connector.status // .status // empty')"
  [ "$ST" = "RUNNING" ] && break
  [ "$ST" = "FAILED" ] && die "conector FAILED: confluent connect cluster describe $CONNECTOR_ID"
  echo "    status do conector: ${ST:-PROVISIONING}..."; sleep 15
done
[ "$ST" = "RUNNING" ] || die "conector não ficou RUNNING a tempo"
ok "conector $CONNECTOR_ID RUNNING"

log "Aguardando snapshot criar os tópicos desafio.public.*"
until confluent kafka topic list --cluster "$CLUSTER_ID" -o json \
      | jq -e '[.[].name] | index("desafio.public.transactions")' >/dev/null; do
  sleep 10
done
confluent kafka topic list --cluster "$CLUSTER_ID" | tee "$EVID/03-topicos.txt"

# ---------------------------------------------------------------- evidência c/u/d
log "Gerando INSERT -> UPDATE -> DELETE e capturando os envelopes (lido como desafio-consumer)"
sleep 60   # chaves recém-criadas e fim do snapshot
RAW="$EVID/03-envelopes-cdc.raw.txt"
( consume_as_reader desafio.public.transactions --group desafio-evidencias-cdc > "$RAW" 2>&1 ) &
PID=$!
sleep 20
psql_owner -q -f "$ROOT/db/05-demo-cud.sql" | tee "$EVID/03-demo-cud.txt"
sleep 40
pkill -P "$PID" 2>/dev/null || true; kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true

# Organiza c/u/d + tombstone num JSON legível (linhas "chave | valor")
python3 - "$RAW" "$EVID/01-envelopes-cdc.json" <<'PY' || warn "não consegui formatar; veja $RAW"
import json, sys
raw, dst = sys.argv[1], sys.argv[2]
events = []
for line in open(raw, encoding="utf-8", errors="replace"):
    if " | " not in line:
        continue
    k, v = line.rstrip("\n").split(" | ", 1)
    try: key = json.loads(k)
    except Exception: key = k
    if v.strip() in ("", "null"):
        events.append({"key": key, "value": None, "nota": "tombstone (compactação remove a chave)"}); continue
    try: events.append({"key": key, "value": json.loads(v)})
    except Exception: events.append({"key": key, "value_raw": v})
json.dump(events, open(dst, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print(f"    {len(events)} eventos -> {dst}")
PY
ok "evidências: evidencias/01-envelopes-cdc.json, 03-*.txt"
