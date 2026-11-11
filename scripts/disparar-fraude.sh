#!/usr/bin/env bash
# Evidência da Camada 4: 3 transações no mesmo cartão em < 60 s -> alerta em desafio.fraud.detected
source "$(dirname "$0")/lib.sh"
need confluent psql

CARD="${1:-${FRAUD_DEMO_CARD_ID:-42}}"
OUT="$EVID/04-alerta-fraude.txt"
ALERT_RAW="$STATE_DIR/fraude.consume.txt"
ENR_RAW="$STATE_DIR/enriched.consume.txt"

log "Escutando desafio.fraud.detected e desafio.transactions.enriched (como desafio-consumer)"
( consume_as_reader desafio.fraud.detected --from-beginning --group "desafio-evid-fraude-$$" > "$ALERT_RAW" 2>&1 ) &
P1=$!
( consume_as_reader desafio.transactions.enriched --group "desafio-evid-enriq-$$" > "$ENR_RAW" 2>&1 ) &
P2=$!
sleep 15

log "Disparando 3 transações no cartão $CARD (0 s, 5 s, 10 s) + 1 transação de outro cartão"
BURST="$(psql_owner -v card="$CARD" -f "$ROOT/db/06-burst-fraude.sql")"
echo "$BURST"

log "Aguardando o Flink fechar o match (até 60 s)"
for _ in $(seq 1 12); do
  grep -q "RULE_VELOCITY-$CARD-" "$ALERT_RAW" 2>/dev/null && break
  sleep 5
done
sleep 5
pkill -P "$P1" 2>/dev/null || true; pkill -P "$P2" 2>/dev/null || true; kill "$P1" "$P2" 2>/dev/null || true; wait "$P1" "$P2" 2>/dev/null || true

{
  echo "# Transações que dispararam o alerta (Postgres, mesmo card_id, < 60 s)"
  echo "$BURST"
  echo
  echo "# Alerta emitido em desafio.fraud.detected (lido pelo desafio-consumer)"
  grep "RULE_VELOCITY-$CARD-" "$ALERT_RAW" || echo "(nenhum alerta ainda — veja a seção 'Se o alerta não aparecer' no README)"
  echo
  echo "# As mesmas transações já enriquecidas com a conta (desafio.transactions.enriched)"
  grep -E "\"card_id\": ?$CARD[,} ]" "$ENR_RAW" | head -5 || true
} | tee "$OUT"
ok "evidência: evidencias/04-alerta-fraude.txt"
