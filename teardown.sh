#!/usr/bin/env bash
# Derruba tudo o que o setup criou e grava a prova em evidencias/99-teardown.txt
#   ./teardown.sh
source "$(dirname "$0")/scripts/lib.sh"
need confluent jq psql

OUT="$EVID/99-teardown.txt"; : > "$OUT"
say() { echo "$*" | tee -a "$OUT"; }

[ -n "${ENV_ID:-}" ] || die ".state.env sem ENV_ID — nada para derrubar (ou o estado se perdeu: apague pelo console)"

# 1. Flink: statements e compute pool (cobram por CFU-minuto)
if [ -n "${POOL_ID:-}" ]; then
  log "Flink statements"
  for s in $(confluent flink statement list --compute-pool "$POOL_ID" $(flink_flags) -o json | jq -r '.[]?.name'); do
    confluent flink statement delete "$s" --force $(flink_flags) >/dev/null && ok "removido $s"
  done
fi

# 2. Conector (cobra por hora de task)
if [ -n "${CONNECTOR_ID:-}" ]; then
  log "Conector CDC"
  confluent connect cluster delete "$CONNECTOR_ID" --cluster "$CLUSTER_ID" --force >/dev/null && ok "removido $CONNECTOR_ID"
fi

# 3. Prova de que não sobrou nada cobrando (antes de apagar cluster/environment)
sleep 10
say "\$ confluent connect cluster list"
confluent connect cluster list --cluster "$CLUSTER_ID" 2>&1 | tee -a "$OUT"
if [ -n "${POOL_ID:-}" ]; then
  say ""; say "\$ confluent flink statement list"
  confluent flink statement list --compute-pool "$POOL_ID" $(flink_flags) 2>&1 | tee -a "$OUT"
  confluent flink compute-pool delete "$POOL_ID" --environment "$ENV_ID" --force >/dev/null && ok "removido $POOL_ID"
fi

# 4. Neon: slot de replicação órfão retém WAL para sempre — removê-lo é obrigatório
log "Neon: slot e publication"
psql_owner -q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'desafio_slot' AND NOT active;" >/dev/null
psql_owner -q -c "DROP PUBLICATION IF EXISTS desafio_pub;"
say ""; say "\$ psql -c 'SELECT slot_name FROM pg_replication_slots'"
psql_owner -c "SELECT slot_name, active FROM pg_replication_slots;" | tee -a "$OUT"

# 5. Chaves, cluster, environment e service accounts
log "API keys"
for sa in "${SA_PRODUCER:-}" "${SA_CONSUMER:-}" "${SA_FLINK:-}" "${SA_OPS:-}"; do
  [ -n "$sa" ] || continue
  for k in $(confluent api-key list --service-account "$sa" -o json | jq -r '.[]?.key'); do
    confluent api-key delete "$k" --force >/dev/null && ok "chave $k"
  done
done
[ -n "${SETUP_SR_KEY:-}" ] && confluent api-key delete "$SETUP_SR_KEY" --force >/dev/null 2>&1 && ok "chave de setup"

log "Cluster e environment"
confluent kafka cluster delete "$CLUSTER_ID" --environment "$ENV_ID" --force >/dev/null && ok "removido $CLUSTER_ID"
confluent environment delete "$ENV_ID" --force >/dev/null && ok "removido $ENV_ID"

log "Service accounts"
for sa in "${SA_PRODUCER:-}" "${SA_CONSUMER:-}" "${SA_FLINK:-}" "${SA_OPS:-}"; do
  [ -n "$sa" ] && confluent iam service-account delete "$sa" --force >/dev/null && ok "removido $sa"
done

say ""; say "\$ confluent environment list | grep $CC_ENV_NAME"
confluent environment list | grep "$CC_ENV_NAME" | tee -a "$OUT" || say "(nenhum environment $CC_ENV_NAME)"

rm -f "$STATE_FILE"; rm -rf "$STATE_DIR"
ok "teardown concluído — evidência em evidencias/99-teardown.txt"
echo "    O banco do Neon foi mantido (é seu). Para apagar os dados: DROP das 5 tabelas e do papel cdc_user."
