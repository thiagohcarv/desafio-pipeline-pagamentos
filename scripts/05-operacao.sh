#!/usr/bin/env bash
# Camada 5 — Operação: observabilidade (Metrics API), segurança (ACL/RBAC),
# custo (billing) e estado dos componentes. Gera evidencias/05-*.txt
source "$(dirname "$0")/lib.sh"
need confluent jq curl psql

HOURS="${1:-3}"
START="$(iso_ago "$HOURS")"; END="$(iso_now)"
METRICS="https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"

metric() { # json-body
  curl -sS -u "$OPS_CLOUD_KEY:$OPS_CLOUD_SECRET" -H "Content-Type: application/json" -X POST "$METRICS" --data "$1"
}

# ---------------------------------------------------------------- ① observabilidade
log "① Observabilidade — received_bytes e consumer lag ($START → $END)"
RB="$(metric "$(jq -n --arg c "$CLUSTER_ID" --arg i "$START/$END" '{
  aggregations: [{metric: "io.confluent.kafka.server/received_bytes"}],
  filter: {field: "resource.kafka.id", op: "EQ", value: $c},
  granularity: "PT1M", intervals: [$i], limit: 1000}')")"
LAG="$(metric "$(jq -n --arg c "$CLUSTER_ID" --arg i "$START/$END" '{
  aggregations: [{metric: "io.confluent.kafka.server/consumer_lag_offsets"}],
  filter: {field: "resource.kafka.id", op: "EQ", value: $c},
  group_by: ["metric.consumer_group_id"],
  granularity: "PT1M", intervals: [$i], limit: 1000}')")"
{
  echo "# Metrics API — cluster $CLUSTER_ID — janela $START / $END (granularidade 1 min)"
  echo; echo "## Pico de received_bytes (bytes/min)"
  echo "$RB" | jq '[.data[]?] | if length == 0 then "sem pontos (métricas atrasam ~5 min)" else
    (max_by(.value) | {pico_bytes_por_minuto: .value, quando: .timestamp}) end'
  echo; echo "## Total recebido na janela (bytes)"
  echo "$RB" | jq '[.data[]?.value] | add // 0'
  echo; echo "## Consumer lag por grupo (último ponto e máximo)"
  echo "$LAG" | jq '[.data[]?] | group_by(."metric.consumer_group_id")
    | map({grupo: .[0]."metric.consumer_group_id",
           lag_ultimo: (sort_by(.timestamp) | last.value),
           lag_max: (map(.value) | max)})'
} | tee "$EVID/05-observabilidade.txt"

# ---------------------------------------------------------------- ② segurança
log "② Segurança — ACLs e role bindings por service account"
{
  confluent iam service-account list | grep -E 'ID|----|desafio-'
  for sa in "$SA_PRODUCER" "$SA_CONSUMER" "$SA_FLINK" "$SA_OPS"; do
    echo; echo "### $sa — ACLs"
    confluent kafka acl list --cluster "$CLUSTER_ID" --service-account "$sa" 2>/dev/null || echo "(nenhuma)"
    echo "### $sa — role bindings"
    confluent iam rbac role-binding list --principal "User:$sa" --environment "$ENV_ID" 2>/dev/null || true
    confluent iam rbac role-binding list --principal "User:$sa" 2>/dev/null || true
  done
} | tee "$EVID/05-seguranca.txt"

# ---------------------------------------------------------------- ③ custos
log "③ Custos — billing cost list (conta pessoal; dados podem atrasar até 72 h)"
{
  echo "\$ confluent billing cost list --start-date $(day_off -7) --end-date $(day_off +1)"
  confluent billing cost list --start-date "$(day_off -7)" --end-date "$(day_off +1)" \
    || echo "(sem permissão de billing ou sem dados ainda)"
} | tee "$EVID/05-custos.txt"

# ---------------------------------------------------------------- ④ confiabilidade (estado)
log "④ Confiabilidade — estado de cada trecho"
{
  echo "\$ confluent connect cluster list"; confluent connect cluster list --cluster "$CLUSTER_ID"
  echo; echo "\$ confluent flink statement list"
  confluent flink statement list --compute-pool "$POOL_ID" $(flink_flags)
  echo; echo "# Slot de replicação no Neon (lag do WAL retido)"
  psql_owner -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_retido FROM pg_replication_slots;"
} | tee "$EVID/05-confiabilidade.txt"
ok "evidências: evidencias/05-*.txt"
