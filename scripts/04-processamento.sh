#!/usr/bin/env bash
# Camada 4 — Processamento: compute pool Flink e statements de sql/ em ordem.
source "$(dirname "$0")/lib.sh"
need confluent jq

[ -n "${CONNECTOR_ID:-}" ] || die "rode scripts/03-ingestao.sh antes"

log "Compute pool $CC_POOL_NAME (teto de ${CC_POOL_MAX_CFU} CFU = teto de custo)"
if [ -z "${POOL_ID:-}" ]; then
  save POOL_ID "$(confluent flink compute-pool create "$CC_POOL_NAME" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" --max-cfu "$CC_POOL_MAX_CFU" \
      --environment "$ENV_ID" -o json | jq -r '.id')"
fi
until [ "$(confluent flink compute-pool describe "$POOL_ID" --environment "$ENV_ID" -o json \
           | jq -r '.status // .phase')" = "PROVISIONED" ]; do
  echo "    aguardando compute pool..."; sleep 10
done
ok "POOL_ID=$POOL_ID"

# Propriedades de sessão de todos os statements:
#  * catálogo = environment, database = cluster (nomes como aparecem no Flink)
#  * idle-timeout 5s: partição parada não segura o watermark (contas mudam pouco)
PROPS="sql.current-catalog=$CC_ENV_NAME,sql.current-database=$CC_CLUSTER_NAME,sql.tables.scan.idle-timeout=5s"

for f in "$ROOT"/sql/*.sql; do
  base="$(basename "$f" .sql)"
  name="desafio-$base"
  PREV="$(confluent flink statement describe "$name" $(flink_flags) -o json 2>/dev/null | jq -r '.status // empty' || true)"
  case "$PREV" in
    RUNNING|COMPLETED) ok "$name já existe ($PREV) — pulando"; continue ;;
    "") ;;
    *) warn "$name estava $PREV — recriando"
       confluent flink statement delete "$name" --force $(flink_flags) >/dev/null; sleep 5 ;;
  esac
  log "statement $name"
  SQL="$(grep -v '^[[:space:]]*--' "$f")"
  confluent flink statement create "$name" \
    --sql "$SQL" \
    --compute-pool "$POOL_ID" \
    --service-account "$SA_FLINK" \
    --database "$CC_CLUSTER_NAME" \
    --environment "$ENV_ID" \
    --property "$PROPS" \
    --wait
  ST="$(confluent flink statement describe "$name" $(flink_flags) -o json | jq -r '.status')"
  case "$ST" in
    RUNNING|COMPLETED) ok "$name: $ST" ;;
    *) die "$name terminou como $ST — veja: confluent flink statement describe $name $(flink_flags)" ;;
  esac
done

confluent flink statement list --compute-pool "$POOL_ID" $(flink_flags) | tee "$EVID/04-statements.txt"
ok "pipeline de processamento no ar. Próximo: ./scripts/disparar-fraude.sh"
