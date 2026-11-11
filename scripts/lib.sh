#!/usr/bin/env bash
# Funções comuns. Use com:  source "$(dirname "$0")/lib.sh"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_FILE="$ROOT/.state.env"      # IDs e chaves gerados no setup (gitignored)
STATE_DIR="$ROOT/.state"           # arquivos renderizados com segredo (gitignored)
EVID="$ROOT/evidencias"
mkdir -p "$STATE_DIR" "$EVID"

[ -f "$ROOT/.env" ] || { echo "ERRO: .env não encontrado. Rode: cp .env.example .env  e preencha."; exit 1; }
set -a
# shellcheck disable=SC1091
source "$ROOT/.env"
# shellcheck disable=SC1090
[ -f "$STATE_FILE" ] && source "$STATE_FILE"
set +a

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m    ✔ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m    ! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERRO: %s\033[0m\n' "$*"; exit 1; }

need() { for c in "$@"; do command -v "$c" >/dev/null || die "comando '$c' não encontrado"; done; }

# save NOME VALOR -> persiste no .state.env e exporta na sessão atual
save() {
  local k="$1" v="$2"
  [ -n "$v" ] && [ "$v" != "null" ] || die "valor vazio para $k (veja o erro acima)"
  touch "$STATE_FILE"; chmod 600 "$STATE_FILE"
  grep -v "^${k}=" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  printf '%s=%q\n' "$k" "$v" >> "$STATE_FILE.tmp"
  mv "$STATE_FILE.tmp" "$STATE_FILE"
  export "$k=$v"
}

# Datas portáveis (GNU date e BSD/macOS date)
iso_ago()  { date -u -d "$1 hours ago" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-"$1"H +%Y-%m-%dT%H:%M:%SZ; }
iso_now()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
day_off()  { date -u -d "$1 days" +%Y-%m-%d 2>/dev/null || date -u -v"$1"d +%Y-%m-%d; }

# Postgres (Neon) como DONO do banco
psql_owner() {
  PGPASSWORD="$NEON_OWNER_PASSWORD" PGOPTIONS="-c client_min_messages=warning" psql \
    "host=$NEON_HOST port=${NEON_PORT:-5432} dbname=$NEON_DB user=$NEON_OWNER_USER sslmode=require" \
    -v ON_ERROR_STOP=1 "$@"
}

# Schema Registry REST com a chave de SETUP (conta pessoal)
sr() {
  curl -sS -u "$SETUP_SR_KEY:$SETUP_SR_SECRET" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" "$@"
}

# Kafka/SR como o service account LEITOR (desafio-consumer)
consume_as_reader() {
  confluent kafka topic consume "$@" \
    --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
    --api-key "$CONSUMER_KAFKA_KEY" --api-secret "$CONSUMER_KAFKA_SECRET" \
    --value-format avro --print-key --delimiter " | " \
    --schema-registry-endpoint "$SR_URL" \
    --schema-registry-api-key "$CONSUMER_SR_KEY" --schema-registry-api-secret "$CONSUMER_SR_SECRET"
}

# Roda um comando em background por N segundos e guarda a saída (portável, sem `timeout`)
run_for() {
  local secs="$1" out="$2"; shift 2
  ( "$@" > "$out" 2>&1 ) & local pid=$!
  sleep "$secs"; kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
}

flink_flags() {
  echo --environment "$ENV_ID" --cloud "$CC_CLOUD" --region "$CC_REGION"
}
