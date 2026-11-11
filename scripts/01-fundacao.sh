#!/usr/bin/env bash
# Camada 1 — Fundação: environment, cluster Basic, Schema Registry e identidades.
# Idempotente: o que já existe no .state.env é reaproveitado.
source "$(dirname "$0")/lib.sh"
need confluent jq

confluent environment list >/dev/null 2>&1 || die "faça login antes: confluent login --save"

j_id()  { jq -r '.id // .resource_id'; }
j_key() { jq -r '.api_key // .key'; }
j_sec() { jq -r '.api_secret // .secret'; }

# ---------------------------------------------------------------- environment
log "Environment $CC_ENV_NAME"
if [ -z "${ENV_ID:-}" ]; then
  save ENV_ID "$(confluent environment create "$CC_ENV_NAME" --governance-package essentials -o json | j_id)"
fi
confluent environment use "$ENV_ID" >/dev/null
ok "ENV_ID=$ENV_ID"

# ---------------------------------------------------------------- cluster
log "Cluster Basic $CC_CLUSTER_NAME ($CC_CLOUD/$CC_REGION)"
if [ -z "${CLUSTER_ID:-}" ]; then
  save CLUSTER_ID "$(confluent kafka cluster create "$CC_CLUSTER_NAME" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" --type basic -o json | j_id)"
fi
until [ "$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.status')" = "UP" ]; do
  echo "    aguardando cluster ficar UP..."; sleep 10
done
confluent kafka cluster use "$CLUSTER_ID" >/dev/null
save BOOTSTRAP "$(confluent kafka cluster describe "$CLUSTER_ID" -o json \
  | jq -r '(.endpoint // .bootstrap_endpoint) | sub("^SASL_SSL://"; "")')"
ok "CLUSTER_ID=$CLUSTER_ID  BOOTSTRAP=$BOOTSTRAP"

# ---------------------------------------------------------------- schema registry
log "Schema Registry do environment"
SRJ="$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json)"
save SR_ID  "$(echo "$SRJ" | jq -r '.cluster_id // .cluster // .id')"
save SR_URL "$(echo "$SRJ" | jq -r '.endpoint_url // .endpoint // .private_endpoint_url')"
ok "SR_ID=$SR_ID  SR_URL=$SR_URL"

# ---------------------------------------------------------------- service accounts
create_sa() { # VAR nome descrição
  local var="$1" name="$2" desc="$3"
  if [ -z "${!var:-}" ]; then
    local id
    id="$(confluent iam service-account list -o json | jq -r --arg n "$name" '.[] | select(.name==$n) | .id' | head -1)"
    [ -n "$id" ] || id="$(confluent iam service-account create "$name" --description "$desc" -o json | j_id)"
    save "$var" "$id"
  fi
  ok "$name = ${!var}"
}
log "Identidades (service accounts)"
create_sa SA_PRODUCER desafio-producer "Escreve em desafio*: usado pelo conector CDC"
create_sa SA_CONSUMER desafio-consumer "Le de desafio* e usa grupos desafio*: consumidor de alertas"
create_sa SA_FLINK    desafio-flink    "Principal dos statements Flink SQL do desafio"
create_sa SA_OPS      desafio-ops      "Somente leitura de metricas (Metrics API)"

# ---------------------------------------------------------------- ACLs Kafka
acl() { confluent kafka acl create --allow --cluster "$CLUSTER_ID" "$@" >/dev/null 2>&1 || warn "ACL já existia ou falhou: $*"; }
log "ACLs do produtor (conector CDC): WRITE/CREATE em desafio* e no tópico de heartbeat"
acl --service-account "$SA_PRODUCER" --cluster-scope --operations describe
acl --service-account "$SA_PRODUCER" --topic desafio --prefix --operations create,write,describe
acl --service-account "$SA_PRODUCER" --topic __debezium-heartbeat --prefix --operations create,write,describe
log "ACLs do consumidor: READ em desafio* + grupo desafio*"
acl --service-account "$SA_CONSUMER" --topic desafio --prefix --operations read,describe
acl --service-account "$SA_CONSUMER" --consumer-group desafio --prefix --operations read

# ---------------------------------------------------------------- RBAC (Schema Registry, Flink, métricas)
rb() { confluent iam rbac role-binding create "$@" >/dev/null 2>&1 || warn "role binding já existia ou falhou: $*"; }
log "Role bindings"
rb --principal "User:$SA_PRODUCER" --role DeveloperWrite --environment "$ENV_ID" \
   --schema-registry-cluster "$SR_ID" --resource Subject:desafio --prefix
rb --principal "User:$SA_PRODUCER" --role DeveloperRead --environment "$ENV_ID" \
   --schema-registry-cluster "$SR_ID" --resource Subject:desafio --prefix
rb --principal "User:$SA_CONSUMER" --role DeveloperRead --environment "$ENV_ID" \
   --schema-registry-cluster "$SR_ID" --resource Subject:desafio --prefix
# Flink: escopo = só este environment, que é descartável e destruído no teardown.
rb --principal "User:$SA_FLINK" --role EnvironmentAdmin --environment "$ENV_ID"
rb --principal "User:$SA_OPS" --role MetricsViewer
# Permite que a SUA conta crie statements "em nome" do SA do Flink.
ME="$(confluent iam user describe -o json 2>/dev/null | jq -r '.id // empty' || true)"
[ -n "$ME" ] && rb --principal "User:$ME" --role Assigner --resource "service-account:$SA_FLINK"
ok "feito"

# ---------------------------------------------------------------- API keys
log "API keys"
if [ -z "${CONSUMER_KAFKA_KEY:-}" ]; then
  K="$(confluent api-key create --service-account "$SA_CONSUMER" --resource "$CLUSTER_ID" -o json)"
  save CONSUMER_KAFKA_KEY "$(echo "$K" | j_key)"; save CONSUMER_KAFKA_SECRET "$(echo "$K" | j_sec)"
fi
if [ -z "${CONSUMER_SR_KEY:-}" ]; then
  K="$(confluent api-key create --service-account "$SA_CONSUMER" --resource "$SR_ID" -o json)"
  save CONSUMER_SR_KEY "$(echo "$K" | j_key)"; save CONSUMER_SR_SECRET "$(echo "$K" | j_sec)"
fi
if [ -z "${SETUP_SR_KEY:-}" ]; then   # chave da conta pessoal: só registra contratos no setup
  K="$(confluent api-key create --resource "$SR_ID" --description "setup do desafio" -o json)"
  save SETUP_SR_KEY "$(echo "$K" | j_key)"; save SETUP_SR_SECRET "$(echo "$K" | j_sec)"
fi
if [ -z "${OPS_CLOUD_KEY:-}" ]; then
  K="$(confluent api-key create --service-account "$SA_OPS" --resource cloud -o json)"
  save OPS_CLOUD_KEY "$(echo "$K" | j_key)"; save OPS_CLOUD_SECRET "$(echo "$K" | j_sec)"
fi
ok "chaves salvas em .state.env (gitignored)"
echo "    (chaves novas levam ~1 min para propagar)"

# ---------------------------------------------------------------- evidência
{
  echo "\$ confluent iam service-account list"
  confluent iam service-account list | grep -E 'ID|----|desafio-'
  for sa in "$SA_PRODUCER" "$SA_CONSUMER"; do
    echo; echo "\$ confluent kafka acl list --service-account $sa"
    confluent kafka acl list --cluster "$CLUSTER_ID" --service-account "$sa"
  done
} > "$EVID/01-fundacao-identidades.txt"
ok "evidência: evidencias/01-fundacao-identidades.txt"
