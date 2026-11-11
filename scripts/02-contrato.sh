#!/usr/bin/env bash
# Camada 2 — Contrato de dados: tópicos de saída, tags PII/PCI, schemas Avro
# registrados com compatibilidade BACKWARD e teste de evolução nos dois sentidos.
source "$(dirname "$0")/lib.sh"
need confluent jq curl

[ -n "${SETUP_SR_KEY:-}" ] || die "rode scripts/01-fundacao.sh antes"
OUT="$EVID/02-contrato-evolucao.txt"; : > "$OUT"

register() { # subject arquivo.avsc
  jq -n --rawfile s "$2" '{schemaType: "AVRO", schema: $s}' \
    | sr -X POST --data @- "$SR_URL/subjects/$1/versions"
}
set_compat() { # subject NIVEL
  sr -X PUT --data "{\"compatibility\": \"$2\"}" "$SR_URL/config/$1" >/dev/null
}
test_compat() { # subject arquivo.avsc
  jq -n --rawfile s "$2" '{schemaType: "AVRO", schema: $s}' \
    | sr -X POST --data @- "$SR_URL/compatibility/subjects/$1/versions/latest?verbose=true"
}

# ---------------------------------------------------------------- tópicos de saída
log "Tópicos de saída do Flink (1 partição: volume de demo, menor custo)"
for t in desafio.transactions.enriched desafio.fraud.detected; do
  confluent kafka topic create "$t" --partitions 1 --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
    >/dev/null 2>&1 && ok "criado $t" || ok "$t já existe"
done

# ---------------------------------------------------------------- tags
log "Definições de tag PII e PCI no catálogo (Stream Governance)"
curl -sS -f -u "$SETUP_SR_KEY:$SETUP_SR_SECRET" -H "Content-Type: application/json" \
  -X POST "$SR_URL/catalog/v1/types/tagdefs" --data '[
  {"entityTypes":["sr_field"],"name":"PII","description":"Dado pessoal identificável (LGPD)"},
  {"entityTypes":["sr_field"],"name":"PCI","description":"Dado de cartão (PCI DSS)"}]' >/dev/null \
  && ok "tags criadas" || warn "não foi possível criar as tagdefs; as tags seguem como metadado no schema"

# ---------------------------------------------------------------- registro
SUBJ_FRAUD="desafio.fraud.detected-value"
SUBJ_ENR="desafio.transactions.enriched-value"
log "Registrando contratos (TopicNameStrategy) com BACKWARD"
for pair in "$SUBJ_FRAUD:schemas/fraud_alert.avsc" "$SUBJ_ENR:schemas/transaction_enriched.avsc"; do
  subj="${pair%%:*}"; file="$ROOT/${pair#*:}"
  set_compat "$subj" BACKWARD
  echo "    $subj -> $(register "$subj" "$file")"
done

# ---------------------------------------------------------------- evolução
log "Teste de evolução contra $SUBJ_FRAUD (BACKWARD)"
{
  echo "# Subject: $SUBJ_FRAUD | compatibilidade: $(sr "$SR_URL/config/$SUBJ_FRAUD" | jq -r '.compatibilityLevel')"
  echo; echo "## Versão registrada"
  sr "$SR_URL/subjects/$SUBJ_FRAUD/versions/latest" | jq '{subject, version, id}'
  for f in v2_campo_novo_com_default v3_campo_novo_sem_default v4_remove_campo_obrigatorio; do
    echo; echo "## POST /compatibility/subjects/$SUBJ_FRAUD/versions/latest  <- fraud_alert_${f}.avsc"
    test_compat "$SUBJ_FRAUD" "$ROOT/schemas/evolucao/fraud_alert_${f}.avsc" | jq .
  done
} | tee -a "$OUT"

# Remoção de campo obrigatório é PERMITIDA em BACKWARD (o leitor novo só ignora o campo).
# Quem a rejeita é FORWARD/FULL. Demonstração num subject descartável:
log "Contraprova: a mesma remoção sob FORWARD (subject descartável)"
SUBJ_TMP="desafio.contrato-forward-demo-value"
set_compat "$SUBJ_TMP" FORWARD
register "$SUBJ_TMP" "$ROOT/schemas/fraud_alert.avsc" >/dev/null
{
  echo; echo "## Contraprova — subject $SUBJ_TMP com FORWARD <- fraud_alert_v4_remove_campo_obrigatorio.avsc"
  test_compat "$SUBJ_TMP" "$ROOT/schemas/evolucao/fraud_alert_v4_remove_campo_obrigatorio.avsc" | jq .
} | tee -a "$OUT"
sr -X DELETE "$SR_URL/subjects/$SUBJ_TMP" >/dev/null
sr -X DELETE "$SR_URL/subjects/$SUBJ_TMP?permanent=true" >/dev/null
ok "evidência: evidencias/02-contrato-evolucao.txt"
