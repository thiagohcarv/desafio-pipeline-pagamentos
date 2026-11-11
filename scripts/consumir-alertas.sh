#!/usr/bin/env bash
# Sobe o consumidor idempotente de alertas com as credenciais do desafio-consumer.
#   ./scripts/consumir-alertas.sh            -> grupo desafio-alertas
#   ALERT_GROUP=desafio-replay ./scripts/consumir-alertas.sh  -> relê tudo: prova a idempotência
source "$(dirname "$0")/lib.sh"
need python3
export BOOTSTRAP SR_URL CONSUMER_KAFKA_KEY CONSUMER_KAFKA_SECRET CONSUMER_SR_KEY CONSUMER_SR_SECRET
exec python3 "$ROOT/consumer/alert_consumer.py"
