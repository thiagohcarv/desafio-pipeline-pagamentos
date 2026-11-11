#!/usr/bin/env bash
# Sobe o pipeline inteiro do zero. Pré-requisitos no README (seção "Como subir").
#   ./setup.sh
set -euo pipefail
cd "$(dirname "$0")"

./scripts/01-fundacao.sh       # environment, cluster, SR, service accounts, ACLs, chaves
./scripts/02-contrato.sh       # tópicos de saída, tags, schemas BACKWARD, teste de evolução
./scripts/03-ingestao.sh       # Neon: modelo + seed + cdc_user; conector CDC; envelopes c/u/d
./scripts/04-processamento.sh  # compute pool + statements Flink SQL (join temporal e fraude)

cat <<'EOF'

Pipeline no ar. Próximos passos:
  ./scripts/disparar-fraude.sh     # 3 transações no mesmo cartão em <60 s -> alerta
  ./scripts/consumir-alertas.sh    # consumidor idempotente (outro terminal)
  ./scripts/05-operacao.sh         # métricas, acessos, custo e estado (após ~10 min)
  ./teardown.sh                    # derruba tudo e prova que não sobrou nada
EOF
