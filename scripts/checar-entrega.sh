#!/usr/bin/env bash
# Checklist do desafio antes de submeter. Não precisa de .env.
set -uo pipefail
cd "$(dirname "$0")/.."
fail=0
chk() { if eval "$2"; then printf '  ✔ %s\n' "$1"; else printf '  ✘ %s\n' "$1"; fail=1; fi; }

echo "Checklist de entrega"
chk "README sem marcadores PREENCHER"            '! grep -q "PREENCHER" README.md'
chk "README sem '<sua evidência aqui>'"           '! grep -qi "sua evidência aqui" README.md'
chk ".env.example presente"                       '[ -f .env.example ]'
chk ".env NÃO versionado"                         '! git ls-files --error-unmatch .env >/dev/null 2>&1'
chk ".state.env NÃO versionado"                   '! git ls-files --error-unmatch .state.env >/dev/null 2>&1'
chk "nenhum segredo com cara de chave no git"     '! git grep -nE "(api_secret|API_SECRET|PASSWORD)=[^\$[:space:]]{8,}" -- . ":!.env.example" ":!scripts/checar-entrega.sh" >/dev/null 2>&1'
chk "evidências geradas (≥ 8 arquivos)"           '[ "$(ls evidencias 2>/dev/null | wc -l)" -ge 8 ]'
chk "nome do repositório minúsculo e sem acento"  '[[ "$(basename "$(git rev-parse --show-toplevel 2>/dev/null || pwd)")" =~ ^[a-z0-9._-]+$ ]]'
exit $fail
