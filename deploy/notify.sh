#!/usr/bin/env bash
# Envia por e-mail o resultado da última rodada do car-etl.service.
# Uso: notify.sh success|failure   (chamado pelo systemd; ver car-etl-notify@.service)
# Precisa do msmtp configurado (~/.msmtprc) e de NOTIFY_TO no .env do repositório.
set -euo pipefail

result="${1:-failure}"
here="$(cd "$(dirname "$0")/.." && pwd)"
to="$(grep -E '^NOTIFY_TO=' "$here/.env" 2>/dev/null | cut -d= -f2- | tr -d "\"' " || true)"
from="$(grep -E '^NOTIFY_FROM=' "$here/.env" 2>/dev/null | cut -d= -f2- | tr -d "\"' " || true)"
from="${from:-dev@castlab.org}"
[ -n "$to" ] || { echo "NOTIFY_TO não definido em $here/.env; nada enviado"; exit 0; }

run="$(systemctl --user show car-etl.service -p InvocationID --value)"
log="$(journalctl --user -u car-etl.service _SYSTEMD_INVOCATION_ID="$run" --no-pager -o cat 2>/dev/null || true)"
[ -n "$log" ] || log="$(journalctl --user -u car-etl.service --no-pager -o cat -n 200)"

if [ "$result" = success ]; then
  subject="CAR: atualização semanal concluída"
else
  subject="CAR: atualização semanal com FALHA"
fi

{
  echo "From: carbr-etl <$from>"
  echo "To: $to"
  echo "Subject: $subject"
  echo "Date: $(date -R)"
  echo "MIME-Version: 1.0"
  echo "Content-Type: text/plain; charset=UTF-8"
  echo
  echo "Servidor: $(hostname)   Rodada: $(date '+%d/%m/%Y %H:%M')"
  echo
  echo "$log" | grep -E "fila concluída|estados com falha|FALHOU|no catálogo;" | tail -40 || true
  echo
  echo "Últimas linhas do log:"
  echo "$log" | tail -25
  echo
  echo "Log completo: journalctl --user -u car-etl.service"
} | msmtp --read-envelope-from -t
