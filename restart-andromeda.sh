#!/usr/bin/env sh
set -eu

# ——— НАСТРОЙ ЭТИ ПУТИ ПОД СЕБЯ ———
BIN="/home/tm_andromeda/agent"     # путь к исполняемому файлу агента
WORKDIR="/home/tm_andromeda"           # рабочая папка агента (где его конфиги/ресурсы)
ARGS=""                                 # если нужны аргументы (например: --config ./agent.yaml)
USER=""                        # если надо запускать от другого пользователя — поставь имя $(whoami)
LOG_DIR="/var/log/andromeda"
# ————————————————————————————————

mkdir -p "$LOG_DIR"

# мягко убиваем старый процесс (если не запущен — ок)
pkill -f "$BIN" || true
sleep 0.4

# стартуем в фоне; если нужен другой пользователь и запущено от root — используем runuser/su
START_CMD="cd \"$WORKDIR\" && nohup \"$BIN\" $ARGS >> \"$LOG_DIR/agent.log\" 2>&1 &"

if [ "$(id -u)" -eq 0 ] && [ "$USER" != "root" ]; then
  if command -v runuser >/dev/null 2>&1; then
    runuser -u "$USER" -- sh -c "$START_CMD"
  else
    su -s /bin/sh "$USER" -c "$START_CMD"
  fi
else
  sh -c "$START_CMD"
fi
