#!/usr/bin/env bash
# =============================================================================
#  deploy.sh — развёртывание Solana Pump Momentum Scalping Bot на Linux VPS
#  (Ubuntu 22.04+/Debian 12; systemd; Python 3.11+)
#
#  ЗАПУСКАЕТСЯ НА СЕРВЕРЕ, в корне проекта:
#      rsync -av --exclude .venv --exclude data --exclude logs --exclude .env \
#          ./ user@vps:/opt/pumpbot/    # (из локальной машины; .env не копируем!)
#      ssh user@vps
#      cd /opt/pumpbot && ./deploy.sh
#
#  Что делает:
#    1) проверяет окружение (python >= 3.11, venv, git);
#    2) подтягивает код (git pull, если репозиторий);
#    3) проверяет/создаёт .env (секреты НЕ трогает и НЕ перезаписывает);
#    4) собирает .venv и ставит зависимости;
#    5) ставит systemd-сервис (автоперезапуск, старт при загрузке);
#    6) ждёт health-check веб-дашборда http://127.0.0.1:<PORT>/api/snapshot
#       (дашборд на aiohttp, уже в зависимостях — отдельный FastAPI не нужен);
#    7) печатает итоговые команды наблюдения (journalctl, ssh-туннель).
#
#  Флаги:  --skip-service   только окружение/код, systemd не трогать
#          --no-health      не ждать health-check
#          --auto-stash     при конфликте pull: git stash → pull → pop
#          --user NAME      сервисный пользователь для systemd-юнита
#          ALLOW_ROOT=1     осознанный запуск от root (не рекомендуется)
#          PORT=8787        порт дашборда для health-check (env)
#          SERVICE_NAME=pumpbot   имя systemd-юнита (env)
#
#  Скрипт идемпотентен: повторный запуск безопасен. Полный лог деплоя
#  пишется в deploy_<дата>.log рядом со скриптом.
# =============================================================================
set -uo pipefail

# ------------------------------------------------------------------ настройки
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

SERVICE_NAME="${SERVICE_NAME:-pumpbot}"
DASHBOARD_PORT="${DASHBOARD_PORT:-8787}"
RUN_USER="${SUDO_USER:-$(id -un)}"
SKIP_SERVICE=0
NO_HEALTH=0
AUTO_STASH=0
STEPS_TOTAL=7
STEP=0

LOG_FILE="$PROJECT_DIR/deploy_$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

# ------------------------------------------------------------------- helpers
step()  { STEP=$((STEP+1)); printf '\n\033[1;34m==> [%d/%d] %s\033[0m\n' "$STEP" "$STEPS_TOTAL" "$1"; }
ok()    { printf '\033[32m  OK\033[0m %s\n' "$1"; }
warn()  { printf '\033[33m  ВНИМАНИЕ:\033[0m %s\n' "$1"; }
fail()  { printf '\033[31m  ОШИБКА:\033[0m %s\n' "$1"; }
hint()  { printf '\033[33m  ПОДСКАЗКА: %s\033[0m\n' "$1"; }

on_error() {
    echo
    fail "деплой прерван на строке $LINENO (полный лог: $LOG_FILE)"
    hint "повторный запуск ./deploy.sh безопасен — шаги идемпотентны"
    hint "посмотрите последние строки лога: tail -n 40 $LOG_FILE"
    exit 1
}
trap on_error ERR

SUDO=""

# ------------------------------------------------------------------ флаги
while [ $# -gt 0 ]; do
    case "$1" in
        --skip-service) SKIP_SERVICE=1 ;;
        --no-health)    NO_HEALTH=1 ;;
        --auto-stash)   AUTO_STASH=1 ;;
        --user)
            shift
            if [ $# -eq 0 ]; then fail "--user требует имя пользователя"; exit 1; fi
            RUN_USER="$1" ;;
        --user=*) RUN_USER="${1#--user=}" ;;
        -h|--help)
            grep '^#  ' "$0" | sed 's/^#  \?//'
            exit 0 ;;
        *) warn "неизвестный аргумент $1 — игнорирую" ;;
    esac
    shift
done

# Бот работает с приватными ключами: сервис под root — критичный риск
# (уязвимость в зависимостях = полный контроль над VPS).
if [ "$(id -u)" -eq 0 ] && [ -z "${ALLOW_ROOT:-}" ]; then
    fail "деплой от root запрещён: сервис получил бы User=root вместе с ключами кошелька"
    hint "создайте сервисного пользователя: adduser --disabled-password pumpbot && chown -R pumpbot $PROJECT_DIR"
    hint "и запустите от него; осознанный обход: ALLOW_ROOT=1 ./deploy.sh (не рекомендуется)"
    exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then SUDO="sudo"; else
        warn "sudo не найден, а вы не root: шаги systemd будут пропущены"
        hint "запустите от root или установите sudo: apt install -y sudo"
    fi
fi

echo "==================================================================="
echo " Pump Scalper deploy · $(date '+%F %T')"
echo " проект: $PROJECT_DIR"
echo " сервис: $SERVICE_NAME · порт дашборда: $DASHBOARD_PORT · user: $RUN_USER"
echo " лог:    $LOG_FILE"
echo "==================================================================="

# ============================================================================
step "1/7 Окружение: ОС и Python >= 3.11"
if [ "$(uname -s)" != "Linux" ]; then
    fail "скрипт рассчитан на Linux; текущая ОС: $(uname -s)"
    hint "на Windows используйте WSL2 или выполняйте шаги вручную"
    exit 1
fi
ok "ОС: $(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME" || uname -sr)"

PY=""
for candidate in python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then PY="$candidate"; break; fi
done
if [ -z "$PY" ]; then
    fail "python3 не найден"
    hint "Ubuntu 22.04: sudo apt update && sudo apt install -y python3.11 python3.11-venv"
    hint "или deadsnakes: sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install -y python3.11-venv"
    exit 1
fi
if ! "$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)'; then
    fail "$(python3 --version 2>&1) — нужен Python >= 3.11"
    hint "Ubuntu 22.04 ставит 3.10 по умолчанию: установите python3.11 (deadsnakes) и повторите"
    exit 1
fi
ok "Python: $($PY --version 2>&1) ($PY)"

if ! "$PY" -m venv --help >/dev/null 2>&1; then
    fail "модуль venv недоступен"
    hint "sudo apt install -y python3-venv (или python3.11-venv)"
    exit 1
fi
ok "модуль venv доступен"

# ============================================================================
step "2/7 Код: обновление из git (если это репозиторий)"
if [ -d .git ]; then
    if git pull --ff-only 2>>"$LOG_FILE"; then
        ok "git pull --ff-only: код актуален ($(git rev-parse --short HEAD 2>/dev/null || echo '?'))"
    elif [ "$AUTO_STASH" -eq 1 ]; then
        warn "локальные правки мешают pull — пробую git stash (флаг --auto-stash)"
        if git stash push -m "deploy auto-stash $(date '+%F %T')" >>"$LOG_FILE" 2>&1 \
            && git pull --ff-only >>"$LOG_FILE" 2>&1; then
            ok "pull выполнен после stash"
            if git stash pop >>"$LOG_FILE" 2>&1; then
                ok "локальные правки возвращены из stash"
            else
                warn "git stash pop конфликтует — правки лежат в stash"
                hint "разрешите вручную: git stash show -p; git stash drop — после разбора"
            fi
        else
            fail "stash/pull не удался — состояние откатывать не буду"
            hint "разберитесь вручную: git stash list; затем повторите ./deploy.sh"
            exit 1
        fi
    else
        fail "git pull не удался (локальные правки конфликтуют)"
        hint "вариант без потери правок: ./deploy.sh --auto-stash (stash → pull → pop)"
        hint "вручную: git stash && ./deploy.sh, или git reset --hard origin/HEAD (ОСТОРОЖНО: потеря правок)"
        exit 1
    fi
else
    warn ".git не найден — считаю, что код доставлен rsync/scp; пропускаю pull"
    hint "обновлять код: rsync -av --exclude .venv --exclude data --exclude logs --exclude .env ./ user@vps:$PROJECT_DIR/"
fi

# ============================================================================
step "3/7 Конфигурация: .env и ключи"
# Код мог быть перенесён с Windows: CRLF ломает grep-проверки ниже.
# Нормализуем и существующий .env, и шаблон: git с autocrlf=true отдаёт
# .env.example с CRLF, и cp переносит их в свежесозданный .env.
if grep -q $'\r' .env 2>/dev/null; then
    sed -i 's/\r$//' .env
    ok ".env приведён к LF (был CRLF)"
fi
if [ ! -f .env ]; then
    if [ -f .env.example ] && grep -q $'\r' .env.example 2>/dev/null; then
        sed -i 's/\r$//' .env.example
        ok ".env.example приведён к LF (был CRLF)"
    fi
    cp .env.example .env 2>/dev/null || { fail "нет ни .env, ни .env.example"; hint "восстановите файлы из репозитория или перенесите rsync-ом"; exit 1; }
    warn ".env отсутствовал — создан из .env.example с дефолтами"
    hint "ЗАПОЛНИТЕ секреты до реальной торговли: nano .env → HELIUS_API_KEY, WALLET_PRIVATE_KEY (или WALLET_KEYPAIR_PATH)"
    hint "для первого запуска можно оставить как есть: режим dry-run включается флагом --dry-run в юните"
elif grep -q "^HELIUS_API_KEY=your-helius-api-key\|^HELIUS_API_KEY=$" .env; then
    warn "HELIUS_API_KEY пустой или заглушка — RPC/WS будут падать с ошибками подключения"
    hint "nano .env → вставьте ключ с https://dashboard.helius.dev (dry-run без ключа тоже не подключится)"
else
    ok ".env на месте, HELIUS_API_KEY заполнен"
fi

WALLET_PATH="$(grep -E '^WALLET_KEYPAIR_PATH=' .env | cut -d= -f2 | tr -d '"' || true)"
if grep -q "^WALLET_PRIVATE_KEY=$" .env && [ -n "$WALLET_PATH" ] && [ ! -f "$WALLET_PATH" ]; then
    warn "кошелёк не найден: приватный ключ пуст, а keypair-файла $WALLET_PATH нет"
    hint "live-торговля не стартует (ошибка загрузки кошелька); dry-run — можно игнорировать"
    hint "ключи не копируйте через git: scp keys/id.json user@vps:$PROJECT_DIR/keys/id.json && chmod 600"
fi

if grep -q "^DASHBOARD_HOST=0.0.0.0" .env; then
    warn "DASHBOARD_HOST=0.0.0.0: дашборд торчит наружу, аутентификации на странице НЕТ"
    hint "закройте порт: sudo ufw deny $DASHBOARD_PORT, смотрите через ssh -L $DASHBOARD_PORT:127.0.0.1:$DASHBOARD_PORT user@vps"
fi
ok "конфигурация проверена"

# ============================================================================
step "4/7 Зависимости: .venv и pip install"
if [ ! -d .venv ]; then
    "$PY" -m venv .venv || { fail "не удалось создать .venv"; hint "проверьте права записи в $PROJECT_DIR и наличие python3-venv"; exit 1; }
    ok ".venv создана"
else
    ok ".venv уже существует"
fi

VPY=".venv/bin/python"
"$VPY" -m pip install --upgrade pip wheel >/dev/null 2>&1 \
    || warn "не удалось обновить pip/wheel — продолжаю с текущей версией"

if "$VPY" -m pip install -r requirements.txt; then
    ok "зависимости установлены"
else
    fail "pip install завершился с ошибкой"
    hint "чаще всего: нет доступа к PyPI. Проверьте интернет: curl -sI https://pypi.org | head -1"
    hint "ошибки сборки нативных пакетов: sudo apt install -y build-essential"
    hint "детали ошибки — выше в этом логе"
    exit 1
fi

if "$VPY" -m compileall -q -x '(^|/)(\.venv|\.git)(/|$)' .; then
    ok "синтаксис всех .py проекта валиден (compileall)"
else
    fail "compileall нашёл битые файлы"
    hint "похоже, код повредился при переносе: повторите rsync/scp и сравните checksums"
    exit 1
fi

# ============================================================================
step "5/7 Порт дашборда: свободен ли $DASHBOARD_PORT"
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ":$DASHBOARD_PORT "; then
    OWNER=$(ss -ltnp 2>/dev/null | grep ":$DASHBOARD_PORT " | grep -oP 'pid=\K[0-9]+' | head -1 || true)
    if [ -n "$OWNER" ] && [ "$OWNER" = "$(systemctl show -p MainPID --value "$SERVICE_NAME" 2>/dev/null)" ]; then
        ok "порт занят текущим сервисом $SERVICE_NAME — перезапустим на шаге 7"
    else
        warn "порт $DASHBOARD_PORT занят процессом ${OWNER:-неизвестно}"
        hint "кто занимает: sudo ss -ltnp | grep :$DASHBOARD_PORT"
        hint "освободите порт или задайте другой: PORT=8788 ./deploy.sh (и DASHBOARD_PORT=8788 в .env)"
    fi
else
    ok "порт $DASHBOARD_PORT свободен"
fi

# ============================================================================
if [ "$SKIP_SERVICE" -eq 1 ]; then
    step "6/7 systemd-сервис (пропущен флагом --skip-service)"
    hint "запуск вручную: .venv/bin/python main.py --dry-run"
else
    step "6/7 systemd-сервис: установка и запуск"

    if ! command -v systemctl >/dev/null 2>&1; then
        warn "systemd недоступен (контейнер?) — поднимаю через nohup"
        hint "без systemd автоперезапуска не будет; рассмотрите docker/supervisor"
        pkill -f "$PROJECT_DIR/.venv/bin/python main.py" 2>/dev/null || true
        ( nohup "$VPY" main.py >> "$PROJECT_DIR/logs/bot_nohup.out" 2>&1 & echo $! > bot.pid )
        ok "бот запущен фоном, PID $(cat bot.pid); лог: logs/bot_nohup.out"
    else
        UNIT="/etc/systemd/system/$SERVICE_NAME.service"
        DIR_OWNER="$(stat -c '%U' "$PROJECT_DIR" 2>/dev/null || true)"
        if [ -n "$DIR_OWNER" ] && [ "$DIR_OWNER" != "$RUN_USER" ]; then
            warn "каталог принадлежит $DIR_OWNER, а сервис будет работать от $RUN_USER"
            hint "перед стартом передайте владение: sudo chown -R $RUN_USER $PROJECT_DIR"
        fi
        cat <<EOF | $SUDO tee "$UNIT" >/dev/null
[Unit]
Description=Solana Pump Momentum Scalping Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$RUN_USER
WorkingDirectory=$PROJECT_DIR
# BOT_ARGS позволяет запускать безопасный режим без правки юнита:
# systemctl edit $SERVICE_NAME → [Service] Environment=BOT_ARGS=--dry-run
Environment=PYTHONUNBUFFERED=1
Environment=BOT_ARGS=
ExecStart=/bin/sh -ec 'exec "$PROJECT_DIR/.venv/bin/python" main.py \${BOT_ARGS}'
# on-failure: чистая остановка (код 0, кнопка дашборда) НЕ перезапускается,
# падения (код != 0) — перезапускаются автоматически
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
        ok "юнит записан: $UNIT"
        hint "первый запуск безопаснее в dry-run: sudo systemctl edit $SERVICE_NAME → [Service] Environment=BOT_ARGS=--dry-run"

        $SUDO systemctl daemon-reload || { fail "daemon-reload не удался"; hint "проверьте синтаксис юнита: systemd-analyze verify $UNIT"; exit 1; }
        $SUDO systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 && ok "автостарт включён (enable)"
        $SUDO systemctl restart "$SERVICE_NAME" || { fail "systemctl restart не удался"; hint "причина: journalctl -u $SERVICE_NAME -n 30 --no-pager"; exit 1; }
        ok "сервис перезапущен"
        hint "права на каталоги у пользователя $RUN_USER: при ошибках записи выполните chown -R $RUN_USER $PROJECT_DIR"
    fi
fi

# ============================================================================
if [ "$NO_HEALTH" -eq 1 ]; then
    step "7/7 Health-check (пропущен флагом --no-health)"
else
    step "7/7 Health-check веб-дашборда (до 40 c)"
    if ! command -v curl >/dev/null 2>&1; then
        warn "curl не найден — health-check пропущен"
        hint "sudo apt install -y curl"
    else
        HEALTH_OK=0
        for i in $(seq 1 20); do
            if curl -fs "http://127.0.0.1:$DASHBOARD_PORT/api/snapshot" >/dev/null 2>&1; then
                HEALTH_OK=1; break
            fi
            sleep 2
        done
        if [ "$HEALTH_OK" -eq 1 ]; then
            ok "дашборд отвечает: http://127.0.0.1:$DASHBOARD_PORT/"
            ok "снять состояние: curl -s http://127.0.0.1:$DASHBOARD_PORT/api/snapshot"
        else
            fail "дашборд не отвечает за отведённое время"
            hint "логи бота: journalctl -u $SERVICE_NAME -n 50 --no-pager (или tail -n 50 logs/bot_nohup.out)"
            hint "типовые причины: нет DNS/ключа Helius в .env (WS reconnect в логе — это не падение), занятый порт, ошибка в .env"
            hint "дашборд стартует ДО сети; если страница не открывается — смотрите именно journalctl"
            warn "деплой продолжён, но сервис требует внимания"
        fi
    fi
fi

# ============================================================================
echo
echo "==================================================================="
echo " Деплой завершён: $(date '+%F %T')"
echo "-------------------------------------------------------------------"
echo " Наблюдение:"
echo "   сервис:        journalctl -u $SERVICE_NAME -f"
echo "   дашборд (VPS): curl -s http://127.0.0.1:$DASHBOARD_PORT/api/snapshot | head -c 400"
echo "   дашборд (у себя): ssh -L $DASHBOARD_PORT:127.0.0.1:$DASHBOARD_PORT $RUN_USER@vps"
echo "                      → открыть http://127.0.0.1:$DASHBOARD_PORT/"
echo " Файлы:"
echo "   логи бота:     $PROJECT_DIR/logs/bot_*.log, errors_*.log"
echo "   БД позиций:    $PROJECT_DIR/data/bot.db"
echo " Управление:"
echo "   systemctl restart $SERVICE_NAME | stop | status"
echo " Обновление кода:"
echo "   rsync из локальной копии → git pull выполняется этим скриптом → ./deploy.sh"
echo "==================================================================="
exit 0
