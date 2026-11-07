# Solana Pump Momentum & Liquidity Scalping Bot

Высокочастотный скальпинг-бот для мемкоинов в сети Solana (Pump.fun / Raydium AMM v4).
Считывает поток сделок через WebSocket (Helius), жёстко фильтрует ликвидность и
безопасность контракта, входит на импульсе объёма и выходит за 1–3 секунды через
каскадный тейк-профит, трейлинг-стоп, жёсткий стоп-лосс и детектор затухания импульса.

## Возможности

- **Потоковый листенер**: `logsSubscribe` + `accountSubscribe` (Helius WebSocket) с
  авто-реконнектом и экспоненциальным backoff.
- **Liquidity Guard**: минимальная ликвидность пула (`MIN_POOL_SOL`), предельный
  Price Impact (`MAX_PRICE_IMPACT_PCT`), расчёт `min_tokens_out` по формуле
  постоянного произведения `x·y = k` с дисконтом `BUY_SLIPPAGE_PCT`.
- **Security Guard**: проверка `mint authority == None`, `freeze authority == None`,
  доля сожжённого/залоченного LP (`MIN_LP_BURNED_PCT`).
- **Momentum Trigger**: Buy/Sell Ratio и Volume Velocity в скользящем окне.
- **Движок выходов**: каскадный TP (+30% / +50%), trailing stop (откат от пика),
  hard stop-loss (−12%), детектор крупных продаж и падения Buy/Sell Ratio.
- **Исполнение через Jito**: сборка транзакций (`solders`), Compute Budget,
  Jito tip, `jitodontfront` (анти-сэндвич), отправка `sendBundle` с ожиданием
  подтверждения через `getBundleStatuses` (`JITO_REQUIRE_CONFIRMATION`) —
  позиция учитывается только после `Landed`.
- **Jupiter fallback**: резервный маршрут через агрегатор (`/swap-instructions`),
  используется для AMM-пулов и мигрировавших токенов (прямые инструкции
  строятся только для bonding curve Pump.fun).
- **Персистентность позиций**: SQLite (`PORTFOLIO_DB_PATH`, WAL) — открытые
  позиции переживают рестарт, бот продолжает вести их выходы; dry-run пишет
  в отдельную БД (`data/bot_dry.db`).
- **Бэктестер**: прогон исторических CSV через те же фильтры и стратегию.

## Архитектура

```
pump_bot/
├── config/settings.py          # pydantic-settings (.env)
├── core/client.py              # Async RPC + WebSocket manager
├── core/listener.py            # Слушатель логов/аккаунтов + парсинг
├── core/tx_builder.py          # Сборка инструкций (Pump.fun / Raydium)
├── core/executor.py            # Jito sendBundle + Jupiter fallback
├── filters/                    # LiquidityGuard, SecurityGuard
├── strategies/                 # MomentumPumpStrategy
├── analytics/                  # indicators, backtester
├── models/trade_data.py        # Pydantic-схемы
├── utils/                      # constants, logger, latency, spl
├── main.py                     # Точка входа (async pipeline)
└── tests/                      # pytest
```

## Установка

Требуется Python 3.11+.

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env   # заполните HELIUS_API_KEY и кошелёк
```

### Кошелёк

Задайте один из вариантов:

- `WALLET_PRIVATE_KEY` — приватный ключ в base58, либо
- `WALLET_KEYPAIR_PATH` — путь к JSON keypair (`solana-keygen new -o keys/id.json`).

## Запуск

```bash
python main.py                    # live-пайплайн
python main.py --dry-run          # без отправки транзакций (бумажное исполнение)
python main.py --positions 5      # до 5 одновременных позиций
python main.py --capital 2.0      # лимит капитала, SOL
python main.py --watchlist MINT1 MINT2   # отслеживать пулы со старта
python main.py --close-on-stop    # продавать позиции при остановке
python main.py --status           # показать открытые позиции/сводку из БД и выйти
```

Открытые позиции по умолчанию **не** закрываются при остановке и восстанавливаются
при следующем запуске из SQLite-БД (`PORTFOLIO_DB_PATH`, dry-run — `data/bot_dry.db`;
пустой `PORTFOLIO_DB_PATH` отключает персистентность).

## Тесты

```bash
pytest -q
ruff check .
mypy --config-file pyproject.toml .
```

## Важные предупреждения

1. **Продакшен — только Linux VPS** в том же датацентре, что и RPC-нода
   (Frankfurt/Amsterdam), иначе скальпинг на 1–3 секунды теряет смысл.
2. **Проскальзывание 20% / 25%** — это максимальный допуск в инструкции, а не
   фактическая потеря, но при затухании импульса выход может стоить дорого.
3. **Jito rate limit** по умолчанию — 1 запрос/сек на IP на регион. Для HFT нужен
   повышенный лимит (тикет в Jito).
4. **Бэктест обязателен** перед торговлей реальными средствами: цифры стратегии
   требуют калибровки на живых данных.

## Лицензия

Проект предоставляется «как есть», без гарантий. Торговля криптоактивами связана
с риском полной потери средств. Автор не несёт ответственности за финансовые потери.
