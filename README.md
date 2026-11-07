# Solana Pump Momentum & Liquidity Scalping Bot

Высокочастотный скальпинг-бот для мемкоинов **Pump.fun** в сети Solana.
Считывает поток сделок через WebSocket (Helius), жёстко фильтрует ликвидность и
безопасность контракта, входит на импульсе объёма и выходит за 1–3 секунды через
каскадный тейк-профит, трейлинг-стоп, жёсткий стоп-лосс и детектор затухания импульса.

Поддерживаются обе стадии жизни токена Pump.fun: bonding curve (прямые
инструкции) и PumpSwap AMM после миграции (через Jupiter fallback).
Raydium по умолчанию **отключён** (`ENABLED_DEX=pump_fun`); вернуть его можно
значением `ENABLED_DEX=pump_fun,raydium` — прямых Raydium-инструкций в коде
нет, сделки по таким пулам всегда идут через Jupiter.

## Возможности

- **Потоковый листенер**: `logsSubscribe` + `accountSubscribe` (Helius WebSocket) с
  авто-реконнектом и экспоненциальным backoff; события Pump.fun разбираются
  из Anchor-логов (`TradeEvent`), для AMM-пулов сделки синтезируются из дельт
  vault-балансов.
- **Liquidity Guard**: минимальная ликвидность пула (`MIN_POOL_SOL`), предельный
  Price Impact (`MAX_PRICE_IMPACT_PCT`), расчёт `min_tokens_out` по формуле
  постоянного произведения `x·y = k` с дисконтом `BUY_SLIPPAGE_PCT`.
- **Контур отбора токенов до торговли**: каждое новое имя монеты при первом
  обнаружении проходит фоновую проверку контракта (mint/freeze authority, LP
  burn) и получает вердикт `approved/rejected` в SQLite-аудит; горячий путь
  торговли не делает ни одного RPC-запроса по безопасности, а отклонённые
  токены вычищаются из реестра пулов и индикаторов.
- **Momentum Trigger**: Buy/Sell Ratio и Volume Velocity в скользящем окне.
- **Движок выходов**: каскадный TP (+30% / +50%), trailing stop (откат от пика),
  hard stop-loss (−30%), детектор крупных продаж и падения Buy/Sell Ratio,
  таймаут удержания (`MAX_HOLD_SECONDS`, по умолчанию 180 c).
- **Два канала отправки** (`EXECUTION_VENUE`): `rpc` (по умолчанию) — обычная
  транзакция через Helius без Jito-чаевых, подтверждение через
  `getSignatureStatuses`; `jito` — бандл с memo `jitodontfront` и чаевыми
  валидатору, подтверждение через `getBundleStatuses`. В обоих каналах позиция
  учитывается только после подтверждения (`JITO_REQUIRE_CONFIRMATION`).
- **Лестница выхода**: первая попытка продажи с дисконтом `SELL_SLIPPAGE_PCT`;
  если транзакция откатилась по порогу — свежая котировка и повтор с
  `min_sol_output = 0`, чтобы выход исполнялся в любом случае (повтор только
  при заведомо неотправленной транзакции; при неопределённом статусе повтор
  опасен двойной продажей).
- **Jupiter fallback** (опция, по умолчанию **выключен**): резервный маршрут
  через агрегатор (`/swap-instructions`) для токенов, мигрировавших с bonding
  curve на PumpSwap AMM. Без него бот торгует только токены на кривой; продажа
  позиции, в которой токен успел мигрировать, завершится ошибкой — включайте
  `USE_JUPITER_FALLBACK=true` для live-торговли с удержанием позиций.
- **Персистентность позиций**: SQLite (`PORTFOLIO_DB_PATH`, WAL, write-through) —
  открытые позиции переживают рестарт, бот продолжает вести их выходы; dry-run
  пишет в отдельную БД (`data/bot_dry.db`).
- **Несколько позиций**: лимит `MAX_CONCURRENT_POSITIONS` (по умолчанию 3)
  и опциональный лимит капитала `CAPITAL_SOL`.
- **Бэктестер**: прогон исторических CSV через те же фильтры, стратегию и
  движок решений (переиспользует `MomentumTrader`, а не копию логики).

## Архитектура

Живой путь события (dry-run отличается только исполнителем):

```
Helius WS (logsSubscribe: Pump.fun, PumpSwap AMM; accountSubscribe: пулы)
  │
  ▼
core/listener.py  PoolStreamListener ── парсинг Anchor-логов в TradeEvent
  │                 (bonding curve: дискриминатор TradeEvent;
  │                  AMM: синтез сделок из дельт vault-ов)
  │
  ├─[новый mint]──► core/screening.py  TokenScreeningService ── КОНТУР ОТБОРА
  │                   очередь → проверка контракта по правилам
  │                   (mint/freeze authority, LP burn) → вердикт
  │                   в кэш + SQLite token_screening (аудит, рестарт);
  │                   rejected → пул вычищается из реестра/индикаторов
  ▼
asyncio.Queue (8192, put_nowait — WS-колбэк не блокируется)
  │
  ▼
core/pipeline.py  LivePipeline ── consume-loop, обслуживание реестра пулов,
  │                 восстановление открытых позиций из БД при старте
  ▼
core/trader.py  MomentumTrader ── единственная точка решений:
  │               индикаторы → фильтры → стратегия → заявка → исполнитель
  ├─► analytics/indicators.py   скользящие окна (ratio, velocity, цена)
  ├─► core/screening.py provider  готовый вердикт из кэша (ноль RPC):
  │     approved → TokenSecurity; rejected/нет вердикта → None (fail-closed)
  ├─► filters/                  LiquidityGuard, SecurityGuard (страховка)
  ├─► strategies/momentum_pump.py   триггеры входа/выхода (чистая логика)
  ├─► core/orders.py            котировки AMM и сборка Buy/SellOrder
  └─► core/executor.py          план свапа → компиляция v0 → подпись
        ├─ core/tx_builder.py   инструкции bonding curve Pump.fun
        ├─ core/jupiter.py      fallback-маршрут (AMM/миграция, опция)
        ├─ core/jito.py         sendBundle + ожидание Landed
        └─ core/dry_run.py      бумажное исполнение (--dry-run, бэктест)
  │
  ▼
core/portfolio.py  PortfolioManager ── memory-ledger позиций
  └─► core/persistence.py  PortfolioStore ── SQLite write-through
        (таблицы positions / closed_trades / token_screening;
         рестарт → load_open_positions + load_persisted вердикты)
```

Исполнители подключаются инъекцией через Protocol `TraderExecutor`
(`core/orders.py`), поэтому live, dry-run и бэктест используют одну и ту же
математику котировок и один движок решений. Все сетевые данные безопасности —
`core/security.py` (`TokenSecurityResolver`, RPC + TTL-кэш) — подаются
трейдеру по требованию перед входом.

## Структура проекта

```
pumpfun/
├── main.py                     # CLI-точка входа: --dry-run/--status/--watchlist…
├── config/settings.py          # pydantic-settings, все параметры (.env)
├── core/
│   ├── client.py               # async RPC (aiohttp) + WS-менеджер с backoff
│   ├── listener.py             # подписки logs/account, парсинг → TradeEvent
│   ├── decoders.py             # Borsh-раскладки: TradeEvent, BondingCurve, пулы
│   ├── pool_registry.py        # TrackedPool / PoolRegistry (индексы, prune)
│   ├── screening.py            # контур отбора: очередь, вердикты, provider
│   ├── pipeline.py             # LivePipeline: очередь, рестарт позиций, stop
│   ├── trader.py               # MomentumTrader: событие → решение
│   ├── orders.py               # quote_entry/exit, build_*, TraderExecutor
│   ├── tx_builder.py           # инструкции bonding curve, PDA, tip, memo
│   ├── executor.py             # план свапа → v0-транзакция → rpc | Jito
│   ├── jito.py                 # Block Engine: tip-аккаунты, sendBundle, статусы
│   ├── jupiter.py              # /quote, /swap-instructions, lookup tables
│   ├── dry_run.py              # бумажный исполнитель (dry-run/бэктест)
│   ├── security.py             # TokenSecurityResolver (mint/LP через RPC)
│   ├── portfolio.py            # PortfolioManager: позиции, PnL, статистика
│   └── persistence.py          # PortfolioStore: SQLite (позиции/сделки/отбор)
├── filters/                    # BaseFilter/FilterChain, LiquidityGuard, SecurityGuard
├── strategies/                 # BaseStrategy, MomentumPumpStrategy (вход/выход)
├── analytics/
│   ├── indicators.py           # RollingTradeWindow / StreamingIndicators
│   ├── replay.py               # CSV-события, ReplayPoolState, сценарии
│   └── backtester.py           # Backtester + BacktestReport (PnL, drawdown)
├── models/trade_data.py        # Pydantic-схемы: Pool, TradeEvent, Signal, Order…
├── utils/                      # constants (адреса/дискриминаторы), amm (x·y=k),
│                               # spl (keypair/ATA/mint), latency, logger (loguru)
├── tests/                      # pytest: 12 модулей, 131 тест
├── data/                       # SQLite-БД позиций (создаётся автоматически)
└── logs/                       # loguru: bot_*.log, errors_*.log (ротация)
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

### Ключевые настройки

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `ENABLED_DEX` | `pump_fun` | Включённые DEX: `pump_fun` и/или `raydium` (неизвестные значения — ошибка старта) |
| `EXECUTION_VENUE` | `rpc` | Канал отправки: `rpc` (Helius, без чаевых) или `jito` (бандл + tip) |
| `USE_JUPITER_FALLBACK` | `false` | Запасной маршрут для мигрировавших токенов; при холдах дольше пары секунд включайте `true` |
| `MAX_CONCURRENT_POSITIONS` | `3` | Лимит одновременных позиций |
| `CAPITAL_SOL` | пусто | Лимит свободного капитала, SOL |
| `PORTFOLIO_DB_PATH` | `data/bot.db` | SQLite-БД позиций (пусто = только память) |
| `JITO_REQUIRE_CONFIRMATION` | `true` | Ждать подтверждения транзакции/бандла перед учётом позиции |
| `TRADE_AMOUNT_SOL` | `0.5` | Размер входного ордера |
| `BUY_SLIPPAGE_PCT` | `20` | Допуск проскальзывания входа, % |
| `SELL_SLIPPAGE_PCT` | `25` | Дисконт первой попытки выхода (вторая — с floor 0, «лестница») |
| `HARD_STOP_LOSS_PCT` | `30` | Жёсткий стоп-лосс, % от входа |
| `MAX_HOLD_SECONDS` | `180` | Принудительный выход по таймауту, c |
| `REQUIRE_MINT/FREEZE_AUTHORITY_NULL`, `MIN_LP_BURNED_PCT` | `true/true/90` | Правила контура отбора (применяются до торговли, см. архитектуру) |
| `SCREEN_RECHECK_SECONDS` | `600` | TTL вердикта отбора до повторной проверки |

Полный список — в [.env.example](.env.example) и `config/settings.py`.

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
