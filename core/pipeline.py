"""Live-пайплайн: поток событий → движок решений → исполнитель.

Модуль связывает сетевые слои в единый работающий процесс:

1. :class:`~core.client.SolanaRpcClient` и :class:`~core.client.SolanaWsClient`
   дают транспорт;
2. :class:`~core.listener.PoolStreamListener` превращает WS-уведомления в
   :class:`~models.trade_data.TradeEvent` и складывает их в очередь;
3. :class:`~core.trader.MomentumTrader` применяет фильтры, стратегию и
   отправляет ордера через исполнитель
   (:class:`~core.executor.OrderExecutor` либо
   :class:`~core.dry_run.DryRunExecutor`);
4. :class:`~core.security.TokenSecurityResolver` подаёт данные безопасности
   контракта по требованию трейдера.

Пайплайн отвечает только за жизненный цикл и маршрутизацию событий. Он не
принимает торговых решений, поэтому режим ``dry_run`` отличается ровно одним
объектом — исполнителем: поток событий, фильтры, стратегия и учёт позиций те
же самые, что и в реальной торговле.

Завершение graceful: подписки снимаются, клиенты закрываются, сводка
пишется в лог. Открытые позиции по умолчанию **не** закрываются принудительно
(принудительный market-sell на выходе может стоить дороже, чем позиция,
оставленная до ручного разбора) — это включается флагом
``close_positions_on_stop``.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from analytics.backtester import Backtester
from analytics.indicators import StreamingIndicators
from analytics.replay import (
    ReplayError,
    ReplayPoolConfig,
    ReplayScenario,
    events_to_csv_text,
    parse_events_csv_text,
)
from config.settings import LAMPORTS_PER_SOL, settings
from core.client import RpcError, SolanaRpcClient, SolanaWsClient
from core.dashboard import DashboardServer
from core.decoders import PumpTradeEvent
from core.dry_run import DryRunExecutor
from core.executor import OrderExecutor
from core.listener import PoolStreamListener, extract_pump_trade_events
from core.orders import TraderExecutor
from core.persistence import PortfolioStore
from core.pool_registry import TrackedPool
from core.portfolio import PortfolioManager, PortfolioStats
from core.screening import ScreeningVerdict, TokenScreeningService
from core.security import TokenSecurityResolver
from core.trader import MomentumTrader
from core.tx_builder import derive_bonding_curve
from models.trade_data import (
    DEXType,
    PoolInfo,
    SignalAction,
    TokenSecurity,
    TradeEvent,
    TradeSide,
)
from utils.constants import PUBLIC_MAINNET_RPC, PUMP_TOKEN_DECIMALS, SOL_DECIMALS
from utils.latency import LatencyRegistry
from utils.logger import get_logger
from utils.spl import load_keypair

logger = get_logger()

#: Таймаут ожидания события из очереди перед обслуживанием пайплайна, c.
QUEUE_POLL_TIMEOUT: float = 1.0


class PipelineError(RuntimeError):
    """Ошибка запуска или работы пайплайна."""


@dataclass(slots=True)
class PipelineStats:
    """Счётчики работы пайплайна.

    Attributes:
        events: Обработано событий сделок.
        entries: Успешных входов в позицию.
        exits: Успешных выходов из позиции.
        orders: Успешно отправленных ордеров.
        failures: Ордеров, отправка которых завершилась ошибкой.
        parse_errors: Исключений при обработке событий.
        started_at: Время запуска, unix ts.
    """

    events: int = 0
    buys: int = 0
    sells: int = 0
    entries: int = 0
    exits: int = 0
    orders: int = 0
    failures: int = 0
    parse_errors: int = 0
    started_at: float = 0.0

    def as_dict(self) -> dict[str, float | int]:
        """Сводка счётчиков для логов.

        Returns:
            Словарь со счётчиками и временем работы.
        """
        uptime = time.time() - self.started_at if self.started_at else 0.0
        return {
            "events": self.events,
            "buys": self.buys,
            "sells": self.sells,
            "entries": self.entries,
            "exits": self.exits,
            "orders": self.orders,
            "failures": self.failures,
            "parse_errors": self.parse_errors,
            "uptime_seconds": round(uptime, 1),
        }


class LivePipeline:
    """Сетевой пайплайн бота (live или dry-run).

    Создаёт и держит все компоненты, гоняет очередь событий и обеспечивает
    корректное завершение работы.
    """

    def __init__(
        self,
        *,
        dry_run: bool = False,
        watchlist: Sequence[str] = (),
        capital_sol: float | None = None,
        close_positions_on_stop: bool = False,
        maintenance_interval_seconds: float | None = None,
        db_path: str | None = None,
        dashboard_enabled: bool | None = None,
    ) -> None:
        """Инициализирует пайплайн (без сетевых вызовов).

        Args:
            dry_run: Не отправлять транзакции (бумажное исполнение).
            watchlist: Mint-адреса, которые нужно отслеживать с момента старта.
                (``None`` — из настроек).
            capital_sol: Ограничение свободного капитала, SOL (``None`` — из
                настроек).
            close_positions_on_stop: Принудительно продавать позиции при
                завершении работы.
            maintenance_interval_seconds: Интервал обслуживания (очистка
                «молчащих» пулов, сводка в лог), c (``None`` — из настроек).
            db_path: Путь к SQLite-БД позиций (``None`` — из настроек;
                dry-run по умолчанию пишет в отдельный файл).
            dashboard_enabled: Веб-дашборд (``None`` — из настроек; занятый
                порт не валит запуск — пайплайн продолжит без дашборда).
        """
        self._dry_run = dry_run
        self._watchlist = tuple(watchlist)
        self._capital_sol = (
            settings.capital_sol if capital_sol is None else capital_sol
        )
        self._close_positions_on_stop = close_positions_on_stop
        self._maintenance_interval = max(
            5.0,
            settings.maintenance_interval_seconds
            if maintenance_interval_seconds is None
            else maintenance_interval_seconds,
        )
        if db_path is not None:
            self._db_path = db_path
        elif dry_run:
            self._db_path = settings.portfolio_dry_db_path
        else:
            self._db_path = settings.portfolio_db_path
        self._dashboard_enabled = (
            settings.dashboard_enabled
            if dashboard_enabled is None
            else dashboard_enabled
        )

        self._latency = LatencyRegistry()
        self._rpc: SolanaRpcClient | None = None
        self._ws: SolanaWsClient | None = None
        self._listener: PoolStreamListener | None = None
        self._resolver: TokenSecurityResolver | None = None
        self._screening: TokenScreeningService | None = None
        self._executor: TraderExecutor | None = None
        self._trader: MomentumTrader | None = None
        self._store: PortfolioStore | None = None
        self._dashboard: DashboardServer | None = None

        self._pnl_curve: list[tuple[float, float]] = []
        self._hold_reasons: dict[str, int] = {}
        self._backtest_stop: asyncio.Event | None = None
        self._backtest_run_id: str = ""
        self._started: bool = False
        self._trading_active: bool = False
        self._consume_task: asyncio.Task[None] | None = None
        self._sweep_task: asyncio.Task[None] | None = None
        self._fully_stopped = asyncio.Event()
        self._portfolio: PortfolioManager | None = None
        self._rate_ts: float = 0.0
        self._rate_events: int = 0
        self._events_per_sec: float = 0.0

        self._stop_event = asyncio.Event()
        self._running = False
        self._last_maintenance = 0.0
        self.stats = PipelineStats()

    # --------------------------------------------------------------- Свойства
    @property
    def dry_run(self) -> bool:
        """Работает ли пайплайн в бумажном режиме."""
        return self._dry_run

    @property
    def is_running(self) -> bool:
        """Запущен ли цикл обработки событий."""
        return self._running

    @property
    def trader(self) -> MomentumTrader:
        """Движок решений.

        Raises:
            PipelineError: Если пайплайн ещё не запущен.
        """
        if self._trader is None:
            raise PipelineError("пайплайн не запущен: трейдер недоступен")
        return self._trader

    @property
    def listener(self) -> PoolStreamListener:
        """Слушатель потока событий.

        Raises:
            PipelineError: Если пайплайн ещё не запущен.
        """
        if self._listener is None:
            raise PipelineError("пайплайн не запущен: слушатель недоступен")
        return self._listener

    @property
    def executor(self) -> TraderExecutor:
        """Исполнитель ордеров.

        Raises:
            PipelineError: Если пайплайн ещё не запущен.
        """
        if self._executor is None:
            raise PipelineError("пайплайн не запущен: исполнитель недоступен")
        return self._executor

    @property
    def latency(self) -> LatencyRegistry:
        """Реестр метрик латентности пайплайна."""
        return self._latency

    @property
    def screening(self) -> TokenScreeningService:
        """Служба отбора токенов.

        Raises:
            PipelineError: Если пайплайн ещё не запущен.
        """
        if self._screening is None:
            raise PipelineError("пайплайн не запущен: служба отбора недоступна")
        return self._screening

    # ---------------------------------------------------------- Жизненный цикл
    async def start(self) -> None:
        """Поднимает транспорт, слушателя, трейдера и исполнителя.

        Raises:
            PipelineError: Если не удалось загрузить кошелёк или установить
                WS-соединение.
        """
        if self._started:
            return
        self._started = True

        if self._db_path:
            self._store = PortfolioStore(self._db_path)
        else:
            self._store = PortfolioStore(
                settings.portfolio_dry_db_path
                if self._dry_run
                else settings.portfolio_db_path
            )

        self._portfolio = PortfolioManager(store=self._store)
        restored = self._portfolio.load_open_positions()
        if restored:
            logger.warning(
                "Рестарт с открытыми позициями: занято {} — новые входы "
                "ограничены, пока восстановленные позиции не закроются",
                self._portfolio.open_count,
            )

        await self.start_trading()

        # Дашборд поднимается до сети: страница доступна сразу — в том числе
        # для диагностики, почему WS не подключается.
        if self._dashboard_enabled:
            self._dashboard = DashboardServer(
                self.dashboard_snapshot,
                actions=self._dashboard_action,
                backtest_runner=self._dashboard_backtest,
                on_shutdown=self._dashboard_shutdown,
                auth_user=settings.dashboard_user,
                auth_password=settings.dashboard_password,
                host=settings.dashboard_host,
                port=settings.dashboard_port,
                interval_seconds=settings.dashboard_interval_seconds,
            )
            self._dashboard.attach_logger()
            try:
                await self._dashboard.start()
            except OSError as exc:
                # Порт занят = почти наверняка уже работает другой экземпляр
                # бота: молча работать «без дашборда» нельзя — иначе незаметно
                # плодятся дубли (общие логи/БД, гонки позиций).
                raise PipelineError(
                    f"веб-дашборд не смог занять порт "
                    f"{settings.dashboard_port} ({exc}) — похоже, бот уже "
                    "запущен другим процессом"
                ) from exc


    async def start_trading(self) -> None:
        """Запускает торговлю: сеть, слушатель, движок, консьюмер.

        Дашборд и БД продолжают работать и после :meth:`stop_trading` —
        кнопка «Старт бота» вызывает этот метод повторно. Открытые
        позиции остаются на месте и продолжают управляться.

        Raises:
            PipelineError: Если не удалось загрузить кошелёк.
        """
        if self._trading_active:
            return
        self._rpc = SolanaRpcClient(latency=self._latency)
        await self._rpc.start()
        self._ws = SolanaWsClient(latency=self._latency)
        self._resolver = TokenSecurityResolver(self._rpc)

        executor: TraderExecutor
        if self._dry_run:
            executor = DryRunExecutor(latency=self._latency)
        else:
            try:
                keypair = load_keypair()
            except Exception as exc:
                raise PipelineError(f"не удалось загрузить кошелёк: {exc}") from exc
            executor = OrderExecutor(self._rpc, keypair, latency=self._latency)
        await executor.start()
        self._executor = executor

        self._listener = PoolStreamListener(
            self._rpc,
            self._ws,
            latency=self._latency,
            on_pool_discovered=self._on_pool_discovered,
        )

        # Контур отбора: проверка каждого нового mint до торговых сигналов.
        self._screening = TokenScreeningService(
            self._resolver,
            store=self._store,
            on_rejected=self._on_screening_rejected,
        )
        self._screening.load_persisted()
        await self._screening.start()

        window = max(
            settings.entry_window_seconds, settings.exit_window_seconds
        )
        self._trader = MomentumTrader(
            executor=executor,
            portfolio=self._portfolio
            if self._portfolio is not None
            else PortfolioManager(),
            security_provider=self._screening.provider,
            indicators=StreamingIndicators(window),
            capital_sol=self._capital_sol,
        )

        self._stop_event.clear()
        try:
            await self._listener.start()
            await self._register_watchlist()
        except RpcError as exc:
            logger.critical(
                "Слушатель не смог стартовать ({}): бот продолжит без событий, "
                "переподключение в фоне",
                exc,
            )

        self._consume_task = asyncio.create_task(self._consume_loop())
        self._sweep_task = asyncio.create_task(self._sweep_loop())
        self._trading_active = True
        self._running = True
        self.stats = PipelineStats(started_at=time.time())
        self._last_maintenance = time.monotonic()
        logger.info(
            "Торговля запущена: mode={}, watchlist={}",
            "dry-run" if self._dry_run else "live",
            list(self._watchlist) or "-",
        )

    async def run(self) -> None:
        """Живёт до полной остановки процесса (Ctrl+C / request_stop / stop).

        Остановка торговли с дашборда (stop_trading) процесс не завершает —
        дашборд остаётся работать, «Старт бота» возобновляет торговлю.
        """
        await self.start()
        try:
            await self._fully_stopped.wait()
        finally:
            await self.stop()

    async def stop_trading(self) -> None:
        """Останавливает торговлю: сеть, слушатель, движок, консьюмер.

        Дашборд и БД продолжают работать. Открытые позиции сохраняются
        (память + БД), но пока торговля остановлена — без управления
        (TP/SL/стоп не срабатывают). Повторный вызов идемпотентен.
        """
        if not self._trading_active and self._consume_task is None:
            return
        self._trading_active = False
        self._running = False
        self._stop_event.set()

        if self._trader is not None and self._close_positions_on_stop:
            closed = self._trader.close_all()
            if closed:
                logger.info("Принудительно закрыто позиций: {}", len(closed))

        open_positions = (
            self._trader.portfolio.all_positions() if self._trader else []
        )
        if open_positions:
            logger.warning(
                "Торговля остановлена с открытыми позициями ({}): {} — "
                "без управления, пока не нажмёте «Старт бота»",
                len(open_positions),
                [position.base_mint for position in open_positions],
            )

        if self._consume_task is not None and not self._consume_task.done():
            self._consume_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consume_task
        self._consume_task = None
        if self._sweep_task is not None and not self._sweep_task.done():
            self._sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweep_task
        self._sweep_task = None

        if self._screening is not None:
            with contextlib.suppress(Exception):
                await self._screening.stop()
            self._screening = None
        if self._listener is not None:
            with contextlib.suppress(Exception):
                await self._listener.stop()
            self._listener = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        if self._executor is not None:
            with contextlib.suppress(Exception):
                await self._executor.close()
            self._executor = None
        self._trader = None
        self._resolver = None
        logger.info("Торговля остановлена: {}", self.stats.as_dict())

    async def stop(self) -> None:
        """Полная остановка процесса: торговля, дашборд, БД, RPC.

        Вызывается при завершении процесса (Ctrl+C, request_stop, stop()).
        Кнопка «Остановить бота» на дашборде использует stop_trading() —
        процесс и дашборд продолжают работать.
        """
        await self.stop_trading()
        if self._dashboard is not None:
            with contextlib.suppress(Exception):
                await self._dashboard.close()
            self._dashboard = None
        if self._store is not None:
            with contextlib.suppress(Exception):
                self._store.close()
            self._store = None
        if self._rpc is not None:
            with contextlib.suppress(Exception):
                await self._rpc.close()
            self._rpc = None
        self._fully_stopped.set()
        logger.info("Пайплайн остановлен: {}", self.stats.as_dict())
        logger.info("Латентность: {}", self._latency.summary())

    def request_stop(self) -> None:
        """Просит пайплайн полностью завершиться (сигнал/закрытие процесса)."""
        self._stop_event.set()
        self._fully_stopped.set()

    # ------------------------------------------------------------- Дашборд
    def dashboard_snapshot(self) -> dict[str, object]:
        """Собирает JSON-снимок состояния для веб-дашборда.

        Защищён от частично инициализированного пайплайна: отсутствующие
        компоненты дают пустые секции, а не исключение. Вызывается из
        event-loop дашборда — сетевых вызовов не делает.

        Returns:
            Словарь с секциями session/positions/stats/stream/screening/
            unrealized_sol/pnl_curve/logs.
        """
        now = time.time()
        trader = self._trader
        stats = trader.portfolio.stats if trader is not None else PortfolioStats()
        closed = trader.portfolio.closed_trades if trader is not None else ()

        positions: list[dict[str, object]] = []
        unrealized_total = 0.0
        if trader is not None:
            for pos in trader.portfolio.all_positions():
                price = trader.indicators.latest_price(pos.pool_address)
                if price <= 0:
                    price = pos.entry_price
                unrealized = pos.token_amount * (price - pos.entry_price)
                unrealized_total += unrealized
                positions.append(
                    {
                        "base_mint": pos.base_mint,
                        "stage": "кривая"
                        if pos.pool_address
                        and self._is_curve_pool(pos.pool_address)
                        else "AMM",
                        "entry_sol": pos.entry_sol,
                        "token_amount": pos.token_amount,
                        "entry_price": pos.entry_price,
                        "current_price": price,
                        "pnl_pct": pos.unrealized_pnl_pct(price),
                        "unrealized_sol": unrealized,
                        "peak_price": pos.peak_price,
                        "hold_seconds": max(0.0, now - pos.opened_at),
                        "tp1_taken": pos.tp1_taken,
                    }
                )

        best_pct = max(
            (trade.pnl_pct for trade in closed), default=0.0
        )
        worst_pct = min(
            (trade.pnl_pct for trade in closed), default=0.0
        )

        events = self.stats.events
        if self._rate_ts <= 0:
            self._rate_ts, self._rate_events = now, events
        elapsed = now - self._rate_ts
        if elapsed >= 2.0:
            self._events_per_sec = max(
                0.0, (events - self._rate_events) / elapsed
            )
            self._rate_ts, self._rate_events = now, events

        listener_stats = (
            self._listener.stats.as_dict() if self._listener is not None else {}
        )
        queue_size = self._listener.trades.qsize() if self._listener else 0
        screening_counts = (
            self._screening.counts()
            if self._screening is not None
            else {"pending": 0, "approved": 0, "rejected": 0}
        )
        rejections = (
            self._screening.recent_rejections(5)
            if self._screening is not None
            else []
        )

        return {
            "ts": now,
            "session": {
                "trading": "running" if self._trading_active else "stopped",
                "mode": "dry-run" if self._dry_run else "live",
                "network": settings.solana_network,
                "venue": settings.execution_venue,
                "uptime_s": max(0.0, now - self.stats.started_at)
                if self.stats.started_at
                else 0.0,
                "wallet": getattr(self._executor, "wallet", "—"),
                "db": self._db_path or "—",
                "positions_open": len(positions),
                "reserved_sol": getattr(trader.portfolio, "_reserved_sol", 0.0)
                if trader is not None
                else 0.0,
                "capital_sol": self._capital_sol,
            },
            "positions": positions,
            "unrealized_sol": unrealized_total,
            "stats": {
                "closed": stats.trades,
                "wins": stats.wins,
                "losses": stats.losses,
                "win_rate_pct": stats.win_rate_pct,
                "pnl_sol": stats.pnl_sol,
                "pnl_pct": stats.pnl_pct,
                "avg_hold_seconds": stats.avg_hold_seconds,
                "best_pct": best_pct,
                "worst_pct": worst_pct,
                "orders_ok": self.stats.orders,
                "orders_fail": self.stats.failures,
                "entries": self.stats.entries,
                "exits": self.stats.exits,
            },
            "stream": {
                "events": events,
                "events_per_sec": round(self._events_per_sec, 1),
                "buys": self.stats.buys,
                "sells": self.stats.sells,
                "pools": listener_stats.get("discovered_pools", 0),
                "queue_size": queue_size,
                "dropped": listener_stats.get("dropped_events", 0),
                "parse_errors": self.stats.parse_errors,
                "latency": {
                    name: {
                        "p50_ms": values.get("p50_ms", 0.0),
                        "p95_ms": values.get("p95_ms", 0.0),
                        "count": values.get("count", 0),
                    }
                    for name, values in self._latency.summary().items()
                },
            },
            "screening": {
                **screening_counts,
                "hold_reasons": self._top_hold_reasons(5),
                "recent_rejections": [
                    {
                        "base_mint": verdict.base_mint,
                        "reason": verdict.reason,
                    }
                    for verdict in rejections
                ],
            },
            "pnl_curve": self._pnl_curve[-300:],
            "logs": self._dashboard.log_tail() if self._dashboard else [],
        }

    # ------------------------------------------------------- Воронка причин HOLD
    _HOLD_REASON_BUCKETS: tuple[tuple[str, str], ...] = (
        ("надёжности", "окно надёжности (< 60 c)"),
        ("ликвидн", "ликвидность (MIN_REAL_POOL_SOL)"),
        ("volume velocity", "низкая Volume Velocity"),
        ("buy/sell", "низкий Buy/Sell Ratio"),
        ("безопасност", "безопасность токена"),
        ("импульс", "нет импульса"),
        ("пауза", "пауза после неуспешного ордера"),
        ("метрик", "нет метрик"),
    )

    def _count_hold_reason(self, reason: str) -> None:
        """Классифицирует причину HOLD и увеличивает её счётчик.

        Args:
            reason: Человекочитаемая причина из :class:`TradeDecision`.
        """
        low = reason.lower()
        for pattern, label in self._HOLD_REASON_BUCKETS:
            if pattern in low:
                self._hold_reasons[label] = self._hold_reasons.get(label, 0) + 1
                return
        self._hold_reasons["другое"] = self._hold_reasons.get("другое", 0) + 1

    def _top_hold_reasons(self, limit: int = 5) -> dict[str, int]:
        """Топ причин удержания (для блока «Отбор токенов»).

        Args:
            limit: Сколько причин вернуть.

        Returns:
            Словарь «причина → число событий», по убыванию счётчика.
        """
        if not self._hold_reasons:
            return {}
        top = sorted(self._hold_reasons.items(), key=lambda kv: -kv[1])[:limit]
        return dict(top)

    def _is_curve_pool(self, pool_address: str) -> bool:
        """Проверяет стадию пула в реестре (кривая или AMM).

        Args:
            pool_address: Адрес пула.

        Returns:
            ``True`` для bonding curve (lp_mint отсутствует).
        """
        if self._listener is None:
            return True
        pool = self._listener.registry.get(pool_address)
        return pool is None or pool.lp_mint is None

    async def _pool_for_mint(
        self, mint: str
    ) -> tuple[PoolInfo | None, str | None, str | None]:
        """Возвращает (pool_info, creator, ошибка) для ручного действия.

        Пул позиции мог отсутствовать в реестре (рестарт процесса, чистка
        «молчащих» пулов) — тогда он восстанавливается с сети: кривая по
        mint + резервы через RPC. При нулевых резервах у найденного пула
        состояние обновляется тем же путём.

        Args:
            mint: Mint мемкоина.

        Returns:
            Кортеж: снимок пула, кошелёк создателя, текст ошибки (если есть).
        """
        trader = self._trader
        if trader is None:
            return None, None, "пайплайн не запущен"
        position = trader.portfolio.get(mint)
        if position is None:
            return None, None, f"позиция {mint} не найдена"
        listener = self._listener
        if listener is None:
            return None, None, "слушатель недоступен"
        tracked = listener.registry.get(position.pool_address)
        if tracked is None:
            try:
                tracked = await listener.register_pool_from_mint(mint)
            except RpcError as exc:
                tracked = None
                logger.debug(
                    "Восстановление пула {} не удалось: {}", mint, exc
                )
            if tracked is None:
                return (
                    None,
                    None,
                    (
                        f"пул {position.pool_address} недоступен: не найден в "
                        "реестре и не восстановлен с сети — токен мог "
                        "мигрировать на AMM"
                    ),
                )
        if tracked.quote_reserve <= 0 or tracked.base_reserve <= 0:
            try:
                await listener.refresh_pool_reserves(tracked)
            except RpcError as exc:
                logger.debug(
                    "Обновление резервов {} не удалось: {}", mint, exc
                )
        if tracked.quote_reserve <= 0 or tracked.base_reserve <= 0:
            return (
                None,
                None,
                (
                    "резервы пула неизвестны (RPC не вернул состояние аккаунта) — "
                    "повторите попытку позже"
                ),
            )
        return tracked.to_pool_info(), tracked.creator, None


    async def _dashboard_set_mode(self, target: str) -> dict[str, object]:
        """Переключает режим исполнения (dry/live) с перезапуском торговли.

        Безопасность: при открытых позициях — отказ (сначала «ПРОДАТЬ ВСЁ»,
        иначе бумажные и живые позиции смешались бы в учёте); переход в LIVE
        требует настроенного кошелька. Хранилище подменяется на БД целевого
        режима, чтобы статистика режимов не смешивалась.

        Args:
            target: ``dry`` или ``live``.

        Returns:
            ``{ok, mode}`` или ``{ok: False, error}``.
        """
        if target not in ("dry", "live"):
            return {"ok": False, "error": f"неизвестный режим: {target}"}
        if self._portfolio is not None and self._portfolio.open_count > 0:
            return {
                "ok": False,
                "error": (
                    f"сначала закройте открытые позиции ({self._portfolio.open_count} шт.) "
                    "кнопкой «ПРОДАТЬ ВСЁ»"
                ),
            }
        if target == "live":
            try:
                load_keypair()
            except Exception as exc:
                return {"ok": False, "error": f"кошелёк не настроен: {exc}"}

        was_active = self._trading_active
        if was_active:
            await self.stop_trading()

        if self._store is not None:
            with contextlib.suppress(Exception):
                self._store.close()
            self._store = None
        self._store = PortfolioStore(
            settings.portfolio_dry_db_path
            if target == "dry"
            else settings.portfolio_db_path
        )
        if self._portfolio is not None:
            self._portfolio.set_store(self._store)
            self._portfolio.load_open_positions()

        self._dry_run = target == "dry"
        if was_active:
            await self.start_trading()
        logger.warning(
            "Режим исполнения переключён: {}",
            "DRY-RUN" if self._dry_run else "LIVE",
        )
        return {
            "ok": True,
            "mode": "dry" if self._dry_run else "live",
            "trading": "running" if was_active else "stopped",
        }

    async def _dashboard_bot_lifecycle(
        self, action: str, payload: dict[str, Any]
    ) -> dict[str, object] | None:
        """Старт/Останов/Смена режима; ``None`` — действие не lifecycle-овое.

        Args:
            action: Имя действия.
            payload: Параметры (для ``bot_mode`` — целевой режим).

        Returns:
            Словарь результата для lifecycle-действий или ``None``.
        """
        if action == "bot_stop":
            if not self._trading_active:
                return {"ok": False, "error": "торговля уже остановлена"}
            await self.stop_trading()
            return {"ok": True, "state": "stopped"}
        if action == "bot_start":
            if self._trading_active:
                return {"ok": False, "error": "бот уже работает"}
            try:
                await self.start_trading()
            except PipelineError as exc:
                return {"ok": False, "error": str(exc)}
            return {"ok": True, "state": "running"}
        if action == "bot_mode":
            target = str(payload.get("mode", "")).strip()
            return await self._dashboard_set_mode(target)
        return None

    async def _dashboard_action(self, payload: dict[str, object]) -> dict[str, object]:
        """Обрабатывает ручные действия дашборда (кнопки сделок).

        Любое исключение превращается в ``{ok: False, error}`` — страница
        никогда не получает 500 без тела.
        """
        try:
            return await self._dashboard_action_inner(payload)
        except Exception as exc:
            logger.exception("Ошибка ручного действия дашборда")
            return {"ok": False, "error": f"внутренняя ошибка: {exc}"}

    async def _sell_all_positions(
        self, trader: MomentumTrader
    ) -> dict[str, object]:
        """Закрывает все открытые позиции (с fallback учётного закрытия).

        Args:
            trader: Движок с портфелем.

        Returns:
            Сводный результат по каждой позиции.
        """
        results: list[dict[str, object]] = []
        for position in trader.portfolio.all_positions():
            pool, creator, err = await self._pool_for_mint(position.base_mint)
            if err is not None or pool is None:
                if self._dry_run:
                    # Бумажная позиция, пул недоступен — учётное закрытие.
                    forced = trader.portfolio.force_close(
                        position.base_mint,
                        exit_price=position.entry_price,
                        exit_reason="manual_no_pool",
                    )
                    results.append(
                        {
                            "mint": position.base_mint,
                            "ok": True,
                            "forced": True,
                            "closed": forced is not None,
                            "note": f"закрыто учётно ({err})",
                        }
                    )
                else:
                    results.append(
                        {
                            "mint": position.base_mint,
                            "ok": False,
                            "error": err or "пул недоступен",
                        }
                    )
                continue
            outcome = await trader.manual_sell(
                position.base_mint, pool, creator=creator
            )
            if not outcome.get("ok") and self._dry_run:
                # Бумажная позиция, продать нечем (пул недоступен) —
                # закрываем учётно: реальных токенов нет, честно.
                forced = trader.portfolio.force_close(
                    position.base_mint,
                    exit_price=position.entry_price,
                    exit_reason="manual_no_pool",
                )
                outcome = {
                    "ok": True,
                    "forced": True,
                    "closed": forced is not None,
                    "note": "закрыто учётно (пул недоступен, dry-run)",
                }
            results.append({"mint": position.base_mint, **outcome})
        ok_all = bool(results) and all(item["ok"] for item in results)
        if any(item.get("closed") for item in results):
            self._pnl_curve.append((time.time(), trader.portfolio.stats.pnl_sol))
        return {
            "ok": ok_all,
            "results": results,
            "error": None if ok_all else "не все позиции закрыты",
        }

    async def _dashboard_action_inner(
        self, payload: dict[str, object]
    ) -> dict[str, object]:
        """Основная логика ручных действий (см. _dashboard_action).

        ``sell`` — закрыть позицию целиком; ``average`` — усредниться
        (докупить ``TRADE_AMOUNT_SOL``); ``sell_all`` — закрыть все позиции
        подряд. Запись в БД выполняется тем же write-through портфеля:
        закрытие удаляет строку из ``positions`` и пишет ``closed_trades``,
        усреднение делает UPSERT позиции — «зависших» строк не появляется.

        Args:
            payload: ``{action, mint?}``.

        Returns:
            Словарь результата (``ok`` и детали/ошибка).
        """
        action = str(payload.get("action", "")).strip()
        mint = str(payload.get("mint", "")).strip()
        lifecycle = await self._dashboard_bot_lifecycle(action, payload)
        if lifecycle is not None:
            return lifecycle

        if action not in ("sell", "average", "sell_all"):
            return {"ok": False, "error": f"неизвестное действие: {action or 'пусто'}"}

        trader = self._trader
        if trader is None:
            return {"ok": False, "error": "пайплайн не запущен"}

        if action == "sell_all":
            return await self._sell_all_positions(trader)

        # Оставшиеся действия: sell / average (по одному mint).
        if not mint:
            return {"ok": False, "error": "не указан mint"}
        pool, creator, err = await self._pool_for_mint(mint)
        if err is not None or pool is None:
            if not (action == "sell" and self._dry_run):
                return {"ok": False, "error": err or "пул недоступен"}
            # dry-run: бумажная позиция, пул недоступен — учётное закрытие
            position = trader.portfolio.get(mint)
            if position is not None:
                trader.portfolio.force_close(
                    mint,
                    exit_price=position.entry_price,
                    exit_reason="manual_no_pool",
                )
                return {"ok": True, "forced": True, "note": f"закрыто учётно ({err})"}
            return {"ok": False, "error": err}
        if action == "sell":
            outcome = await trader.manual_sell(mint, pool, creator=creator)
            outcome = self._force_close_if_stale(outcome, mint, trader)
        else:
            outcome = await trader.manual_buy(mint, pool, creator=creator)
        if outcome.get("ok") and action == "sell":
            self._pnl_curve.append((time.time(), trader.portfolio.stats.pnl_sol))
        return outcome

    def _force_close_if_stale(
        self,
        outcome: dict[str, object],
        mint: str,
        trader: MomentumTrader,
    ) -> dict[str, object]:
        """Fallback dry-run: учётное закрытие бумажной позиции, если продать
        нечем (пул недоступен/не восстановлен). В live — outcome без изменений.

        Args:
            outcome: Результат попытки продажи.
            mint: Mint мемкоина.
            trader: Движок с портфелем.

        Returns:
            Исходный outcome при успехе; иначе учётное закрытие.
        """
        if outcome.get("ok") or not self._dry_run:
            return outcome
        position = trader.portfolio.get(mint)
        if position is None:
            return outcome
        trader.portfolio.force_close(
            mint, exit_price=position.entry_price, exit_reason="manual_no_pool"
        )
        return {
            "ok": True,
            "forced": True,
            "note": "закрыто учётно (пул недоступен, dry-run)",
        }

    # ------------------------------------------------- Свип устаревших позиций
    async def _sweep_loop(self) -> None:
        """Раз в 5 c закрывает позиции, удерживаемые дольше MAX_HOLD_SECONDS.

        Событийный выход по таймауту (стратегия) не срабатывает по мёртвым
        токенам — событий по ним больше нет, и поза зависает навсегда,
        занимая слоты входов. Свип гарантирует закрытие: попытка продажи,
        при неудаче — учётное закрытие полным убытком.
        """
        while True:
            await asyncio.sleep(5.0)
            trader = self._trader
            if trader is None or not self._trading_active:
                continue
            try:
                await self._sweep_stale_positions(trader)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ошибка свипа устаревших позиций")

    async def _sweep_stale_positions(self, trader: MomentumTrader) -> None:
        """Закрывает позиции старше ``max_hold_seconds``.

        Сначала обычная продажа через лестницу; если продать нечем
        (пул мёртв/недоступен) — учётное закрытие по нулевой цене, т.е.
        полный убыток в размере вложенного SOL (write-through: строка
        открытой позиции удаляется из БД, пишется запись в closed_trades).

        Args:
            trader: Движок с портфелем открытых позиций.
        """
        now = time.time()
        for position in trader.portfolio.all_positions():
            if now - position.opened_at <= settings.max_hold_seconds:
                continue
            mint = position.base_mint
            age = now - position.opened_at
            pool, creator, err = await self._pool_for_mint(mint)
            if pool is not None:
                outcome = await trader.manual_sell(
                    mint, pool, creator=creator, exit_reason="max_hold"
                )
                if outcome.get("ok"):
                    self._pnl_curve.append(
                        (time.time(), trader.portfolio.stats.pnl_sol)
                    )
                    logger.info(
                        "Свип: позиция {} закрыта по MAX_HOLD ({:.0f} c)", mint, age
                    )
                    continue
                error = str(outcome.get("error", ""))
            else:
                error = err or "пул недоступен"
            logger.warning(
                "Свип: поза {} держится {:.0f} c > MAX_HOLD, продажа не удалась "
                "({}) — закрытие убытком",
                mint, age, error,
            )
            if not self._dry_run:
                logger.critical(
                    "LIVE: токены {} могли остаться в кошельке — позиция "
                    "закрыта учётно (убыток учтён в статистике)",
                    mint,
                )
            trader.portfolio.force_close(
                mint, exit_price=0.0, exit_reason="max_hold_unsellable"
            )
            self._pnl_curve.append((time.time(), trader.portfolio.stats.pnl_sol))

    # ------------------------------------------------------------- Бэктест
    @staticmethod
    def _extract_mint(raw: str) -> str | None:
        """Извлекает mint из ссылки pump.fun или сырого base58-адреса.

        Args:
            raw: URL вида ``https://pump.fun/coin/<MINT>`` или сам адрес.

        Returns:
            Кандидат в mint (base58, 32-44 символа) или ``None``.
        """
        match = re.search(r"[1-9A-HJ-NP-Za-km-z]{32,44}", raw or "")
        return match.group(0) if match else None

    def _backtest_sample_csv(self) -> str:
        """Готовит демо-CSV: импульс, вход, каскад TP на пуле 90 SOL.

        Returns:
            Текст CSV канонического формата.
        """
        events: list[TradeEvent] = []
        seeds = ((1.0, 4_000_000), (1.2, 4_000_000), (1.4, 4_000_000), (1.6, 4_000_000))
        pushes = ((1.8, 36_000_000), (2.0, 30_000_000), (2.2, 25_000_000))
        for index, (timestamp, tokens) in enumerate(seeds):
            events.append(
                TradeEvent(
                    signature=f"demo-seed-{index}",
                    pool_address="DemoPool1111111111111111111111111111111",
                    base_mint="DemoMint11111111111111111111111111111111",
                    dex=DEXType.PUMP_FUN,
                    side=TradeSide.BUY,
                    sol_amount=0.6,
                    token_amount=tokens,
                    wallet="DemoUser111111111111111111111111111111111",
                    timestamp=timestamp,
                )
            )
        for index, (timestamp, tokens) in enumerate(pushes):
            events.append(
                TradeEvent(
                    signature=f"demo-push-{index}",
                    pool_address="DemoPool1111111111111111111111111111111",
                    base_mint="DemoMint11111111111111111111111111111111",
                    dex=DEXType.PUMP_FUN,
                    side=TradeSide.BUY,
                    sol_amount=8.0,
                    token_amount=tokens,
                    wallet="DemoUser111111111111111111111111111111111",
                    timestamp=timestamp,
                )
            )
        return events_to_csv_text(events)

    async def _backtest_events(
        self,
        events: list[TradeEvent],
        *,
        quote_reserve: int,
        base_reserve: int,
        creator: str | None,
        trade_amount_sol: float,
        entry_window: float,
        exit_window: float,
        max_positions: int,
        cancel_event: asyncio.Event | None = None,
    ) -> dict[str, object]:
        """Прогоняет события через живую стратегию и возвращает отчёт.

        Args:
            events: События сделок (хронологически).
            quote_reserve: Начальный виртуальный резерв SOL, лампорты.
            base_reserve: Начальный резерв токенов, base units.
            creator: Кошелёк создателя (для прямого маршрута).
            trade_amount_sol: Размер входа, SOL.
            entry_window: Окно входа, c.
            exit_window: Окно выхода, c.
            max_positions: Максимум одновременных позиций.

        Returns:
            Отчёт :meth:`BacktestReport.to_dict`.
        """
        first = events[0]
        scenario = ReplayScenario(
            pool=ReplayPoolConfig(
                pool_address=first.pool_address,
                base_mint=first.base_mint,
                dex=first.dex,
                quote_reserve=quote_reserve,
                base_reserve=base_reserve,
                base_decimals=PUMP_TOKEN_DECIMALS,
                quote_decimals=SOL_DECIMALS,
                creator=creator,
            ),
            events=tuple(events),
            security=TokenSecurity(
                mint_authority=None, freeze_authority=None, lp_burned_pct=100.0
            ),
        )
        backtester = Backtester(
            trade_amount_sol=trade_amount_sol,
            max_concurrent_positions=max_positions,
            entry_window_seconds=entry_window,
            exit_window_seconds=exit_window,
            cancel_event=cancel_event,
        )
        report = await backtester.run(scenario)
        report_dict = report.to_dict()
        report_dict["stopped"] = backtester.was_cancelled
        return {"ok": True, "report": report_dict}

    async def _dashboard_backtest(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Запускает бэктест по запросу из дашборда.

        Режимы: ``sample`` — вернуть демо-CSV; ``csv`` — прогнать текст CSV;
        ``mint`` — скачать историю сделок bonding curve через Helius
        (getSignaturesForAddress + getTransaction), разобрать настоящие
        TradeEvent-логи и прогнать тест. История берётся ограниченной
        порцией (``max_tx``), начальное состояние пула сеется виртуальными
        резервами самого раннего события.

        Args:
            payload: Параметры режима (см. форму в HTML).

        Returns:
            ``{ok, report}`` / ``{ok, csv}`` / ``{ok, error}``.
        """
        # Любой сбой (нет DNS до Helius, таймауты, битые данные) должен
        # приходить на страницу JSON-ом с ошибкой, а не 500 без тела.
        try:
            return await self._dashboard_backtest_inner(payload)
        except RpcError as exc:
            return {
                "ok": False,
                "error": f"Helius RPC недоступен: {exc} — проверьте DNS/сеть и HELIUS_API_KEY",
            }
        except TimeoutError:
            return {
                "ok": False,
                "error": "Helius RPC не ответил вовремя (таймаут) — повторите позже",
            }
        except ReplayError as exc:
            return {"ok": False, "error": f"реплей не удался: {exc}"}
        except Exception as exc:
            logger.exception("Ошибка бэктеста")
            return {"ok": False, "error": f"внутренняя ошибка бэктеста: {exc}"}

    async def _dashboard_backtest_inner(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Основная логика режимов бэктеста (см. _dashboard_backtest)."""
        mode = str(payload.get("mode", "csv")).strip()
        if mode == "stop":
            run_id = str(payload.get("run_id", ""))
            if self._backtest_stop is None or run_id != self._backtest_run_id:
                return {"ok": False, "error": "активного запуска бэктеста нет"}
            self._backtest_stop.set()
            return {"ok": True}
        run_id = str(payload.get("run_id", "")) or str(time.time_ns())
        self._backtest_stop = asyncio.Event()
        self._backtest_run_id = run_id

        try:
            trade_amount_sol = float(
                payload.get("trade_amount_sol", settings.trade_amount_sol)
            )
            entry_window = float(
                payload.get("entry_window_seconds", settings.entry_window_seconds)
            )
            exit_window = float(
                payload.get("exit_window_seconds", settings.exit_window_seconds)
            )
            max_positions = int(payload.get("max_positions", 1))
        except (TypeError, ValueError):
            return {"ok": False, "error": "числовые параметры заданы неверно"}

        if mode == "sample":
            return {"ok": True, "csv": self._backtest_sample_csv()}

        if mode == "csv":
            text = str(payload.get("csv", "")).strip()
            if not text:
                return {"ok": False, "error": "CSV пуст"}
            try:
                events = parse_events_csv_text(text)
            except Exception as exc:
                return {"ok": False, "error": f"CSV не разобран: {exc}"}
            if len(events) < 5:
                return {"ok": False, "error": f"мало событий: {len(events)} < 5"}
            events.sort(key=lambda e: e.timestamp)
            try:
                quote_reserve = int(
                    float(payload.get("initial_sol", 90.0)) * LAMPORTS_PER_SOL
                )
                base_reserve = int(
                    float(payload.get("initial_token", 600_000_000.0))
                    * 10**PUMP_TOKEN_DECIMALS
                )
            except (TypeError, ValueError):
                return {"ok": False, "error": "начальные резервы заданы неверно"}
            return await self._backtest_events(
                events,
                quote_reserve=quote_reserve,
                base_reserve=base_reserve,
                creator=str(payload.get("creator") or "") or None,
                trade_amount_sol=trade_amount_sol,
                entry_window=entry_window,
                exit_window=exit_window,
                max_positions=max_positions,
                cancel_event=self._backtest_stop,
            )

        if mode == "mint":
            return await self._backtest_from_mint(
                payload,
                trade_amount_sol=trade_amount_sol,
                entry_window=entry_window,
                exit_window=exit_window,
                max_positions=max_positions,
            )

        return {"ok": False, "error": f"неизвестный режим бэктеста: {mode or 'пусто'}"}

    async def _backtest_from_mint(
        self,
        payload: dict[str, Any],
        *,
        trade_amount_sol: float,
        entry_window: float,
        exit_window: float,
        max_positions: int,
    ) -> dict[str, object]:
        """Бэктест по ссылке на монету: история кривой через Helius.

        Args:
            payload: ``{url, max_tx?}``.
            trade_amount_sol: Размер входа, SOL.
            entry_window: Окно входа, c.
            exit_window: Окно выхода, c.
            max_positions: Максимум одновременных позиций.

        Returns:
            ``{ok, report}`` или ``{ok, error}``.
        """
        mint = self._extract_mint(str(payload.get("url", "")))
        if mint is None:
            return {"ok": False, "error": "не удалось извлечь mint из ссылки"}
        rpc = self._rpc
        if rpc is None:
            return {"ok": False, "error": "пайплайн не запущен"}
        try:
            curve_address = str(derive_bonding_curve(mint))
        except Exception as exc:
            return {"ok": False, "error": f"не удалось вывести bonding curve: {exc}"}
        limit = min(1000, max(50, int(payload.get("max_tx", 500))))
        fallback_client: SolanaRpcClient | None = None
        try:
            try:
                signatures = await rpc.get_signatures_for_address(
                    curve_address, limit=limit
                )
            except RpcError as exc:
                # Helius недоступен/не настроен — пробуем публичный Solana RPC
                # (работает без ключа, но с жёсткими rate limits).
                logger.warning(
                    "Helius RPC не ответил ({}), переключаюсь на публичный RPC",
                    exc,
                )
                fallback_client = SolanaRpcClient(http_url=PUBLIC_MAINNET_RPC)
                await fallback_client.start()
                rpc = fallback_client
                signatures = await rpc.get_signatures_for_address(
                    curve_address, limit=limit
                )

            # Параллельная загрузка: 500 транзакций последовательно = минуты.
            semaphore = asyncio.Semaphore(8)

            async def _fetch(info: dict[str, Any]) -> dict[str, Any] | None:
                async with semaphore:
                    try:
                        return await rpc.get_transaction(
                            str(info.get("signature", ""))
                        )
                    except RpcError as tx_exc:
                        logger.debug("getTransaction не удался: {}", tx_exc)
                        return None

            transactions = await asyncio.gather(
                *(_fetch(info) for info in signatures)
            )
            entries: list[tuple[float, str, PumpTradeEvent]] = []
            for info, tx in zip(signatures, transactions, strict=True):
                if not tx:
                    continue
                signature = str(info.get("signature", ""))
                logs = (tx.get("meta") or {}).get("logMessages") or []
                block_time = float(tx.get("blockTime") or 0)
                for parsed in extract_pump_trade_events(logs):
                    entries.append((block_time, signature, parsed))
            if len(entries) < 5:
                return {
                    "ok": False,
                    "error": (
                        f"мало событий на кривой: {len(entries)} < 5 "
                        "(токен мог мигрировать на AMM — нужен Jupiter-режим)"
                    ),
                }
            entries.sort(key=lambda item: item[0])
            first_time, _first_sig, first_parsed = entries[0]
            events = [
                TradeEvent(
                    signature=signature,
                    pool_address=curve_address,
                    base_mint=mint,
                    dex=DEXType.PUMP_FUN,
                    side=TradeSide.BUY if parsed.is_buy else TradeSide.SELL,
                    sol_amount=parsed.sol_amount / 10**SOL_DECIMALS,
                    token_amount=parsed.token_amount
                    / float(10**PUMP_TOKEN_DECIMALS),
                    wallet=parsed.user,
                    timestamp=block_time
                    if block_time > 0
                    else first_time + index * 0.001,
                )
                for index, (block_time, signature, parsed) in enumerate(
                    entries[1:], start=1
                )
            ]
            return await self._backtest_events(
                events,
                quote_reserve=first_parsed.virtual_sol_reserves,
                base_reserve=first_parsed.virtual_token_reserves,
                creator=first_parsed.user,
                trade_amount_sol=trade_amount_sol,
                entry_window=entry_window,
                exit_window=exit_window,
                max_positions=max_positions,
                cancel_event=self._backtest_stop,
            )
        finally:
            if fallback_client is not None:
                await fallback_client.close()

    async def _dashboard_shutdown(self) -> None:
        """Останавливает торговлю по запросу с дашборда (процесс живёт).

        Подписки закрываются — расход кредитов Helius прекращается.
        Дашборд остаётся работать; «Старт бота» возобновляет торговлю.

        """
        logger.warning("Остановка торговли: запрос с веб-дашборда")
        await self.stop_trading()

    # -------------------------------------------------------------- Внутреннее
    async def _register_watchlist(self) -> None:
        """Регистрирует пулы из watchlist на старте."""
        if not self._watchlist or self._listener is None:
            return
        for mint in self._watchlist:
            pool = await self._listener.register_pool_from_mint(mint)
            if pool is None:
                logger.warning("Пул для mint {} не найден — пропуск", mint)
                continue
            logger.info(
                "Watchlist: {} → {} ({})",
                mint,
                pool.pool_address,
                pool.dex.value,
            )

    async def _consume_loop(self) -> None:
        """Читает очередь событий, обслуживая пайплайн по таймауту."""
        listener = self.listener
        while not self._stop_event.is_set():
            try:
                event = await asyncio.wait_for(
                    listener.trades.get(), timeout=QUEUE_POLL_TIMEOUT
                )
            except TimeoutError:
                await self._maintenance()
                continue
            except asyncio.CancelledError:
                raise
            try:
                await self._handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.stats.parse_errors += 1
                logger.exception("Ошибка обработки события")

    async def _handle_event(self, event: TradeEvent) -> None:
        """Передаёт событие в движок решений.

        Args:
            event: Событие сделки из очереди.
        """
        self.stats.events += 1
        if event.side == TradeSide.BUY:
            self.stats.buys += 1
        else:
            self.stats.sells += 1
        pool: TrackedPool | None = self.listener.registry.get(event.pool_address)
        if pool is None:
            return
        if not pool.is_ready:
            return

        decision = await self.trader.process_event(
            event, pool.to_pool_info(), creator=pool.creator
        )
        if decision.action == SignalAction.HOLD:
            self._count_hold_reason(decision.reason)

        if decision.order is not None:
            if decision.order.success:
                self.stats.orders += 1
            else:
                self.stats.failures += 1
        if decision.executed and decision.action == SignalAction.ENTER:
            self.stats.entries += 1
        if decision.executed and decision.action == SignalAction.EXIT:
            self.stats.exits += 1
        if decision.closed is not None:
            self._pnl_curve.append(
                (time.time(), self.trader.portfolio.stats.pnl_sol)
            )
            logger.info(
                "Сделка закрыта: {} ({}) PnL={:+.4f} SOL",
                decision.closed.base_mint,
                decision.closed.exit_reason,
                decision.closed.pnl_sol,
            )

    async def _maintenance(self) -> None:
        """Периодическая очистка реестра и сводка состояния."""
        now = time.monotonic()
        if now - self._last_maintenance < self._maintenance_interval:
            return
        self._last_maintenance = now

        pruned = self.listener.registry.prune_stale(settings.stale_pool_seconds)
        for pool_address in pruned:
            self.trader.forget_pool(pool_address)
            try:
                await self.listener.unsubscribe_pool(pool_address)
            except RpcError as exc:
                logger.debug("Отписка пула {} не удалась: {}", pool_address, exc)
        if pruned:
            logger.info("Удалено «молчащих» пулов: {}", len(pruned))

        logger.info(
            "Сводка: pipeline={}, portfolio={}",
            self.stats.as_dict(),
            self.trader.portfolio.summary(),
        )

    def _on_pool_discovered(self, pool: TrackedPool) -> None:
        """Колбэк слушателя на обнаружение нового пула.

        Args:
            pool: Зарегистрированный пул.
        """
        if self._screening is not None:
            self._screening.submit(pool.to_pool_info())
        logger.info(
            "Новый пул в работе: {} ({}, mint {})",
            pool.pool_address,
            pool.dex.value,
            pool.base_mint,
        )

    async def _on_screening_rejected(self, verdict: ScreeningVerdict) -> None:
        """Реакция контура отбора на отклонённый токен.

        Пул отклонённого mint вычищается из реестра и индикаторов, чтобы
        события по нему больше не обрабатывались.

        Args:
            verdict: Отклоняющий вердикт.
        """
        listener = self._listener
        if listener is None:
            return
        pool = listener.registry.get_by_mint(verdict.base_mint)
        if pool is None:
            return
        listener.registry.remove(pool.pool_address)
        if self._trader is not None:
            self._trader.forget_pool(pool.pool_address)
        logger.warning(
            "Пул {} удалён: токен {} отклонён отбором ({})",
            pool.pool_address,
            verdict.base_mint,
            verdict.reason,
        )
