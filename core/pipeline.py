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
import time
from collections.abc import Sequence
from dataclasses import dataclass

from analytics.indicators import StreamingIndicators
from config.settings import settings
from core.client import SolanaRpcClient, SolanaWsClient
from core.dry_run import DryRunExecutor
from core.executor import OrderExecutor
from core.listener import PoolStreamListener
from core.orders import TraderExecutor
from core.persistence import PortfolioStore
from core.pool_registry import TrackedPool
from core.portfolio import PortfolioManager
from core.screening import ScreeningVerdict, TokenScreeningService
from core.security import TokenSecurityResolver
from core.trader import MomentumTrader
from models.trade_data import SignalAction, TradeEvent
from utils.latency import LatencyRegistry
from utils.logger import get_logger
from utils.spl import load_keypair

logger = get_logger()

#: Таймаут ожидания события из очереди перед обслуживанием пайплайна, c.
QUEUE_POLL_TIMEOUT: float = 1.0

#: Возраст «молчания» пула, после которого он удаляется из реестра, c.
STALE_POOL_SECONDS: float = 600.0

#: Отдельная БД для dry-run, чтобы бумажные позиции не смешивались с боевыми.
DRY_RUN_DB_PATH: str = "data/bot_dry.db"


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
        max_concurrent_positions: int | None = None,
        capital_sol: float | None = None,
        close_positions_on_stop: bool = False,
        maintenance_interval_seconds: float = 60.0,
        db_path: str | None = None,
    ) -> None:
        """Инициализирует пайплайн (без сетевых вызовов).

        Args:
            dry_run: Не отправлять транзакции (бумажное исполнение).
            watchlist: Mint-адреса, которые нужно отслеживать с момента старта.
            max_concurrent_positions: Максимум одновременно открытых позиций
                (``None`` — из настроек).
            capital_sol: Ограничение свободного капитала, SOL (``None`` — из
                настроек).
            close_positions_on_stop: Принудительно продавать позиции при
                завершении работы.
            maintenance_interval_seconds: Интервал обслуживания (очистка
                «молчащих» пулов, сводка в лог), c.
            db_path: Путь к SQLite-БД позиций (``None`` — из настроек;
                dry-run по умолчанию пишет в отдельный файл).
        """
        self._dry_run = dry_run
        self._watchlist = tuple(watchlist)
        self._max_concurrent_positions = max(
            1,
            settings.max_concurrent_positions
            if max_concurrent_positions is None
            else max_concurrent_positions,
        )
        self._capital_sol = (
            settings.capital_sol if capital_sol is None else capital_sol
        )
        self._close_positions_on_stop = close_positions_on_stop
        self._maintenance_interval = max(5.0, maintenance_interval_seconds)
        if db_path is not None:
            self._db_path = db_path
        elif dry_run:
            self._db_path = DRY_RUN_DB_PATH
        else:
            self._db_path = settings.portfolio_db_path

        self._latency = LatencyRegistry()
        self._rpc: SolanaRpcClient | None = None
        self._ws: SolanaWsClient | None = None
        self._listener: PoolStreamListener | None = None
        self._resolver: TokenSecurityResolver | None = None
        self._screening: TokenScreeningService | None = None
        self._executor: TraderExecutor | None = None
        self._trader: MomentumTrader | None = None
        self._store: PortfolioStore | None = None

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
        if self._running:
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

        window = max(
            settings.entry_window_seconds, settings.exit_window_seconds
        )
        if self._db_path:
            self._store = PortfolioStore(self._db_path)

        # Контур отбора: проверка каждого нового mint до торговых сигналов.
        self._screening = TokenScreeningService(
            self._resolver,
            store=self._store,
            on_rejected=self._on_screening_rejected,
        )
        self._screening.load_persisted()
        await self._screening.start()

        portfolio = PortfolioManager(
            max_concurrent_positions=self._max_concurrent_positions,
            store=self._store,
        )
        restored = portfolio.load_open_positions()
        if restored:
            logger.warning(
                "Рестарт с открытыми позициями: лимит {}, занято {} — новые "
                "входы ограничены, пока восстановленные позиции не закроются",
                self._max_concurrent_positions,
                portfolio.open_count,
            )
        self._trader = MomentumTrader(
            executor=executor,
            portfolio=portfolio,
            security_provider=self._screening.provider,
            indicators=StreamingIndicators(window),
            capital_sol=self._capital_sol,
        )

        await self._listener.start()
        await self._register_watchlist()

        self.stats = PipelineStats(started_at=time.time())
        self._last_maintenance = time.monotonic()
        self._running = True
        logger.info(
            "Пайплайн запущен: mode={}, positions={}, watchlist={}",
            "dry-run" if self._dry_run else "live",
            self._max_concurrent_positions,
            list(self._watchlist) or "-",
        )

    async def run(self) -> None:
        """Запускает пайплайн и обрабатывает поток до остановки."""
        await self.start()
        try:
            await self._consume_loop()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Останавливает пайплайн и освобождает сетевые ресурсы."""
        if not self._running and self._listener is None:
            return
        self._running = False
        self._stop_event.set()

        if self._trader is not None and self._close_positions_on_stop:
            closed = self._trader.close_all()
            if closed:
                logger.info("Принудительно закрыто позиций: {}", len(closed))

        if self._screening is not None:
            with contextlib.suppress(Exception):
                await self._screening.stop()
            self._screening = None

        open_positions = (
            self._trader.portfolio.all_positions() if self._trader else []
        )
        if open_positions:
            logger.warning(
                "Остались открытые позиции ({}): {}",
                len(open_positions),
                [position.base_mint for position in open_positions],
            )

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
        if self._store is not None:
            with contextlib.suppress(Exception):
                self._store.close()
            self._store = None
        if self._rpc is not None:
            with contextlib.suppress(Exception):
                await self._rpc.close()
            self._rpc = None

        logger.info("Пайплайн остановлен: {}", self.stats.as_dict())
        logger.info("Латентность: {}", self._latency.summary())

    def request_stop(self) -> None:
        """Просит пайплайн завершиться после текущего события."""
        self._stop_event.set()

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
        pool: TrackedPool | None = self.listener.registry.get(event.pool_address)
        if pool is None:
            return
        if not pool.is_ready:
            return

        decision = await self.trader.process_event(
            event, pool.to_pool_info(), creator=pool.creator
        )

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

        pruned = self.listener.registry.prune_stale(STALE_POOL_SECONDS)
        for pool_address in pruned:
            self.trader.forget_pool(pool_address)
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
