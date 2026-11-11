"""Движок торговых решений: событие сделки → фильтры → стратегия → ордер.

Модуль — единственное место, где сходятся все слои принятия решения. Он не
знает, откуда пришло событие (WebSocket листенера или CSV бэктестера) и как
именно исполняется ордер (Jito-бандл или dry-run), — оба конца подключаются
инъекцией зависимостей:

* :class:`~core.orders.TraderExecutor` — исполнитель
  (:class:`~core.executor.OrderExecutor` либо
  :class:`~core.dry_run.DryRunExecutor`);
* ``security_provider`` — асинхронная функция получения
  :class:`~models.trade_data.TokenSecurity` для пула
  (:class:`~core.security.TokenSecurityResolver` в live, статическая запись в
  бэктесте).

Логика одного события:

1. событие пишется в :class:`~analytics.indicators.StreamingIndicators`;
2. если по mint позиция открыта — считается окно ``exit_window_seconds`` и
   вызывается :meth:`~strategies.base_strategy.BaseStrategy.evaluate_exit`;
   при сигнале ``EXIT`` собирается и отправляется заявка на продажу
   (частичная — для TP1);
3. иначе — окно ``entry_window_seconds``, цепочка фильтров и
   :meth:`~strategies.base_strategy.BaseStrategy.evaluate_entry`; при ``ENTER``
   собирается и отправляется заявка на покупку, а успешный результат открывает
   позицию в :class:`~core.portfolio.PortfolioManager`.

Все расчёты объёмов делегированы :mod:`core.orders`, поэтому live-торговля,
dry-run и бэктест используют одну и ту же математику.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

from analytics.candles import DIP_TIMEFRAMES, CandleBuilder
from analytics.indicators import StreamingIndicators
from config.settings import settings
from core.orders import (
    OrderBuildError,
    TraderExecutor,
    build_buy_order,
    build_sell_order,
    execution_price,
    quote_entry,
    quote_exit,
)
from core.portfolio import ClosedTrade, PortfolioManager
from filters.base_filter import FilterContext
from models.trade_data import (
    ExitReason,
    MomentumMetrics,
    OrderResult,
    PoolInfo,
    Position,
    SignalAction,
    TokenSecurity,
    TradeEvent,
    TradeSide,
    TradeSignal,
    clamp_sell_fraction,
)
from strategies.base_strategy import BaseStrategy
from strategies.momentum_pump import MomentumPumpStrategy
from utils.amm import AmmQuote
from utils.logger import get_logger

logger = get_logger()

#: Поставщик данных безопасности токена по снимку пула.
SecurityProvider = Callable[[PoolInfo], Awaitable[TokenSecurity | None]]


async def no_security_provider(_pool: PoolInfo) -> TokenSecurity | None:
    """Заглушка провайдера безопасности: данных нет.

    Используется, если явный провайдер не передан. Фильтр
    :class:`~filters.security_guard.SecurityGuard` в этом случае отклоняет
    вход: «молчаливое» доверие любому токену было бы опаснее отказа.

    Args:
        _pool: Снимок пула (не используется).

    Returns:
        Всегда ``None`` — данных безопасности нет.
    """
    return None


@dataclass(frozen=True, slots=True)
class TradeDecision:
    """Итог обработки одного события сделки.

    Attributes:
        action: Действие, к которому пришёл движок.
        reason: Человекочитаемое пояснение (для логов и бэктест-отчёта).
        signal: Сигнал стратегии, если он был сформирован.
        order: Результат исполнения, если ордер отправлялся.
        closed: Запись о закрытой сделке, если позиция закрылась полностью.
    """

    action: SignalAction
    reason: str
    signal: TradeSignal | None = None
    order: OrderResult | None = None
    closed: ClosedTrade | None = None

    @property
    def executed(self) -> bool:
        """Был ли ордер успешно исполнен и подтверждён."""
        return (
            self.order is not None
            and self.order.success
            and self.order.confirmed
        )


class MomentumTrader:
    """Движок решений моментум-скальпинга.

    Держит состояние портфеля и потоковых индикаторов, применяет стратегию к
    каждому событию и отправляет ордера через инъектированный исполнитель.
    """

    def __init__(
        self,
        *,
        executor: TraderExecutor,
        portfolio: PortfolioManager | None = None,
        strategy: BaseStrategy | None = None,
        security_provider: SecurityProvider | None = None,
        indicators: StreamingIndicators | None = None,
        trade_amount_sol: float | None = None,
        entry_window_seconds: float | None = None,
        exit_window_seconds: float | None = None,
        retry_cooldown_seconds: float | None = None,
        capital_sol: float | None = None,
        token_reliable_seconds: float | None = None,
        max_entries_per_mint: int | None = None,
        reentry_cooldown_seconds: float | None = None,
    ) -> None:
        """Инициализирует движок решений.

        Args:
            executor: Исполнитель заявок (Jito либо dry-run).
            portfolio: Менеджер позиций (создаётся при ``None``).
            strategy: Торговая стратегия (по умолчанию моментум-скальпинг).
            security_provider: Поставщик данных безопасности токена.
            indicators: Реестр потоковых индикаторов (создаётся при ``None``
                с глубиной окна ``max(entry, exit)``).
            trade_amount_sol: Размер входа, SOL (по умолчанию из настроек).
            entry_window_seconds: Окно анализа входа, c.
            exit_window_seconds: Окно анализа выхода, c.
            retry_cooldown_seconds: Пауза перед повторной попыткой входа по
                пулу после неуспешного ордера, c (``None`` — из настроек).
            capital_sol: Ограничение свободного капитала, SOL (``None`` — без
                ограничения).
            token_reliable_seconds: Возраст токена (от обнаружения пула),
                после которого разрешены входы/усреднения, c (``None`` — из
                настроек).
            max_entries_per_mint: Максимум входов по одному mint
                (вход + усреднения; ``None`` — из настроек).
            reentry_cooldown_seconds: Пауза после закрытия сделки по mint
                перед новым входом, c (``None`` — из настроек).
        """
        self._executor = executor
        self._portfolio = portfolio if portfolio is not None else PortfolioManager()
        self._strategy: BaseStrategy = (
            strategy if strategy is not None else MomentumPumpStrategy()
        )
        self._security_provider: SecurityProvider = (
            security_provider if security_provider is not None else no_security_provider
        )

        self._trade_amount_sol = (
            settings.trade_amount_sol if trade_amount_sol is None else trade_amount_sol
        )
        self._entry_window_seconds = (
            settings.entry_window_seconds
            if entry_window_seconds is None
            else entry_window_seconds
        )
        self._exit_window_seconds = (
            settings.exit_window_seconds
            if exit_window_seconds is None
            else exit_window_seconds
        )
        if indicators is not None:
            self._indicators = indicators
        else:
            self._indicators = StreamingIndicators(
                max(self._entry_window_seconds, self._exit_window_seconds)
            )

        self._retry_cooldown_seconds = max(
            0.0,
            settings.retry_cooldown_seconds
            if retry_cooldown_seconds is None
            else retry_cooldown_seconds,
        )
        self._capital_sol = capital_sol
        self._token_reliable_seconds = (
            settings.token_reliable_seconds
            if token_reliable_seconds is None
            else token_reliable_seconds
        )
        self._max_entries_per_mint = (
            settings.max_entries_per_mint
            if max_entries_per_mint is None
            else max_entries_per_mint
        )
        self._reentry_cooldown_seconds = (
            settings.reentry_cooldown_seconds
            if reentry_cooldown_seconds is None
            else reentry_cooldown_seconds
        )
        self._blocked_until: dict[str, float] = {}
        self._position_locks: dict[str, asyncio.Lock] = {}
        # Свечной агрегатор по пулам (детект отката для входа).
        self._candles: dict[str, CandleBuilder] = {}
        self._dip_timeframe = DIP_TIMEFRAMES.get(
            settings.entry_dip_mode, DIP_TIMEFRAMES["1s"]
        )

    #: Окно, в котором клик по только что закрытой позиции считает
    #: «уже закрыто», а не ошибкой (снимок дашборда отстаёт на интервал SSE).
    _RECENT_CLOSE_WINDOW: float = 30.0

    # --------------------------------------------------------------- Свойства
    @property
    def portfolio(self) -> PortfolioManager:
        """Менеджер позиций."""
        return self._portfolio

    @property
    def strategy(self) -> BaseStrategy:
        """Применяемая торговая стратегия."""
        return self._strategy

    @property
    def indicators(self) -> StreamingIndicators:
        """Реестр потоковых индикаторов."""
        return self._indicators

    @property
    def executor(self) -> TraderExecutor:
        """Исполнитель заявок."""
        return self._executor

    @property
    def trade_amount_sol(self) -> float:
        """Размер входного ордера, SOL."""
        return self._trade_amount_sol

    @property
    def entry_window_seconds(self) -> float:
        """Окно анализа входа, c."""
        return self._entry_window_seconds

    @property
    def exit_window_seconds(self) -> float:
        """Окно анализа выхода, c."""
        return self._exit_window_seconds

    # ------------------------------------------------------------- Публичное
    async def process_event(
        self,
        event: TradeEvent,
        pool: PoolInfo,
        *,
        creator: str | None = None,
    ) -> TradeDecision:
        """Обрабатывает одно событие сделки.

        Args:
            event: Событие сделки из потока.
            pool: Актуальный снимок пула (после применения события).
            creator: Кошелёк создателя токена (для прямого маршрута Pump.fun).

        Returns:
            :class:`TradeDecision` с действием, сигналом и результатом ордера.
        """
        self._indicators.record(event)
        builder = self._candles.get(event.pool_address)
        if builder is None:
            builder = CandleBuilder(self._dip_timeframe)
            self._candles[event.pool_address] = builder
        builder.add(event)

        position = self._portfolio.get(event.base_mint)
        if position is None:
            return await self._handle_entry(pool, event, creator)
        if position.pool_address != event.pool_address:
            return TradeDecision(
                action=SignalAction.HOLD,
                reason="событие относится к другому пулу",
            )
        decision = await self._handle_exit(position, pool, event, creator)
        if decision.action != SignalAction.HOLD:
            return decision
        if position.entries_count >= self._max_entries_per_mint:
            return decision
        # Поза открыта, выхода нет, лимит входов не исчерпан: сильный
        # моментум-сигнал = усреднение (вход + доборы по одному mint).
        averaged = await self._handle_average(position, pool, event, creator)
        return averaged if averaged is not None else decision

    def _token_reliable(self, pool: PoolInfo, now: float) -> bool:
        """Проверяет, что токен «надёжен»: живёт дольше окна надёжности.

        Args:
            pool: Снимок пула (``created_at`` — момент обнаружения; 0 = неизвестен).
            now: Текущий unix ts события.

        Returns:
            ``True`` — входы/усреднения разрешены.
        """
        if pool.created_at <= 0.0:
            return True
        return now - pool.created_at >= self._token_reliable_seconds

    def forget_pool(self, pool_address: str) -> None:
        """Сбрасывает индикаторы и блокировки по пулу.

        Args:
            pool_address: Адрес пула.
        """
        self._indicators.prune_pool(pool_address)
        self._blocked_until.pop(pool_address, None)

    def close_all(self, *, exit_reason: str = "shutdown") -> tuple[ClosedTrade, ...]:
        """Принудительно закрывает все открытые позиции по последней цене.

        Используется при завершении бэктеста и при graceful shutdown
        live-пайплайна, чтобы отчёт содержал все сделки.

        Args:
            exit_reason: Метка причины закрытия для журнала.

        Returns:
            Кортеж закрытых сделок в порядке закрытия.
        """
        closed: list[ClosedTrade] = []
        for position in self._portfolio.all_positions():
            price = self._indicators.latest_price(position.pool_address)
            if price <= 0:
                price = position.peak_price or position.entry_price
            trade = self._portfolio.force_close(
                position.base_mint,
                exit_price=price,
                exit_reason=exit_reason,
            )
            if trade is not None:
                closed.append(trade)
        return tuple(closed)

    # -------------------------------------------------------------- Внутреннее
    async def _handle_entry(
        self, pool: PoolInfo, event: TradeEvent, creator: str | None
    ) -> TradeDecision:
        """Пытается открыть позицию по текущему событию.

        Args:
            pool: Снимок пула после события.
            event: Событие сделки.
            creator: Кошелёк создателя токена.

        Returns:
            :class:`TradeDecision` (``ENTER`` при успешной покупке).
        """
        if not self._portfolio.can_open(capital_sol=self._capital_sol):
            return TradeDecision(
                action=SignalAction.HOLD,
                reason="лимит позиций или капитала исчерпан",
            )

        if self._blocked_until.get(pool.pool_address, 0.0) > event.timestamp:
            return TradeDecision(
                action=SignalAction.HOLD,
                reason="пауза после неуспешного ордера",
            )

        recent = self._portfolio.last_closed(pool.base_mint)
        if (
            recent is not None
            and self._reentry_cooldown_seconds > 0
            and event.timestamp - recent.closed_at < self._reentry_cooldown_seconds
        ):
            return TradeDecision(
                action=SignalAction.HOLD,
                reason=(
                    f"куладаун повторного входа: {event.timestamp - recent.closed_at:.0f} c "
                    f"< {self._reentry_cooldown_seconds:.0f} c с прошлого закрытия"
                ),
            )

        if not self._token_reliable(pool, event.timestamp):
            return TradeDecision(
                action=SignalAction.HOLD,
                reason=(
                    "токен младше окна надёжности "
                    f"({self._token_reliable_seconds:.0f} c от обнаружения)"
                ),
            )

        metrics = self._indicators.window_metrics(
            pool.pool_address, self._entry_window_seconds, now=event.timestamp
        )
        security = await self._security_provider(pool)
        context = self._build_context(pool, metrics, security, event.timestamp)

        signal = self._strategy.evaluate_entry(context)
        if signal.action != SignalAction.ENTER:
            return TradeDecision(
                action=signal.action, reason=signal.reason, signal=signal
            )

        try:
            entry_quote = quote_entry(pool, sol_amount_sol=self._trade_amount_sol)
            order = build_buy_order(pool, creator=creator, quote=entry_quote)
        except OrderBuildError as exc:
            return TradeDecision(
                action=SignalAction.HOLD,
                reason=f"заявка не собрана: {exc}",
                signal=signal,
            )

        result = await self._executor.execute_buy(order)
        if not result.success or not result.confirmed:
            # Не села покупка — забываем её: без паузы на пул, следующий
            # сигнал может войти сразу.
            return TradeDecision(
                action=SignalAction.ENTER,
                reason=result.error or "ордер покупки отклонён",
                signal=signal,
                order=result,
            )

        entry_price = execution_price(entry_quote, pool.base_decimals)
        if entry_price <= 0:
            entry_price = signal.price_sol_per_token
        if entry_price <= 0 or result.token_amount <= 0:
            return TradeDecision(
                action=SignalAction.ENTER,
                reason="не удалось определить цену входа",
                signal=signal,
                order=result,
            )

        self._portfolio.open_position(
            base_mint=pool.base_mint,
            pool_address=pool.pool_address,
            dex=pool.dex,
            entry_price=entry_price,
            entry_sol=result.sol_amount,
            token_amount=result.token_amount,
            signature=result.signature,
            opened_at=event.timestamp,
            stop_loss_pct=self._stop_pct_for_pool(pool),
        )
        return TradeDecision(
            action=SignalAction.ENTER,
            reason=signal.reason,
            signal=signal,
            order=result,
        )

    def _stop_pct_for_pool(self, pool: PoolInfo) -> float:
        """Тировый стоп-лосс по объёму пула в момент входа.

        Класс позиции фиксируется при открытии: усреднения и рост объёма
        позднее не переклассифицируют позицию.

        Args:
            pool: Снимок пула (объём торгов с отслеживания).

        Returns:
            Стоп-лосс в процентах от входа.
        """
        if pool.volume_sol >= settings.hot_volume_sol:
            return settings.hard_stop_hot_pct
        if pool.volume_sol < settings.cold_volume_sol:
            return settings.hard_stop_cold_pct
        return settings.hard_stop_loss_pct

    async def _handle_average(
        self,
        position: Position,
        pool: PoolInfo,
        event: TradeEvent,
        creator: str | None,
    ) -> TradeDecision | None:
        """Усредняет открытую позицию по сильному моментум-сигналу.

        Вызывается, когда выходные триггеры не сработали, лимит входов по
        mint не исчерпан, и стратегия снова даёт ``ENTER``. Все гейты входа
        (капитал, пауза после отказа, окно надёжности, фильтры) применяются
        полностью.

        Args:
            position: Открытая позиция по этому mint.
            pool: Снимок пула после события.
            event: Событие сделки.
            creator: Кошелёк создателя токена.

        Returns:
            :class:`TradeDecision` при попытке усреднения, ``None`` — если
            сигнал не сильный (решение об удержании остаётся за выходом).
        """
        if not self._portfolio.can_open(capital_sol=self._capital_sol):
            return None
        if self._blocked_until.get(pool.pool_address, 0.0) > event.timestamp:
            return None
        if not self._token_reliable(pool, event.timestamp):
            return None

        metrics = self._indicators.window_metrics(
            pool.pool_address, self._entry_window_seconds, now=event.timestamp
        )
        security = await self._security_provider(pool)
        context = self._build_context(
            pool, metrics, security, event.timestamp, is_average=True
        )
        signal = self._strategy.evaluate_entry(context)
        if signal.action != SignalAction.ENTER:
            return None

        # Усреднение только в плюс: докупки в затухающий токен (цена ниже
        # средней входа) усиливают убыток, а не спасают позицию.
        if settings.average_only_in_profit:
            current = metrics.last_price
            if current <= 0:
                current = pool.price_sol_per_token()
            if 0 < current < position.entry_price:
                return None

        try:
            entry_quote = quote_entry(pool, sol_amount_sol=self._trade_amount_sol)
            order = build_buy_order(pool, creator=creator, quote=entry_quote)
        except OrderBuildError as exc:
            return TradeDecision(
                action=SignalAction.HOLD,
                reason=f"заявка усреднения не собрана: {exc}",
                signal=signal,
            )

        result = await self._executor.execute_buy(order)
        if not result.success or not result.confirmed:
            # Не села покупка усреднения — забываем её, без паузы на пул.
            return TradeDecision(
                action=SignalAction.ENTER,
                reason=result.error or "ордер усреднения отклонён",
                signal=signal,
                order=result,
            )

        updated = self._portfolio.add_to_position(
            position.base_mint,
            added_tokens=result.token_amount,
            added_sol=result.sol_amount,
        )
        logger.info(
            "Усреднение {}: +{:.4f} SOL (+{:.4f} токенов), входов {} / {}",
            position.base_mint,
            result.sol_amount,
            result.token_amount,
            updated.entries_count if updated else "?",
            self._max_entries_per_mint,
        )
        return TradeDecision(
            action=SignalAction.ENTER,
            reason=f"усреднение позиции: {signal.reason}",
            signal=signal,
            order=result,
        )

    async def _handle_exit(
        self,
        position: Position,
        pool: PoolInfo,
        event: TradeEvent,
        creator: str | None,
    ) -> TradeDecision:
        """Проверяет триггеры выхода по открытой позиции.

        Args:
            position: Открытая позиция.
            pool: Снимок пула после события.
            event: Событие сделки.
            creator: Кошелёк создателя токена.

        Returns:
            :class:`TradeDecision` (``EXIT`` при успешной продаже).
        """
        metrics = self._indicators.window_metrics(
            pool.pool_address, self._exit_window_seconds, now=event.timestamp
        )
        current_price = metrics.last_price
        if current_price <= 0:
            current_price = pool.price_sol_per_token()
        if current_price > 0:
            self._portfolio.mark_price(position.base_mint, current_price)

        context = self._build_context(pool, metrics, None, event.timestamp)
        signal = self._strategy.evaluate_exit(position, context)
        if signal is None or signal.action != SignalAction.EXIT:
            return TradeDecision(
                action=SignalAction.HOLD,
                reason="условия выхода не выполнены",
                signal=signal,
            )

        sell_fraction = clamp_sell_fraction(signal.sell_fraction)
        tokens_to_sell = position.token_amount * sell_fraction
        if tokens_to_sell < _dust_tokens(pool.base_decimals):
            return TradeDecision(
                action=SignalAction.EXIT,
                reason="остаток позиции ниже пыли",
                signal=signal,
            )

        result, quote_used = await self._send_sell_with_ladder(
            pool, tokens_to_sell, creator
        )
        if not result.success or not result.confirmed or quote_used is None:
            self._block_for(pool.pool_address, event.timestamp)
            return TradeDecision(
                action=SignalAction.EXIT,
                reason=result.error or "ордер продажи отклонён",
                signal=signal,
                order=result,
            )

        # Цена выхода из фактического результата (SOL/токен, человеческие
        # единицы) — execution_price() ориентирована на BUY и для продажи
        # даёт мусорные единицы (лямпорты за raw-токен).
        exit_price = (
            result.sol_amount / result.token_amount
            if result.token_amount > 0
            else execution_price(quote_used, pool.base_decimals)
        )
        closed = self._portfolio.apply_sell(
            base_mint=position.base_mint,
            sold_tokens=result.token_amount,
            received_sol=result.sol_amount,
            exit_price=exit_price,
            sold_at=event.timestamp,
            exit_reason=(signal.exit_reason or ExitReason.MANUAL).value,
        )
        if signal.exit_reason == ExitReason.TAKE_PROFIT_1 and closed is None:
            self._portfolio.mark_tp1_taken(position.base_mint)

        return TradeDecision(
            action=SignalAction.EXIT,
            reason=signal.reason,
            signal=signal,
            order=result,
            closed=closed,
        )

    def _build_context(
        self,
        pool: PoolInfo,
        metrics: MomentumMetrics,
        security: TokenSecurity | None,
        timestamp: float,
        *,
        is_average: bool = False,
    ) -> FilterContext:
        """Собирает контекст фильтрации для стратегии.

        Args:
            pool: Снимок пула.
            metrics: Метрики импульса за нужное окно.
            security: Данные безопасности токена (только для входа).
            timestamp: Время события, unix ts.

        Returns:
            :class:`~filters.base_filter.FilterContext`.
        """
        builder = self._candles.get(pool.pool_address)
        dip = builder.dip_pct(settings.entry_dip_mode) if builder else None
        return FilterContext(
            pool=pool,
            trade_amount_sol=self._trade_amount_sol,
            security=security,
            metrics=metrics,
            timestamp=timestamp if timestamp > 0 else time.time(),
            dip_pct=dip,
            is_average=is_average,
        )

    def _block_for(self, pool_address: str, now: float) -> None:
        """Ставит пул на паузу после неуспешной ПРОДАЖИ (выходов).

        Args:
            pool_address: Адрес пула.
            now: Текущее время события, unix ts.
        """
        if self._retry_cooldown_seconds <= 0:
            return
        self._blocked_until[pool_address] = now + self._retry_cooldown_seconds

    # ------------------------------------------------- Ручные операции (дашборд)
    def _position_lock(self, base_mint: str) -> asyncio.Lock:
        """Блокировка позиции: сериализует ручные и автоматические операции.

        Кнопки дашборда обрабатываются в loop'е дашборда параллельно с
        торговым циклом — без блокировки возможны двойная продажа и
        двойная запись в портфель/БД.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            :class:`asyncio.Lock` для данного mint.
        """
        lock = self._position_locks.get(base_mint)
        if lock is None:
            lock = asyncio.Lock()
            self._position_locks[base_mint] = lock
        return lock

    async def _send_sell_with_ladder(
        self,
        pool: PoolInfo,
        tokens_to_sell: float,
        creator: str | None,
    ) -> tuple[OrderResult, AmmQuote | None]:
        """Собирает и отправляет продажу с лестницей (дисконт → floor 0).

        Общий путь для автоматических выходов и ручной кнопки. Повтор с
        нулевым порогом выполняется только при заведомо не отправленной
        транзакции (`success=False`); при неопределённом статусе повтор
        опасен двойной продажей и не выполняется.

        Args:
            pool: Снимок пула.
            tokens_to_sell: Количество токенов к продаже (человекочитаемое).
            creator: Кошелёк создателя (для прямого маршрута Pump.fun).

        Returns:
            Кортеж (результат исполнения, котировка фактически
            использованной попытки; ``None``, если заявка не собралась).
        """
        try:
            quote = quote_exit(pool, token_amount=tokens_to_sell)
            order = build_sell_order(
                pool, creator=creator, token_amount=tokens_to_sell, quote=quote
            )
        except OrderBuildError as exc:
            return (
                OrderResult.failed(
                    side=TradeSide.SELL,
                    base_mint=pool.base_mint,
                    pool_address=pool.pool_address,
                    error=f"заявка не собрана: {exc}",
                ),
                None,
            )
        result = await self._executor.execute_sell(order)
        if result.success:
            return result, quote
        try:
            retry_quote = replace(
                quote_exit(pool, token_amount=tokens_to_sell, sell_slippage_pct=0.0),
                min_amount_out=0,
            )
            retry_order = build_sell_order(
                pool,
                creator=creator,
                token_amount=tokens_to_sell,
                quote=retry_quote,
            )
        except OrderBuildError as exc:
            return (
                OrderResult.failed(
                    side=TradeSide.SELL,
                    base_mint=pool.base_mint,
                    pool_address=pool.pool_address,
                    error=f"повторная заявка не собрана: {exc}",
                ),
                None,
            )
        logger.warning(
            "Лестница выхода {}: первая попытка не прошла ({}), повтор с порогом 0",
            pool.base_mint,
            result.error or "причина неизвестна",
        )
        result = await self._executor.execute_sell(retry_order)
        quote_used = retry_quote if result.success else quote
        return result, quote_used

    async def manual_sell(
        self,
        base_mint: str,
        pool: PoolInfo,
        *,
        creator: str | None = None,
        exit_reason: str = "manual",
    ) -> dict[str, object]:
        """Ручная продажа всей позиции (кнопка дашборда «Закрыть»).

        Проходит через тот же исполнитель и лестницу, что и автоматические
        выходы, и через тот же write-through портфель: полное закрытие
        атомарно удаляет позицию из БД и пишет запись в ``closed_trades``.

        Args:
            base_mint: Mint мемкоина.
            pool: Свежий снимок пула (для котировки).
            creator: Кошелёк создателя токена.
            exit_reason: Причина закрытия для ``closed_trades``
                (``"manual"`` — кнопка дашборда; свип использует свою).

        Returns:
            Словарь ``{ok, error?, sol?, tokens?, closed?}``.
        """
        async with self._position_lock(base_mint):
            position = self._portfolio.get(base_mint)
            if position is None:
                recent = self._portfolio.last_closed(base_mint)
                if (
                    recent is not None
                    and time.time() - recent.closed_at < self._RECENT_CLOSE_WINDOW
                ):
                    return {
                        "ok": True,
                        "already_closed": True,
                        "note": (
                            f"позиция уже закрыта стратегией "
                            f"({recent.exit_reason}, {recent.pnl_sol:+.4f} SOL)"
                        ),
                    }
                return {"ok": False, "error": f"позиция {base_mint} не найдена"}
            if position.pool_address != pool.pool_address:
                return {"ok": False, "error": "несоответствие пула позиции"}
            result, quote_used = await self._send_sell_with_ladder(
                pool, position.token_amount, creator
            )
            if not result.success or not result.confirmed or quote_used is None:
                return {
                    "ok": False,
                    "error": result.error or "ордер продажи отклонён",
                }
            exit_price = (
                result.sol_amount / result.token_amount
                if result.token_amount > 0
                else execution_price(quote_used, pool.base_decimals)
            )
            closed = self._portfolio.apply_sell(
                base_mint=base_mint,
                sold_tokens=result.token_amount,
                received_sol=result.sol_amount,
                exit_price=exit_price,
                exit_reason=exit_reason,
            )
            logger.info(
                "Продажа {} ({}): {:+.4f} SOL за {:.4f} токенов",
                base_mint,
                exit_reason,
                result.sol_amount,
                result.token_amount,
            )
            return {
                "ok": True,
                "sol": result.sol_amount,
                "tokens": result.token_amount,
                "closed": closed is not None,
            }

    async def manual_buy(
        self,
        base_mint: str,
        pool: PoolInfo,
        *,
        creator: str | None = None,
    ) -> dict[str, object]:
        """Ручное усреднение открытой позиции (кнопка дашборда).

        Покупает ``TRADE_AMOUNT_SOL`` через обычный исполнитель и добавляет
        результат к позиции средневзвешенной ценой
        (:meth:`~core.portfolio.PortfolioManager.add_to_position`). Моментум-гейты
        и фильтры входа не применяются — это осознанное ручное действие.

        Args:
            base_mint: Mint мемкоина.
            pool: Свежий снимок пула.
            creator: Кошелёк создателя токена.

        Returns:
            Словарь ``{ok, error?, added_tokens?, sol?, entry_price?}``.
        """
        async with self._position_lock(base_mint):
            position = self._portfolio.get(base_mint)
            if position is None:
                recent = self._portfolio.last_closed(base_mint)
                if (
                    recent is not None
                    and time.time() - recent.closed_at < self._RECENT_CLOSE_WINDOW
                ):
                    return {
                        "ok": False,
                        "error": (
                            f"позиция уже закрыта стратегией "
                            f"({recent.exit_reason}, {recent.pnl_sol:+.4f} SOL)"
                        ),
                    }
                return {"ok": False, "error": "усреднять можно только открытую позицию"}
            if position.pool_address != pool.pool_address:
                return {"ok": False, "error": "несоответствие пула позиции"}
            if position.entries_count >= self._max_entries_per_mint:
                return {
                    "ok": False,
                    "error": (
                        "лимит входов по минту исчерпан "
                        f"({position.entries_count}/{self._max_entries_per_mint})"
                    ),
                }
            try:
                entry_quote = quote_entry(pool, sol_amount_sol=self._trade_amount_sol)
                order = build_buy_order(pool, creator=creator, quote=entry_quote)
            except OrderBuildError as exc:
                return {"ok": False, "error": f"заявка не собрана: {exc}"}
            result = await self._executor.execute_buy(order)
            if not result.success or not result.confirmed:
                return {
                    "ok": False,
                    "error": result.error or "ордер покупки отклонён",
                }
            updated = self._portfolio.add_to_position(
                base_mint,
                added_tokens=result.token_amount,
                added_sol=result.sol_amount,
            )
            return {
                "ok": True,
                "added_tokens": result.token_amount,
                "sol": result.sol_amount,
                "entry_price": updated.entry_price if updated else 0.0,
            }


def _dust_tokens(base_decimals: int) -> float:
    """Возвращает «пылевой» порог количества токенов.

    Args:
        base_decimals: Десятичные знаки базового токена.

    Returns:
        Минимально осмысленное количество токенов в человекочитаемых единицах.
    """
    return 1.0 / float(10**base_decimals)
