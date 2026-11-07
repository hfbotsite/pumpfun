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

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

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
    TradeSignal,
    clamp_sell_fraction,
)
from strategies.base_strategy import BaseStrategy
from strategies.momentum_pump import MomentumPumpStrategy
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
        retry_cooldown_seconds: float = 2.0,
        capital_sol: float | None = None,
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
                пулу после неуспешного ордера, c.
            capital_sol: Ограничение свободного капитала, SOL (``None`` — без
                ограничения).
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

        self._retry_cooldown_seconds = max(0.0, retry_cooldown_seconds)
        self._capital_sol = capital_sol
        self._blocked_until: dict[str, float] = {}

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

        position = self._portfolio.get(event.base_mint)
        if position is not None:
            if position.pool_address != event.pool_address:
                return TradeDecision(
                    action=SignalAction.HOLD,
                    reason="событие относится к другому пулу",
                )
            return await self._handle_exit(position, pool, event, creator)
        return await self._handle_entry(pool, event, creator)

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
            self._block_for(pool.pool_address, event.timestamp)
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
            self._block_for(pool.pool_address, event.timestamp)
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
        )
        return TradeDecision(
            action=SignalAction.ENTER,
            reason=signal.reason,
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

        try:
            exit_quote = quote_exit(pool, token_amount=tokens_to_sell)
            order = build_sell_order(
                pool, creator=creator, token_amount=tokens_to_sell, quote=exit_quote
            )
        except OrderBuildError as exc:
            return TradeDecision(
                action=SignalAction.EXIT,
                reason=f"заявка не собрана: {exc}",
                signal=signal,
            )

        result = await self._executor.execute_sell(order)
        if not result.success:
            # Лестница выхода: первая попытка с дисконтом не прошла —
            # повторяем по свежей котировке с нулевым порогом (min_sol_output
            # = 0), чтобы продажа исполнилась в любом случае. Важно: 0%
            # проскальзывания в котировке означает «без дисконта», а не
            # floor=0, поэтому порог обнуляется явно. Повтор только при
            # заведомо не отправленной транзакции: при неопределённом
            # статусе (confirmed = False) повтор опасен двойной продажей.
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
                self._block_for(pool.pool_address, event.timestamp)
                return TradeDecision(
                    action=SignalAction.EXIT,
                    reason=f"повторная заявка не собрана: {exc}",
                    signal=signal,
                    order=result,
                )
            logger.warning(
                "Лестница выхода {}: первая попытка не прошла ({}), "
                "повтор с порогом 0",
                position.base_mint,
                result.error or "причина неизвестна",
            )
            result = await self._executor.execute_sell(retry_order)

        if not result.success or not result.confirmed:
            self._block_for(pool.pool_address, event.timestamp)
            return TradeDecision(
                action=SignalAction.EXIT,
                reason=result.error or "ордер продажи отклонён",
                signal=signal,
                order=result,
            )

        closed = self._portfolio.apply_sell(
            base_mint=position.base_mint,
            sold_tokens=result.token_amount,
            received_sol=result.sol_amount,
            exit_price=execution_price(exit_quote, pool.base_decimals),
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
        return FilterContext(
            pool=pool,
            trade_amount_sol=self._trade_amount_sol,
            security=security,
            metrics=metrics,
            timestamp=timestamp if timestamp > 0 else time.time(),
        )

    def _block_for(self, pool_address: str, now: float) -> None:
        """Ставит пул на паузу после неуспешного ордера.

        Args:
            pool_address: Адрес пула.
            now: Текущее время события, unix ts.
        """
        if self._retry_cooldown_seconds <= 0:
            return
        self._blocked_until[pool_address] = now + self._retry_cooldown_seconds


def _dust_tokens(base_decimals: int) -> float:
    """Возвращает «пылевой» порог количества токенов.

    Args:
        base_decimals: Десятичные знаки базового токена.

    Returns:
        Минимально осмысленное количество токенов в человекочитаемых единицах.
    """
    return 1.0 / float(10**base_decimals)
