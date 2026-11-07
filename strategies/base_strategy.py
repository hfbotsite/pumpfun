"""Абстрактный базовый класс торговой стратегии.

Стратегия получает контекст (пул, метрики импульса, безопасность) и решает
одно из трёх действий: :attr:`~models.trade_data.SignalAction.ENTER`,
:attr:`~models.trade_data.SignalAction.EXIT` или
:attr:`~models.trade_data.SignalAction.HOLD`.

Стратегия не выполняет сетевых вызовов и не отправляет транзакции — она лишь
порождает :class:`~models.trade_data.TradeSignal`, который исполняет слой
:mod:`core.executor`. Такое разделение позволяет прогонять одну и ту же
логику и в live-режиме, и в бэктестере.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from filters.base_filter import FilterContext
from models.trade_data import (
    ExitReason,
    Position,
    SignalAction,
    TradeSignal,
    clamp_sell_fraction,
)


class BaseStrategy(ABC):
    """Базовый интерфейс стратегии.

    Наследники реализуют :meth:`evaluate_entry` (решение о входе) и
    :meth:`evaluate_exit` (решение о выходе из открытой позиции).
    """

    #: Человекочитаемое имя стратегии.
    name: str = "base_strategy"

    @abstractmethod
    def evaluate_entry(self, context: FilterContext) -> TradeSignal:
        """Принимает решение о входе по контексту актива.

        Args:
            context: Контекст фильтрации с пулом, метриками и безопасностью.

        Returns:
            Сигнал ``ENTER`` или ``HOLD``.
        """
        raise NotImplementedError

    @abstractmethod
    def evaluate_exit(
        self, position: Position, context: FilterContext
    ) -> TradeSignal | None:
        """Принимает решение о выходе по открытой позиции.

        Args:
            position: Текущая открытая позиция.
            context: Свежий контекст актива (цена, метрики).

        Returns:
            Сигнал ``EXIT`` с :class:`~models.trade_data.ExitReason` или
            ``None``, если удерживать позицию.
        """
        raise NotImplementedError

    # ------------------------------------------------------------- Хелперы
    def _hold_signal(
        self, context: FilterContext, reason: str = "условия не выполнены"
    ) -> TradeSignal:
        """Создаёт сигнал удержания (без входа).

        Args:
            context: Контекст актива.
            reason: Пояснение причины удержания.

        Returns:
            Сигнал ``HOLD``.
        """
        return TradeSignal(
            action=SignalAction.HOLD,
            base_mint=context.base_mint,
            pool_address=context.pool_address,
            dex=context.pool.dex,
            price_sol_per_token=context.pool.price_sol_per_token(),
            reason=reason,
            metrics=context.metrics,
            timestamp=context.timestamp,
        )

    def _enter_signal(
        self, context: FilterContext, reason: str
    ) -> TradeSignal:
        """Создаёт сигнал входа.

        Args:
            context: Контекст актива.
            reason: Пояснение причины входа.

        Returns:
            Сигнал ``ENTER``.
        """
        return TradeSignal(
            action=SignalAction.ENTER,
            base_mint=context.base_mint,
            pool_address=context.pool_address,
            dex=context.pool.dex,
            price_sol_per_token=context.pool.price_sol_per_token(),
            reason=reason,
            metrics=context.metrics,
            timestamp=context.timestamp,
        )

    def _exit_signal(
        self,
        position: Position,
        context: FilterContext,
        exit_reason: ExitReason,
        sell_fraction: float,
        reason: str,
    ) -> TradeSignal:
        """Создаёт сигнал выхода.

        Args:
            position: Открытая позиция.
            context: Контекст актива.
            exit_reason: Причина выхода.
            sell_fraction: Доля позиции к продаже (0..1).
            reason: Пояснение причины выхода.

        Returns:
            Сигнал ``EXIT``.
        """
        return TradeSignal(
            action=SignalAction.EXIT,
            base_mint=position.base_mint,
            pool_address=position.pool_address,
            dex=position.dex,
            price_sol_per_token=context.pool.price_sol_per_token(),
            reason=reason,
            exit_reason=exit_reason,
            sell_fraction=clamp_sell_fraction(sell_fraction),
            metrics=context.metrics,
            timestamp=context.timestamp,
        )
