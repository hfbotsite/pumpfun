"""Фабрика заявок на исполнение и контракт исполнителя.

Модуль отделяет **расчёт** ордера (сколько SOL/токенов и какой
``min_amount_out``) от его **отправки**. Здесь нет ни одного сетевого вызова:
на вход подаётся снимок пула, на выходе — :class:`~core.executor.BuyOrder`
или :class:`~core.executor.SellOrder` с уже применённым дисконтом
проскальзывания.

Такой разрыв важен по двум причинам:

* одна и та же логика котирования используется live-пайплайном, dry-run и
  бэктестером — расхождения между «бумажной» и реальной сделкой минимальны;
* :class:`TraderExecutor` описывает минимальный контракт, которому
  удовлетворяют и :class:`~core.executor.OrderExecutor` (Jito/Jupiter), и
  :class:`~core.dry_run.DryRunExecutor` (симуляция без отправки).

Все суммы переводятся в «сырые» единицы (лампорты / base units) на границе
этого модуля: и инструкции Pump.fun, и Jupiter, и бэктест оперируют целыми
числами.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from config.settings import LAMPORTS_PER_SOL, settings
from core.executor import BuyOrder, SellOrder
from filters.liquidity_guard import pool_fee_bps
from models.trade_data import OrderResult, PoolInfo
from utils.amm import AmmQuote, quote_buy, quote_sell


class OrderBuildError(RuntimeError):
    """Невозможно построить заявку (пул без резервов, нулевой объём)."""


@runtime_checkable
class TraderExecutor(Protocol):
    """Минимальный контракт исполнителя ордеров.

    Реализуется :class:`~core.executor.OrderExecutor` (реальная отправка
    Jito-бандлом) и :class:`~core.dry_run.DryRunExecutor` (симуляция fill по
    котировке AMM без обращения к сети).
    """

    async def execute_buy(self, order: BuyOrder) -> OrderResult:
        """Исполняет покупку.

        Args:
            order: Заявка на покупку.

        Returns:
            Результат исполнения.
        """
        ...

    async def execute_sell(self, order: SellOrder) -> OrderResult:
        """Исполняет продажу.

        Args:
            order: Заявка на продажу.

        Returns:
            Результат исполнения.
        """
        ...

    async def close(self) -> None:
        """Освобождает сетевые ресурсы исполнителя."""
        ...


def _require_reserves(pool: PoolInfo) -> None:
    """Проверяет, что в пуле есть резервы по обеим сторонам.

    Args:
        pool: Снимок пула.

    Raises:
        OrderBuildError: Если резервы нулевые.
    """
    if pool.base_reserve <= 0 or pool.quote_reserve <= 0:
        raise OrderBuildError(
            "пул без резервов: "
            f"base={pool.base_reserve}, quote={pool.quote_reserve}"
        )


def quote_entry(
    pool: PoolInfo,
    *,
    sol_amount_sol: float | None = None,
    buy_slippage_pct: float | None = None,
) -> AmmQuote:
    """Котирует покупку базового токена за SOL.

    Args:
        pool: Снимок пула с текущими резервами.
        sol_amount_sol: Размер входа, SOL (по умолчанию из настроек).
        buy_slippage_pct: Проскальзывание входа, % (по умолчанию из настроек).

    Returns:
        :class:`~utils.amm.AmmQuote` с ожидаемым и минимальным выходом.

    Raises:
        OrderBuildError: Если резервы пула нулевые или вход меньше лампорта.
    """
    _require_reserves(pool)
    amount_sol = settings.trade_amount_sol if sol_amount_sol is None else sol_amount_sol
    slippage = (
        settings.buy_slippage_pct
        if buy_slippage_pct is None
        else buy_slippage_pct
    )
    sol_in = int(amount_sol * LAMPORTS_PER_SOL)
    if sol_in <= 0:
        raise OrderBuildError(f"нулевой размер входа: {amount_sol} SOL")
    return quote_buy(
        sol_in=sol_in,
        sol_reserve=pool.quote_reserve,
        token_reserve=pool.base_reserve,
        fee_bps=pool_fee_bps(pool.dex),
        slippage_pct=slippage,
    )


def quote_exit(
    pool: PoolInfo,
    *,
    token_amount: float,
    sell_slippage_pct: float | None = None,
) -> AmmQuote:
    """Котирует продажу базового токена за SOL.

    Args:
        pool: Снимок пула с текущими резервами.
        token_amount: Продаваемое количество токенов (человекочитаемое).
        sell_slippage_pct: Проскальзывание выхода, % (по умолчанию из настроек).

    Returns:
        :class:`~utils.amm.AmmQuote` с ожидаемым и минимальным выходом SOL.

    Raises:
        OrderBuildError: Если резервы пула нулевые, объём неположителен или
            вход превышает резерв токенов.
    """
    _require_reserves(pool)
    tokens_in = int(token_amount * float(10**pool.base_decimals))
    if tokens_in <= 0:
        raise OrderBuildError(f"нулевой размер продажи: {token_amount} токенов")
    if tokens_in >= pool.base_reserve:
        raise OrderBuildError(
            "размер продажи превышает резерв токенов: "
            f"{tokens_in} >= {pool.base_reserve}"
        )
    slippage = (
        settings.sell_slippage_pct
        if sell_slippage_pct is None
        else sell_slippage_pct
    )
    return quote_sell(
        tokens_in=tokens_in,
        sol_reserve=pool.quote_reserve,
        token_reserve=pool.base_reserve,
        fee_bps=pool_fee_bps(pool.dex),
        slippage_pct=slippage,
    )


def build_buy_order(
    pool: PoolInfo,
    *,
    creator: str | None,
    quote: AmmQuote | None = None,
    sol_amount_sol: float | None = None,
    buy_slippage_pct: float | None = None,
) -> BuyOrder:
    """Собирает заявку на покупку из снимка пула.

    Args:
        pool: Снимок пула.
        creator: Кошелёк создателя токена (для прямого маршрута Pump.fun).
        quote: Готовая котировка (считается заново при ``None``).
        sol_amount_sol: Размер входа, SOL (по умолчанию из настроек).
        buy_slippage_pct: Проскальзывание входа, % (по умолчанию из настроек).

    Returns:
        :class:`~core.executor.BuyOrder` с явным ``min_tokens_out``.

    Raises:
        OrderBuildError: Если заявку невозможно корректно построить.
    """
    entry_quote = quote or quote_entry(
        pool, sol_amount_sol=sol_amount_sol, buy_slippage_pct=buy_slippage_pct
    )
    if entry_quote.amount_out <= 0:
        raise OrderBuildError("котировка покупки дала нулевой выход токенов")
    return BuyOrder(
        pool=pool,
        sol_amount_lamports=entry_quote.amount_in,
        expected_tokens=entry_quote.amount_out,
        min_tokens_out=entry_quote.min_amount_out,
        price_impact_pct=entry_quote.price_impact_pct,
        creator=creator,
    )


def build_sell_order(
    pool: PoolInfo,
    *,
    creator: str | None,
    token_amount: float,
    quote: AmmQuote | None = None,
    sell_slippage_pct: float | None = None,
) -> SellOrder:
    """Собирает заявку на продажу из снимка пула.

    Args:
        pool: Снимок пула.
        creator: Кошелёк создателя токена (для прямого маршрута Pump.fun).
        token_amount: Продаваемое количество токенов (человекочитаемое).
        quote: Готовая котировка (считается заново при ``None``).
        sell_slippage_pct: Проскальзывание выхода, % (по умолчанию из настроек).

    Returns:
        :class:`~core.executor.SellOrder` с явным ``min_amount_out``.

    Raises:
        OrderBuildError: Если заявку невозможно корректно построить.
    """
    exit_quote = quote or quote_exit(
        pool, token_amount=token_amount, sell_slippage_pct=sell_slippage_pct
    )
    return SellOrder(
        pool=pool,
        token_amount=exit_quote.amount_in,
        quote=exit_quote,
        price_impact_pct=exit_quote.price_impact_pct,
        creator=creator,
    )


def execution_price(quote: AmmQuote, base_decimals: int) -> float:
    """Считает фактическую цену исполнения в SOL за токен.

    Цена входа/выхода берётся из котировки, а не из спот-цены: она уже
    включает комиссию пула и ценовое воздействие собственного ордера,
    поэтому PnL относительно неё консервативен и честен.

    Args:
        quote: Котировка покупки (SOL → токены) или продажи (токены → SOL).
        base_decimals: Десятичные знаки базового токена.

    Returns:
        Цена в SOL за токен; ``0.0`` при некорректной котировке.
    """
    if quote.amount_out <= 0 or quote.amount_in <= 0:
        return 0.0
    tokens_ui = quote.amount_out / float(10**base_decimals)
    sol_ui = quote.amount_in / float(LAMPORTS_PER_SOL)
    if tokens_ui <= 0:
        return 0.0
    return sol_ui / tokens_ui
