"""Чистая математика AMM с постоянным произведением (``x * y = k``).

Модуль не зависит от сети и состояния — только целочисленные резервы и
параметры. Используется фильтром ликвидности, построителем транзакций и
бэктестером, что гарантирует единообразие расчётов во всех слоях.

Все количества — в «сырых» единицах соответствующих токенов (лампорты для SOL).
Комиссия задаётся в базисных пунктах (bps, 1 bps = 0.01%).
"""

from __future__ import annotations

from dataclasses import dataclass

from utils.constants import BPS_DENOMINATOR


def _apply_fee(amount_in: int, fee_bps: int) -> int:
    """Вычитает комиссию пула из входного количества.

    Args:
        amount_in: Входное количество (сырые единицы).
        fee_bps: Комиссия в базисных пунктах.

    Returns:
        Количество после вычета комиссии.
    """
    if fee_bps <= 0:
        return amount_in
    return amount_in * (BPS_DENOMINATOR - fee_bps) // BPS_DENOMINATOR


def get_amount_out(
    amount_in: int,
    reserve_in: int,
    reserve_out: int,
    fee_bps: int = 0,
) -> int:
    """Считает выходное количество по формуле constant product.

    Формула::

        amount_in_after_fee = amount_in * (1 - fee)
        amount_out = reserve_out * amount_in_after_fee / (reserve_in + amount_in_after_fee)

    Args:
        amount_in: Входное количество (сырые единицы).
        reserve_in: Резерв входного токена в пуле.
        reserve_out: Резерв выходного токена в пуле.
        fee_bps: Комиссия пула в базисных пунктах.

    Returns:
        Выходное количество (сырые единицы). ``0`` при некорректных входах.
    """
    if amount_in <= 0 or reserve_in <= 0 or reserve_out <= 0:
        return 0
    amount_in_after_fee = _apply_fee(amount_in, fee_bps)
    if amount_in_after_fee <= 0:
        return 0
    numerator = reserve_out * amount_in_after_fee
    denominator = reserve_in + amount_in_after_fee
    return numerator // denominator


def price_impact_pct(amount_in: int, reserve_in: int) -> float:
    """Считает Price Impact сделки без учёта комиссии.

    Для constant product относительный сдвиг цены равен
    ``dx / (reserve_in + dx)``.

    Args:
        amount_in: Входное количество (сырые единицы).
        reserve_in: Резерв входного токена в пуле.

    Returns:
        Price Impact в процентах (0..100+).
    """
    if amount_in <= 0 or reserve_in <= 0:
        return 0.0
    return 100.0 * amount_in / (reserve_in + amount_in)


def spot_price(reserve_in: int, reserve_out: int) -> float:
    """Считает спот-цену одного входного токена в выходных единицах.

    Комиссия на спот-цену не влияет по определению, поэтому параметра
    комиссии у функции нет.

    Args:
        reserve_in: Резерв входного токена в пуле.
        reserve_out: Резерв выходного токена в пуле.

    Returns:
        Цена в выходных единицах за одну входную единицу (сырые единицы).
    """
    if reserve_in <= 0 or reserve_out <= 0:
        return 0.0
    return reserve_out / reserve_in


def min_out_with_slippage(quoted_out: int, slippage_pct: float) -> int:
    """Считает ``min_amount_out`` с защитным дисконтом от проскальзывания.

    Args:
        quoted_out: Ожидаемое выходное количество (сырые единицы).
        slippage_pct: Проскальзывание в процентах (например, 20.0).

    Returns:
        Минимально приемлемое выходное количество (с округлением вниз),
        но не менее 1.
    """
    if quoted_out <= 0:
        return 0
    factor = max(0.0, 1.0 - slippage_pct / 100.0)
    result = int(quoted_out * factor)
    return max(1, result)


def max_in_with_slippage(quoted_in: int, slippage_pct: float) -> int:
    """Считает ``max_amount_in`` с запасом на проскальзывание.

    Args:
        quoted_in: Ожидаемое входное количество (сырые единицы).
        slippage_pct: Проскальзывание в процентах.

    Returns:
        Максимально допустимое входное количество (с округлением вверх).
    """
    if quoted_in <= 0:
        return 0
    factor = 1.0 + max(0.0, slippage_pct) / 100.0
    return int(quoted_in * factor) + 1


@dataclass(frozen=True, slots=True)
class AmmQuote:
    """Результат расчёта сделки в constant-product пуле."""

    amount_in: int
    amount_out: int
    min_amount_out: int
    price_impact_pct: float
    spot_price: float
    fee_bps: int

    @property
    def execution_price(self) -> float:
        """Фактическая цена исполнения (выход на вход)."""
        if self.amount_in <= 0:
            return 0.0
        return self.amount_out / self.amount_in


def quote_buy(
    sol_in: int,
    sol_reserve: int,
    token_reserve: int,
    fee_bps: int,
    slippage_pct: float,
) -> AmmQuote:
    """Расчёт покупки токенов за SOL.

    Args:
        sol_in: Входной объём в лампортах.
        sol_reserve: Резерв SOL в пуле (лампорты).
        token_reserve: Резерв токенов в пуле (сырые единицы).
        fee_bps: Комиссия пула в базисных пунктах.
        slippage_pct: Проскальзывание для расчёта ``min_amount_out``.

    Returns:
        :class:`AmmQuote` с ожидаемым и минимальным выходом.
    """
    amount_out = get_amount_out(sol_in, sol_reserve, token_reserve, fee_bps)
    return AmmQuote(
        amount_in=sol_in,
        amount_out=amount_out,
        min_amount_out=min_out_with_slippage(amount_out, slippage_pct),
        price_impact_pct=price_impact_pct(sol_in, sol_reserve),
        spot_price=spot_price(sol_reserve, token_reserve),
        fee_bps=fee_bps,
    )


def quote_sell(
    tokens_in: int,
    sol_reserve: int,
    token_reserve: int,
    fee_bps: int,
    slippage_pct: float,
) -> AmmQuote:
    """Расчёт продажи токенов за SOL.

    Args:
        tokens_in: Входное количество токенов (сырые единицы).
        sol_reserve: Резерв SOL в пуле (лампорты).
        token_reserve: Резерв токенов в пуле (сырые единицы).
        fee_bps: Комиссия пула в базисных пунктах.
        slippage_pct: Проскальзывание для расчёта ``min_amount_out``.

    Returns:
        :class:`AmmQuote` с ожидаемым и минимальным выходом SOL.
    """
    amount_out = get_amount_out(tokens_in, token_reserve, sol_reserve, fee_bps)
    return AmmQuote(
        amount_in=tokens_in,
        amount_out=amount_out,
        min_amount_out=min_out_with_slippage(amount_out, slippage_pct),
        price_impact_pct=price_impact_pct(tokens_in, token_reserve),
        spot_price=spot_price(token_reserve, sol_reserve),
        fee_bps=fee_bps,
    )
