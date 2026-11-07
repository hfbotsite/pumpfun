"""Фильтр ликвидности: минимальный пул, price impact и расчёт выхода.

Фильтр выполняет три роли:

1. **Проверка ликвидности пула** — отсекает «сухие» пулы с ликвидностью
   ниже :data:`~config.settings.settings.min_pool_sol`.
2. **Проверка Price Impact** — отклоняет вход, если один ордер сдвигает цену
   сильнее допустимого :data:`~config.settings.settings.max_price_impact_pct`.
3. **Расчёт `min_amount_out`** — считает ожидаемое количество токенов по
   формуле постоянного произведения ``x·y = k`` и применяет жёсткий дисконт
   :data:`~config.settings.settings.buy_slippage_pct` (20% по умолчанию),
   получая минимально приемлемый выход для инструкции свопа.

Все вычисления выполняются на «сырых» резервах через :mod:`utils.amm`, что
гарантирует единообразие с построителем транзакций и бэктестером.
"""

from __future__ import annotations

from dataclasses import dataclass

from config.settings import LAMPORTS_PER_SOL, settings
from filters.base_filter import BaseFilter, FilterContext
from models.trade_data import DEXType, FilterResult
from utils.amm import AmmQuote, quote_buy
from utils.constants import PUMP_FUN_FEE_BPS, RAYDIUM_FEE_BPS


def pool_fee_bps(dex: DEXType) -> int:
    """Возвращает комиссию пула (в bps) для конкретного DEX.

    Args:
        dex: Тип биржи.

    Returns:
        Комиссия пула в базисных пунктах.
    """
    if dex == DEXType.PUMP_FUN:
        return PUMP_FUN_FEE_BPS
    return RAYDIUM_FEE_BPS


@dataclass(frozen=True, slots=True)
class LiquidityCheck:
    """Результат проверки ликвидности и расчёта выхода по покупке."""

    pool_sol: float
    price_impact_pct: float
    quote: AmmQuote

    @property
    def expected_tokens(self) -> int:
        """Ожидаемое количество токенов (base units) без дисконта."""
        return self.quote.amount_out

    @property
    def min_tokens_out(self) -> int:
        """Минимально приемлемое количество токенов (с дисконтом проскальзывания)."""
        return self.quote.min_amount_out


class LiquidityGuard(BaseFilter):
    """Фильтр допуска по ликвидности пула и проскальзыванию.

    Проверяет минимальную ликвидность пула и предельный price impact для
    планируемого ордера, а также предоставляет расчёт ``min_amount_out``.
    """

    name = "liquidity_guard"

    def __init__(
        self,
        *,
        min_pool_sol: float | None = None,
        max_price_impact_pct: float | None = None,
        buy_slippage_pct: float | None = None,
    ) -> None:
        """Инициализирует фильтр.

        Args:
            min_pool_sol: Переопределение минимума ликвидности, SOL.
            max_price_impact_pct: Переопределение предела price impact, %.
            buy_slippage_pct: Переопределение проскальзывания входа, %.
        """
        self._min_pool_sol = (
            settings.min_pool_sol if min_pool_sol is None else min_pool_sol
        )
        self._max_price_impact_pct = (
            settings.max_price_impact_pct
            if max_price_impact_pct is None
            else max_price_impact_pct
        )
        self._buy_slippage_pct = (
            settings.buy_slippage_pct
            if buy_slippage_pct is None
            else buy_slippage_pct
        )

    @property
    def min_pool_sol(self) -> float:
        """Минимальная ликвидность пула, SOL."""
        return self._min_pool_sol

    @property
    def max_price_impact_pct(self) -> float:
        """Предельный price impact ордера, %."""
        return self._max_price_impact_pct

    @property
    def buy_slippage_pct(self) -> float:
        """Проскальзывание на вход, %."""
        return self._buy_slippage_pct

    # ------------------------------------------------------------ Публичное
    def check_pool_liquidity(
        self,
        context: FilterContext,
    ) -> LiquidityCheck:
        """Считает метрики ликвидности и выход по покупке.

        Args:
            context: Контекст фильтрации с текущим состоянием пула.

        Returns:
            :class:`LiquidityCheck` с ликвидностью, price impact и котировкой.
        """
        pool = context.pool
        sol_in = int(context.trade_amount_sol * LAMPORTS_PER_SOL)
        quote = quote_buy(
            sol_in=sol_in,
            sol_reserve=pool.quote_reserve,
            token_reserve=pool.base_reserve,
            fee_bps=pool_fee_bps(pool.dex),
            slippage_pct=self._buy_slippage_pct,
        )
        return LiquidityCheck(
            pool_sol=pool.pool_sol,
            price_impact_pct=quote.price_impact_pct,
            quote=quote,
        )

    def min_tokens_out(self, context: FilterContext) -> int:
        """Возвращает ``min_amount_out`` с дисконтом проскальзывания входа.

        Args:
            context: Контекст фильтрации.

        Returns:
            Минимально приемлемое количество токенов (base units).
        """
        return self.check_pool_liquidity(context).min_tokens_out

    # -------------------------------------------------------------- Фильтр
    def evaluate(self, context: FilterContext) -> FilterResult:
        """Проверяет ликвидность пула и price impact ордера.

        Args:
            context: Контекст фильтрации.

        Returns:
            Успех при достаточной ликвидности и допустимом price impact,
            иначе отказ с описанием причины и метриками.
        """
        pool = context.pool
        check = self.check_pool_liquidity(context)

        if pool.base_reserve <= 0 or pool.quote_reserve <= 0:
            return self._reject(
                "пул без резервов",
                pool_sol=check.pool_sol,
                base_reserve=pool.base_reserve,
                quote_reserve=pool.quote_reserve,
            )

        if check.pool_sol < self._min_pool_sol:
            return self._reject(
                "недостаточная ликвидность пула",
                pool_sol=round(check.pool_sol, 4),
                min_pool_sol=self._min_pool_sol,
            )

        if check.price_impact_pct > self._max_price_impact_pct:
            return self._reject(
                "price impact превышает предел",
                price_impact_pct=round(check.price_impact_pct, 4),
                max_price_impact_pct=self._max_price_impact_pct,
            )

        return self._ok(
            pool_sol=round(check.pool_sol, 4),
            price_impact_pct=round(check.price_impact_pct, 4),
            expected_tokens=check.expected_tokens,
            min_tokens_out=check.min_tokens_out,
            buy_slippage_pct=self._buy_slippage_pct,
        )
