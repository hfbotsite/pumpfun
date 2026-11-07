"""Тесты фильтров допуска: ликвидность, безопасность, цепочка."""

from __future__ import annotations

from filters.base_filter import FilterChain, FilterContext
from filters.liquidity_guard import LiquidityGuard
from filters.security_guard import SecurityGuard
from models.trade_data import MomentumMetrics, TokenSecurity
from strategies.momentum_pump import MomentumPumpStrategy
from tests.conftest import make_pool


def make_context(
    pool=None,
    security: TokenSecurity | None = None,
    metrics: MomentumMetrics | None = None,
    trade_amount_sol: float = 0.5,
) -> FilterContext:
    """Собирает контекст фильтрации с хорошим пулом по умолчанию.

    Args:
        pool: Снимок пула (по умолчанию фабричный).
        security: Данные безопасности.
        metrics: Метрики импульса.
        trade_amount_sol: Размер входного ордера.

    Returns:
        :class:`~filters.base_filter.FilterContext`.
    """
    return FilterContext(
        pool=pool if pool is not None else make_pool(),
        trade_amount_sol=trade_amount_sol,
        security=security,
        metrics=metrics,
        timestamp=1000.0,
    )


def clean_security() -> TokenSecurity:
    """Безопасный контракт: authorities сняты, LP сожжён на 100%."""
    return TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0)


class TestLiquidityGuard:
    """Проверки ликвидности пула и price impact."""

    def test_good_pool_passes(self) -> None:
        """Пул 40 SOL и ордер 0.5 SOL проходят с чистой котировкой."""
        guard = LiquidityGuard()
        result = guard.evaluate(make_context())
        assert result.passed

    def test_insufficient_liquidity_rejected(self) -> None:
        """Пул ниже MIN_POOL_SOL отклоняется с причиной."""
        poor_pool = make_pool(quote_reserve=10_000_000_000)
        guard = LiquidityGuard()
        result = guard.evaluate(make_context(pool=poor_pool))
        assert not result.passed
        assert "ликвидн" in result.reason.lower()

    def test_price_impact_rejected(self) -> None:
        """Ордер с impact выше порога отклоняется (ликвидность достаточна)."""
        # 4 SOL против 40 SOL: impact ~9.1% > 5%, ликвидность 40 >= 30.
        guard = LiquidityGuard()
        result = guard.evaluate(make_context(trade_amount_sol=4.0))
        assert not result.passed
        assert "impact" in result.reason.lower()

    def test_min_tokens_out_discounted(self) -> None:
        """min_tokens_out равен котировке с дисконтом 20%."""
        guard = LiquidityGuard()
        context = make_context()
        check = guard.check_pool_liquidity(context)
        expected = int(check.quote.amount_out * 0.8)
        assert check.min_tokens_out <= expected + 1
        assert check.min_tokens_out >= expected - 1


class TestSecurityGuard:
    """Проверки безопасности контракта."""

    def test_clean_contract_passes(self) -> None:
        """Снятые authorities и сожжённый LP проходят."""
        guard = SecurityGuard()
        result = guard.evaluate(make_context(security=clean_security()))
        assert result.passed

    def test_missing_security_rejected_fail_closed(self) -> None:
        """Нет данных безопасности — отказ (fail-closed)."""
        guard = SecurityGuard()
        result = guard.evaluate(make_context(security=None))
        assert not result.passed

    def test_mint_authority_active_rejected(self) -> None:
        """Активный mint authority — отказ."""
        security = clean_security().model_copy(
            update={"mint_authority": "Auth111111111111111111111111111111111111"}
        )
        guard = SecurityGuard()
        result = guard.evaluate(make_context(security=security))
        assert not result.passed
        assert "mint" in result.reason.lower()

    def test_freeze_authority_active_rejected(self) -> None:
        """Активный freeze authority — отказ."""
        security = clean_security().model_copy(
            update={"freeze_authority": "Freeze111111111111111111111111111111111"}
        )
        guard = SecurityGuard()
        result = guard.evaluate(make_context(security=security))
        assert not result.passed
        assert "freeze" in result.reason.lower()

    def test_low_lp_burn_rejected(self) -> None:
        """LP сожжён ниже 90% — отказ."""
        security = clean_security().model_copy(update={"lp_burned_pct": 50.0})
        guard = SecurityGuard()
        result = guard.evaluate(make_context(security=security))
        assert not result.passed
        assert "LP" in result.reason


class TestFilterChain:
    """Цепочка фильтров: порядок и fail-fast."""

    def test_chain_passes_all_good(self) -> None:
        """Чистый контекст проходит обе ступени."""
        chain = FilterChain([LiquidityGuard(), SecurityGuard()])
        results = chain.run(make_context(security=clean_security()))
        assert all(result.passed for result in results)

    def test_chain_stops_on_first_failure(self) -> None:
        """При отказе ликвидности безопасность не проверяется дальше."""
        poor_pool = make_pool(quote_reserve=1_000_000_000)
        chain = FilterChain([LiquidityGuard(), SecurityGuard()])
        results = chain.run(make_context(pool=poor_pool, security=clean_security()))
        assert len(results) == 1
        assert not results[0].passed
        assert results[0].filter_name == "liquidity_guard"

    def test_entry_held_when_security_missing(self) -> None:
        """Стратегия не входит без данных безопасности (сквозной сценарий)."""
        metrics = MomentumMetrics(
            window_seconds=5.0,
            buy_count=8,
            sell_count=2,
            buy_volume_sol=2.5,
            last_price=5e-9,
        )
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(make_context(security=None, metrics=metrics))
        assert signal.action.value == "hold"
        assert "security_guard" in signal.reason
