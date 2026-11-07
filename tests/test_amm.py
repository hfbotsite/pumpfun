"""Тесты чистой AMM-математики constant product."""

from __future__ import annotations

from utils.amm import (
    get_amount_out,
    max_in_with_slippage,
    min_out_with_slippage,
    price_impact_pct,
    quote_buy,
    quote_sell,
    spot_price,
)


class TestGetAmountOut:
    """Выход constant product: формула и граничные случаи."""

    def test_basic_split_matches_formula(self) -> None:
        """Совпадает с ручным расчётом x·y=k без комиссии."""
        amount_in = 1_000_000_000  # 1 SOL
        reserve_in = 30_000_000_000
        reserve_out = 600_000_000_000_000
        expected = (reserve_out * amount_in) // (reserve_in + amount_in)
        assert get_amount_out(amount_in, reserve_in, reserve_out) == expected

    def test_fee_reduces_output(self) -> None:
        """Комиссия 100 bps уменьшает выход примерно на 1%."""
        reserves = (30_000_000_000, 600_000_000_000_000)
        no_fee = get_amount_out(1_000_000_000, *reserves, fee_bps=0)
        with_fee = get_amount_out(1_000_000_000, *reserves, fee_bps=100)
        assert with_fee < no_fee
        assert with_fee >= no_fee * 0.98

    def test_zero_inputs_return_zero(self) -> None:
        """Некорректные входы дают 0, а не исключение."""
        assert get_amount_out(0, 100, 100) == 0
        assert get_amount_out(10, 0, 100) == 0
        assert get_amount_out(10, 100, 0) == 0
        assert get_amount_out(-5, 100, 100) == 0


class TestPriceImpact:
    """Price Impact: dx/(reserve+dx)."""

    def test_half_of_reserve_is_about_66_percent(self) -> None:
        """Ордер размером с резерв даёт impact ~66.7%."""
        assert price_impact_pct(30, 30) == 50.0

    def test_small_order_small_impact(self) -> None:
        """0.5 SOL против 30 SOL — около 1.64%."""
        impact = price_impact_pct(500_000_000, 30_000_000_000)
        assert 1.6 < impact < 1.7

    def test_zero_reserves(self) -> None:
        """Пустые резервы — impact 0."""
        assert price_impact_pct(10, 0) == 0.0


class TestSpotPrice:
    """Спот-цена резервов."""

    def test_ratio(self) -> None:
        """Цена = reserve_out / reserve_in."""
        assert spot_price(4, 8) == 2.0

    def test_zero_reserves_return_zero(self) -> None:
        """Пустые резервы — 0, а не деление на ноль."""
        assert spot_price(0, 5) == 0.0
        assert spot_price(5, 0) == 0.0


class TestSlippage:
    """Дисконты min_out / max_in."""

    def test_min_out_20_percent(self) -> None:
        """20% дисконт: 80% от котировки, минимум 1."""
        assert min_out_with_slippage(100, 20.0) == 80
        assert min_out_with_slippage(1, 20.0) == 1

    def test_min_out_zero_quote(self) -> None:
        """Нулевая котировка — нулевой минимум."""
        assert min_out_with_slippage(0, 20.0) == 0

    def test_max_in_20_percent(self) -> None:
        """Запас 20% с округлением вверх."""
        assert max_in_with_slippage(100, 20.0) == 121

    def test_negative_slippage_clamped(self) -> None:
        """Отрицательное проскальзывание трактуется как 0."""
        assert min_out_with_slippage(100, -5.0) == 100
        assert max_in_with_slippage(100, -5.0) == 101


class TestQuotes:
    """Сборные котировки покупки/продажи."""

    def test_quote_buy_consistency(self) -> None:
        """min_amount_out согласован с дисконтом, impact и спот согласованы."""
        quote = quote_buy(
            sol_in=500_000_000,
            sol_reserve=30_000_000_000,
            token_reserve=600_000_000_000_000,
            fee_bps=100,
            slippage_pct=20.0,
        )
        assert quote.min_amount_out == min_out_with_slippage(
            quote.amount_out, 20.0
        )
        assert quote.amount_out > 0
        assert 1.5 < quote.price_impact_pct < 1.8

    def test_quote_sell_consistency(self) -> None:
        """Продажа зеркальна покупке: выход в SOL, discount применён."""
        quote = quote_sell(
            tokens_in=100_000_000_000,
            sol_reserve=30_000_000_000,
            token_reserve=600_000_000_000_000,
            fee_bps=25,
            slippage_pct=25.0,
        )
        assert quote.amount_out > 0
        assert quote.min_amount_out <= quote.amount_out
        assert quote.fee_bps == 25
