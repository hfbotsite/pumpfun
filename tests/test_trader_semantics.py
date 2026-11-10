"""Тесты семантики входов: окно надёжности токена, авто-усреднение по
ENTER-сигналу, лимит входов на mint и свип устаревших позиций."""

from __future__ import annotations

import time
from typing import Any

import pytest

from core.pipeline import LivePipeline
from core.portfolio import PortfolioManager
from core.trader import MomentumTrader
from models.trade_data import DEXType, SignalAction, TradeSignal
from strategies.momentum_pump import MomentumPumpStrategy
from tests.conftest import BASE_MINT, POOL_ADDRESS, make_event, make_pool
from tests.test_execution import _confirmed_sell, _failed_sell
from tests.test_manual_actions import ManualExecutor, buy_result, make_manual_trader
from tests.test_strategy import clean_security

ENTER_SIGNAL = TradeSignal(
    action=SignalAction.ENTER,
    base_mint=BASE_MINT,
    pool_address=POOL_ADDRESS,
    dex=DEXType.PUMP_FUN,
    reason="тест: всегда вход",
    timestamp=1000.0,
)


async def clean_provider(_pool: object) -> object:
    """Провайдер безопасности: чистый контракт (гейты пропускают).

    Args:
        _pool: Снимок пула (игнорируется).

    Returns:
        :class:`~models.trade_data.TokenSecurity` без authority и с сожжённым LP.
    """
    return clean_security()


def hot_event(timestamp: float = 1000.0) -> object:
    """Горячее событие покупки: 2.5 SOL за окно (velocity ≥ 1.5).

    Args:
        timestamp: Unix ts события.

    Returns:
        :class:`~models.trade_data.TradeEvent`.
    """
    return make_event(sol_amount=2.5, token_amount=500_000, timestamp=timestamp)


class AlwaysEnterStrategy(MomentumPumpStrategy):
    """Стратегия, всегда дающая ENTER (для проверки маршрутизации)."""

    def evaluate_entry(self, context: Any) -> TradeSignal:
        """Возвращает готовый ENTER-сигнал.

        Args:
            context: Контекст фильтров (игнорируется).

        Returns:
            :class:`~models.trade_data.TradeSignal` с ``ENTER``.
        """
        return ENTER_SIGNAL


def make_fresh_trader(
    executor: ManualExecutor, **kwargs: Any
) -> MomentumTrader:
    """Трейдер без открытых позиций.

    Args:
        executor: Фейковый исполнитель.
        **kwargs: Дополнительные параметры :class:`MomentumTrader`.

    Returns:
        :class:`~core.trader.MomentumTrader`.
    """
    return MomentumTrader(
        executor=executor,
        portfolio=PortfolioManager(),
        strategy=MomentumPumpStrategy(min_entry_buy_count=1),
        security_provider=clean_provider,
        trade_amount_sol=0.5,
        **kwargs,
    )


class TestTokenReliabilityGate:
    """Вход разрешён только по токенам старше окна надёжности."""

    async def test_young_token_blocked(self) -> None:
        """Токен младше TOKEN_RELIABLE_SECONDS — вход заблокирован."""
        executor = ManualExecutor()
        trader = make_fresh_trader(executor, token_reliable_seconds=60.0)
        pool = make_pool(created_at=1000.0)
        decision = await trader.process_event(
            hot_event(timestamp=1005.0), pool
        )
        assert decision.action is SignalAction.HOLD
        assert "надёжности" in decision.reason
        assert trader.portfolio.open_count == 0
        assert not executor.buy_orders

    async def test_mature_token_enters(self) -> None:
        """Токен старше окна — вход проходит."""
        executor = ManualExecutor()
        trader = make_fresh_trader(executor, token_reliable_seconds=60.0)
        pool = make_pool(created_at=900.0)
        decision = await trader.process_event(
            hot_event(timestamp=1000.0), pool
        )
        assert decision.action is SignalAction.ENTER
        assert trader.portfolio.open_count == 1
        assert len(executor.buy_orders) == 1

    async def test_unknown_age_allowed(self) -> None:
        """created_at=0 (возраст неизвестен) — гейт не блокирует."""
        executor = ManualExecutor()
        trader = make_fresh_trader(executor, token_reliable_seconds=60.0)
        pool = make_pool(created_at=0.0)
        decision = await trader.process_event(
            hot_event(timestamp=1000.0), pool
        )
        assert decision.action is SignalAction.ENTER


class TestAutoAverage:
    """ENTER-сигнал по открытой позиции = усреднение.

    Цена входа позиций = 5e-6 (цена события) — иначе срабатывают
    стоп-лосс/тейк-профит вместо удержания и маршрутизации на усреднение.
    """

    def open_position(
        self,
        trader: MomentumTrader,
        *,
        entries_count: int = 1,
    ) -> None:
        """Открывает позицию по цене события.

        Args:
            trader: Движок с портфелем.
            entries_count: Стартовое число входов.
        """
        position = trader.portfolio.open_position(
            base_mint=BASE_MINT,
            pool_address=POOL_ADDRESS,
            dex=DEXType.PUMP_FUN,
            entry_price=5e-6,
            entry_sol=0.5,
            token_amount=1_000_000,
            opened_at=1000.0,
        )
        assert position is not None
        position.entries_count = entries_count

    async def test_enter_signal_averages(self) -> None:
        """Сильный сигнал по открытой позиции добавляет к позиции."""
        executor = ManualExecutor()
        executor.buy_results = [buy_result(500_000)]
        trader = MomentumTrader(
            executor=executor,
            portfolio=PortfolioManager(),
            strategy=AlwaysEnterStrategy(),
            security_provider=clean_provider,
            trade_amount_sol=0.5,
            token_reliable_seconds=60.0,
        )
        self.open_position(trader)
        pool = make_pool(created_at=900.0)
        decision = await trader.process_event(
            hot_event(timestamp=1000.0), pool
        )
        assert decision.action is SignalAction.ENTER
        assert "усреднение" in decision.reason
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        assert position.token_amount == 1_500_000
        assert position.entries_count == 2

    async def test_average_respects_limit(self) -> None:
        """При исчерпанном лимите входов усреднения больше нет."""
        executor = ManualExecutor()
        trader = MomentumTrader(
            executor=executor,
            portfolio=PortfolioManager(),
            strategy=AlwaysEnterStrategy(),
            security_provider=clean_provider,
            trade_amount_sol=0.5,
            max_entries_per_mint=2,
        )
        self.open_position(trader, entries_count=2)
        pool = make_pool(created_at=900.0)
        decision = await trader.process_event(
            hot_event(timestamp=1000.0), pool
        )
        assert decision.action is SignalAction.HOLD
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        assert position.token_amount == 1_000_000
        assert not executor.buy_orders

    async def test_failed_buy_keeps_position(self) -> None:
        """Неудачный ордер усреднения не меняет позицию, пул блокируется."""
        executor = ManualExecutor()
        executor.buy_results = [_failed_sell()]
        trader = MomentumTrader(
            executor=executor,
            portfolio=PortfolioManager(),
            strategy=AlwaysEnterStrategy(),
            security_provider=clean_provider,
            trade_amount_sol=0.5,
        )
        self.open_position(trader)
        pool = make_pool(created_at=900.0)
        decision = await trader.process_event(
            hot_event(timestamp=1000.0), pool
        )
        assert decision.action is SignalAction.ENTER
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        assert position.token_amount == 1_000_000
        assert position.entries_count == 1


class TestEntriesLimitManualBuy:
    """Лимит входов для ручного усреднения."""

    async def test_manual_buy_limit_reached(self) -> None:
        """manual_buy при исчерпанном лимите — отказ."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        position.entries_count = 10
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert "лимит входов" in str(result["error"])
        assert not executor.buy_orders

    async def test_manual_buy_within_limit(self) -> None:
        """manual_buy внутри лимита работает."""
        executor = ManualExecutor()
        executor.buy_results = [buy_result(500_000)]
        trader = make_manual_trader(executor)
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is True
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        assert position.entries_count == 2


class TestSweepStale:
    """Свип позиций старше MAX_HOLD."""

    def make_pipeline(
        self, trader: MomentumTrader
    ) -> LivePipeline:
        """Собирает dry-run пайплайн с подменённым пулом.

        Args:
            trader: Движок с позицией.

        Returns:
            :class:`~core.pipeline.LivePipeline` без сети.
        """

        async def fake_pool_for_mint(_mint: str) -> tuple[Any, Any, Any]:
            return (make_pool(created_at=0.0), None, None)

        pipeline = LivePipeline(dry_run=True)
        pipeline._trader = trader
        pipeline._pool_for_mint = fake_pool_for_mint  # type: ignore[method-assign]
        return pipeline

    async def test_unsellable_stale_closed_as_loss(self) -> None:
        """Позу старше MAX_HOLD нечего продавать → полный убыток."""
        executor = ManualExecutor()
        executor.sell_results = [_failed_sell(), _failed_sell()]
        trader = make_manual_trader(executor)  # opened_at=1000 → давно устарела
        pipeline = self.make_pipeline(trader)
        await pipeline._sweep_stale_positions(trader)
        assert trader.portfolio.open_count == 0
        trade = trader.portfolio.closed_trades[-1]
        assert trade.exit_reason == "max_hold_unsellable"
        # Полный убыток: получили 0, вложили 0.5 SOL.
        assert trade.pnl_sol == pytest.approx(-0.5)

    async def test_sellable_stale_sold(self) -> None:
        """Если пул жив — свип продаёт позицию обычной лестницей."""
        executor = ManualExecutor()
        executor.sell_results = [_confirmed_sell(0.4, 1_000_000)]
        trader = make_manual_trader(executor)
        pipeline = self.make_pipeline(trader)
        await pipeline._sweep_stale_positions(trader)
        assert trader.portfolio.open_count == 0
        trade = trader.portfolio.closed_trades[-1]
        assert trade.exit_reason == "max_hold"
        # 0.4 SOL вернулось из 0.5 вложенных — убыток 0.1, но через продажу.
        assert trade.pnl_sol == pytest.approx(-0.1)

    async def test_fresh_position_untouched(self) -> None:
        """Свежая позиция (моложе MAX_HOLD) свипом не трогается."""
        executor = ManualExecutor()
        executor.sell_results = [_failed_sell(), _failed_sell()]
        trader = make_manual_trader(executor)
        position = trader.portfolio.get(BASE_MINT)
        assert position is not None
        position.opened_at = time.time()
        pipeline = self.make_pipeline(trader)
        await pipeline._sweep_stale_positions(trader)
        assert trader.portfolio.open_count == 1
        assert not executor.sell_calls
