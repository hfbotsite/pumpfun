"""Тесты ручных операций дашборда: закрытие, усреднение, аутентификация."""

from __future__ import annotations

import time

import pytest

from core.persistence import PortfolioStore
from core.portfolio import PortfolioError, PortfolioManager
from core.trader import MomentumTrader
from models.trade_data import DEXType, OrderResult, TradeSide
from strategies.momentum_pump import MomentumPumpStrategy
from tests.conftest import BASE_MINT, POOL_ADDRESS, make_pool
from tests.test_execution import (
    LadderExecutor,
    _confirmed_sell,
    _failed_sell,
    make_trader,  # noqa: F401 - переиспользуем фабрику
)


def buy_result(tokens: float) -> OrderResult:
    """Успешная подтверждённая покупка.

    Args:
        tokens: Сколько токенов куплено.

    Returns:
        :class:`~models.trade_data.OrderResult`.
    """
    return OrderResult(
        success=True,
        side=TradeSide.BUY,
        base_mint=BASE_MINT,
        pool_address=POOL_ADDRESS,
        signature="sig-buy",
        sol_amount=0.5,
        token_amount=tokens,
        confirmed=True,
    )


class TestAddToPosition:
    """Усреднение на уровне портфеля."""

    def test_weighted_average_entry(self) -> None:
        """Новая цена входа — средневзвешенная по SOL."""
        manager = PortfolioManager()
        manager.open_position(
            base_mint=BASE_MINT,
            pool_address=POOL_ADDRESS,
            dex=DEXType.PUMP_FUN,
            entry_price=1.0,
            entry_sol=0.5,
            token_amount=1_000_000,
            opened_at=1000.0,
        )
        updated = manager.add_to_position(
            BASE_MINT, added_tokens=500_000, added_sol=0.75
        )
        assert updated is not None
        assert updated.entry_sol == 1.25
        assert updated.token_amount == 1_500_000
        assert updated.initial_token_amount == 1_500_000
        assert updated.entry_price == 1.25 / 1_500_000

    def test_missing_position_returns_none(self) -> None:
        """Усреднять несуществующую позицию нельзя."""
        manager = PortfolioManager()
        assert (
            manager.add_to_position(BASE_MINT, added_tokens=1, added_sol=1) is None
        )

    def test_invalid_addition_raises(self) -> None:
        """Неположительное добавление — PortfolioError."""
        manager = PortfolioManager()
        manager.open_position(
            base_mint=BASE_MINT,
            pool_address=POOL_ADDRESS,
            dex=DEXType.PUMP_FUN,
            entry_price=1.0,
            entry_sol=0.5,
            token_amount=1_000_000,
            opened_at=1000.0,
        )
        try:
            manager.add_to_position(BASE_MINT, added_tokens=0, added_sol=1)
        except PortfolioError:
            pass
        else:
            raise AssertionError("ожидался PortfolioError")

    def test_persisted_in_db(self, tmp_path) -> None:
        """Усреднение write-through: БД хранит новую цену и объём."""
        store = PortfolioStore(tmp_path / "db.sqlite")
        manager = PortfolioManager(store=store)
        manager.open_position(
            base_mint=BASE_MINT,
            pool_address=POOL_ADDRESS,
            dex=DEXType.PUMP_FUN,
            entry_price=1.0,
            entry_sol=0.5,
            token_amount=1_000_000,
            opened_at=1000.0,
        )
        manager.add_to_position(BASE_MINT, added_tokens=500_000, added_sol=0.75)
        loaded = store.load_open_positions()[0]
        assert loaded.entry_price == 1.25 / 1_500_000
        assert loaded.entry_sol == 1.25
        store.close()


class ManualExecutor(LadderExecutor):
    """Исполнитель с раздельными очередями результатов buy/sell."""

    def __init__(self) -> None:
        """Пустые очереди: заполняются в тесте."""
        self.buy_results: list[OrderResult] = []
        self.buy_orders: list[object] = []
        self.sell_results: list[OrderResult] = []
        self.sell_calls = 0

    async def execute_buy(self, order) -> OrderResult:
        """Отдаёт следующий buy-результат.

        Args:
            order: Заявка на покупку.

        Returns:
            Заготовленный результат (последний, если очередь пуста).
        """
        self.buy_orders.append(order)
        result = self.buy_results[-1] if self.buy_results else buy_result(500_000)
        return result

    async def execute_sell(self, order) -> OrderResult:
        """Отдаёт следующий sell-результат по индексу вызова.

        Args:
            order: Заявка на продажу.

        Returns:
            Заготовленный результат.
        """
        index = min(self.sell_calls, len(self.sell_results) - 1)
        self.sell_calls += 1
        return self.sell_results[index]

    async def close(self) -> None:
        """Совместимость с контрактом исполнителя."""


def make_manual_trader(executor: ManualExecutor) -> MomentumTrader:
    """Трейдер с открытой позицией для ручных операций.

    Args:
        executor: Фейковый исполнитель.

    Returns:
        :class:`~core.trader.MomentumTrader`.
    """
    trader = MomentumTrader(
        executor=executor,
        portfolio=PortfolioManager(),
        strategy=MomentumPumpStrategy(),
        trade_amount_sol=0.5,
    )
    trader.portfolio.open_position(
        base_mint=BASE_MINT,
        pool_address=POOL_ADDRESS,
        dex=DEXType.PUMP_FUN,
        entry_price=1.0,
        entry_sol=0.5,
        token_amount=1_000_000,
        opened_at=1000.0,
    )
    return trader


class TestManualSell:
    """Кнопка «Закрыть сделку»."""

    async def test_success_closes_position(self) -> None:
        """Успешная продажа удаляет позицию и пишет сделку с manual."""
        executor = ManualExecutor()
        executor.sell_results = [_confirmed_sell(0.4, 1_000_000)]
        trader = make_manual_trader(executor)
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is True
        assert trader.portfolio.open_count == 0
        trade = trader.portfolio.closed_trades[-1]
        assert trade.exit_reason == "manual"
        # Цена выхода в правильных единицах: 0.4 SOL / 1e6 токенов = 4e-7.
        assert trade.exit_price == pytest.approx(4e-7, rel=1e-9)

    async def test_failure_keeps_position(self) -> None:
        """Неуспешная продажа оставляет позицию открытой."""
        executor = ManualExecutor()
        executor.sell_results = [_failed_sell(), _failed_sell()]
        trader = make_manual_trader(executor)
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert trader.portfolio.open_count == 1

    async def test_missing_position(self) -> None:
        """Продажа без позиции (давно закрытой) — ошибка, исполнитель не дёргается."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        trader.portfolio.force_close(
            BASE_MINT, exit_price=1.0, closed_at=time.time() - 3600.0
        )
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert "не найдена" in str(result["error"])

    async def test_write_through_db(self, tmp_path) -> None:
        """После ручного закрытия в БД нет «зависшей» строки позиции."""
        store = PortfolioStore(tmp_path / "db.sqlite")
        executor = ManualExecutor()
        executor.sell_results = [_confirmed_sell(0.4, 1_000_000)]
        trader = MomentumTrader(
            executor=executor,
            portfolio=PortfolioManager(store=store),
            strategy=MomentumPumpStrategy(),
            trade_amount_sol=0.5,
        )
        trader.portfolio.open_position(
            base_mint=BASE_MINT,
            pool_address=POOL_ADDRESS,
            dex=DEXType.PUMP_FUN,
            entry_price=1.0,
            entry_sol=0.5,
            token_amount=1_000_000,
            opened_at=1000.0,
        )
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is True
        assert store.count_open_positions() == 0
        summary = store.closed_trades_summary()
        assert summary["closed_trades"] == 1
        store.close()


class TestManualBuy:
    """Кнопка «Усредниться»."""

    async def test_average_updates_position(self) -> None:
        """Успешная покупка добавляет токены и пересчитывает цену входа."""
        executor = ManualExecutor()
        executor.buy_results = [buy_result(500_000)]
        trader = make_manual_trader(executor)
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is True
        position = trader.portfolio.get(BASE_MINT)
        assert position.token_amount == 1_500_000
        assert position.entry_sol == 1.0
        assert position.entry_price == 1.0 / 1_500_000

    async def test_failed_buy_keeps_position(self) -> None:
        """Неуспешная покупка не меняет позицию."""
        executor = ManualExecutor()
        executor.buy_results = [_failed_sell()]  # неуспешный результат любого side
        trader = make_manual_trader(executor)
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is False
        position = trader.portfolio.get(BASE_MINT)
        assert position.token_amount == 1_000_000
        assert position.entry_price == 1.0

    async def test_average_requires_position(self) -> None:
        """Усреднение без позиции (давно закрытой) запрещено."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        trader.portfolio.force_close(
            BASE_MINT, exit_price=1.0, closed_at=time.time() - 3600.0
        )
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert "открытую позицию" in str(result["error"])


class TestAlreadyClosed:
    """Клик по позиции, которую только что закрыла стратегия."""

    async def test_sell_recently_closed_returns_note(self) -> None:
        """manual_sell после авто-закрытия — ok=True, «уже закрыта»."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        closed = trader.portfolio.force_close(
            BASE_MINT, exit_price=1.0, exit_reason="volume_exhaustion"
        )
        assert closed is not None
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is True
        assert result["already_closed"] is True
        assert "уже закрыта" in str(result["note"])
        assert "volume_exhaustion" in str(result["note"])
        assert not executor.sell_calls

    async def test_sell_old_closed_is_error(self) -> None:
        """Закрытие давно (вне окна) — обычная ошибка «не найдена»."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        trader.portfolio.force_close(
            BASE_MINT, exit_price=1.0, closed_at=time.time() - 3600.0
        )
        result = await trader.manual_sell(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert "не найдена" in str(result["error"])

    async def test_buy_recently_closed_reports_reason(self) -> None:
        """manual_buy после авто-закрытия — понятная ошибка, не «нет позиции»."""
        executor = ManualExecutor()
        trader = make_manual_trader(executor)
        trader.portfolio.force_close(
            BASE_MINT, exit_price=1.0, exit_reason="take_profit_1"
        )
        result = await trader.manual_buy(BASE_MINT, make_pool())
        assert result["ok"] is False
        assert "уже закрыта" in str(result["error"])
        assert not executor.buy_orders
