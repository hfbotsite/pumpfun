"""Тесты исполнения: лестница выхода, compose-инструкции, дефолты настроек."""

from __future__ import annotations

import pytest
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from config.settings import Settings
from core.executor import (
    BuyOrder,
    ExecutorError,
    OrderExecutor,
    SellOrder,
)
from core.portfolio import PortfolioManager
from core.trader import MomentumTrader
from models.trade_data import DEXType, OrderResult, TradeSide
from strategies.momentum_pump import MomentumPumpStrategy
from tests.conftest import BASE_MINT, POOL_ADDRESS, make_event, make_pool
from utils.constants import JITO_TIP_ACCOUNTS, MEMO_PROGRAM_ID


def _failed_sell() -> OrderResult:
    """Неуспешная продажа (транзакция заведомо не отправлена)."""
    return OrderResult.failed(
        side=TradeSide.SELL,
        base_mint=BASE_MINT,
        pool_address=POOL_ADDRESS,
        error="минимальный выход не достигнут",
    )


def _confirmed_sell(sol_amount: float, token_amount: float) -> OrderResult:
    """Успешная подтверждённая продажа."""
    return OrderResult(
        success=True,
        side=TradeSide.SELL,
        base_mint=BASE_MINT,
        pool_address=POOL_ADDRESS,
        signature="sig-sell",
        sol_amount=sol_amount,
        token_amount=token_amount,
        confirmed=True,
    )


class LadderExecutor:
    """Фейковый исполнитель: отдаёт заготовленные результаты по очереди."""

    def __init__(self, results: list[OrderResult]) -> None:
        """Сохраняет последовательность результатов.

        Args:
            results: Результаты по порядку вызовов execute_sell.
        """
        self.results = list(results)
        self.orders: list[SellOrder] = []
        self.calls = 0

    async def execute_buy(self, order: BuyOrder) -> OrderResult:
        """Покупки в этих тестах не используются.

        Args:
            order: Заявка на покупку (игнорируется).

        Raises:
            AssertionError: Всегда.
        """
        raise AssertionError("execute_buy не ожидается в тесте лестницы")

    async def execute_sell(self, order: SellOrder) -> OrderResult:
        """Отдаёт следующий результат, фиксируя заявку.

        Args:
            order: Заявка на продажу.

        Returns:
            Заготовленный результат.
        """
        self.orders.append(order)
        index = min(self.calls, len(self.results) - 1)
        self.calls += 1
        return self.results[index]

    async def close(self) -> None:
        """Совместимость с контрактом исполнителя."""


def make_trader(executor: LadderExecutor) -> MomentumTrader:
    """Собирает трейдер с открытой позицией и стопом 12%.

    Args:
        executor: Фейковый исполнитель.

    Returns:
        Готовый :class:`~core.trader.MomentumTrader`.
    """
    trader = MomentumTrader(
        executor=executor,
        portfolio=PortfolioManager(),
        strategy=MomentumPumpStrategy(hard_stop_loss_pct=12.0),
        trade_amount_sol=0.5,
    )
    trader.portfolio.open_position(
        base_mint=BASE_MINT,
        pool_address=POOL_ADDRESS,
        dex=DEXType.PUMP_FUN,
        entry_price=1.0,
        entry_sol=0.5,
        token_amount=1e8,
        opened_at=1000.0,
    )
    return trader


async def stop_loss_event() -> object:
    """Событие продажи с ценой -15% от входа (пробой стопа).

    Returns:
        :class:`~models.trade_data.TradeEvent`.
    """
    return make_event(
        sol_amount=0.85e8,
        token_amount=1e8,
        timestamp=1001.0,
        signature="stop-sig",
    )


class TestSellLadder:
    """Лестница выхода: 25% → floor 0 при неудаче."""

    async def test_failed_sell_retries_with_zero_floor(self) -> None:
        """Неудачная продажа повторяется с нулевым порогом и закрывает позицию."""
        executor = LadderExecutor([_failed_sell(), _confirmed_sell(0.35, 1e8)])
        trader = make_trader(executor)
        event = await stop_loss_event()
        decision = await trader.process_event(event, make_pool(), creator=None)

        assert executor.calls == 2
        assert executor.orders[0].quote.min_amount_out > 0
        assert executor.orders[1].quote.min_amount_out == 0
        assert decision.executed
        assert trader.portfolio.open_count == 0
        assert trader.portfolio.stats.trades == 1

    async def test_unconfirmed_success_does_not_retry(self) -> None:
        """Успех без подтверждения НЕ повторяется (риск двойной продажи)."""
        result = _confirmed_sell(0.35, 1e8).model_copy(update={"confirmed": False})
        executor = LadderExecutor([result])
        trader = make_trader(executor)
        event = await stop_loss_event()
        decision = await trader.process_event(event, make_pool(), creator=None)

        assert executor.calls == 1
        assert not decision.executed
        assert trader.portfolio.open_count == 1

    async def test_double_failure_keeps_position_and_blocks(self) -> None:
        """Обе попытки неудачны — позиция остаётся, пул на паузе."""
        executor = LadderExecutor([_failed_sell(), _failed_sell()])
        trader = make_trader(executor)
        event = await stop_loss_event()
        decision = await trader.process_event(event, make_pool(), creator=None)

        assert executor.calls == 2
        assert not decision.executed
        assert trader.portfolio.open_count == 1
        assert trader._blocked_until.get(POOL_ADDRESS, 0.0) > event.timestamp


class TestComposeInstructions:
    """Сборка инструкций для каналов rpc и jito."""

    def make_executor(self, venue: str) -> OrderExecutor:
        """Создаёт исполнитель с ключом-заглушкой.

        Args:
            venue: Канал отправки.

        Returns:
            :class:`~core.executor.OrderExecutor`.
        """
        return OrderExecutor(None, Keypair(), venue=venue)

    def test_rpc_channel_has_no_memo_and_tip(self) -> None:
        """rpc: только Compute Budget + свап, без memo и чаевых."""
        executor = self.make_executor("rpc")
        swap = [Instruction(Pubkey.new_unique(), b"", [])]
        instructions = executor._compose_instructions(swap, None)
        assert len(instructions) == len(swap) + 2  # CU limit + CU price
        programs = {str(i.program_id) for i in instructions}
        assert MEMO_PROGRAM_ID not in programs

    def test_jito_channel_has_memo_and_tip(self) -> None:
        """jito: memo первым, чаевые последними."""
        executor = self.make_executor("jito")
        swap = [Instruction(Pubkey.new_unique(), b"", [])]
        instructions = executor._compose_instructions(swap, JITO_TIP_ACCOUNTS[0])
        assert len(instructions) == len(swap) + 4  # memo + 2 CU + tip
        assert str(instructions[0].program_id) == MEMO_PROGRAM_ID

    def test_unknown_venue_rejected(self) -> None:
        """Неизвестный канал — ошибка на этапе конструктора."""

        with pytest.raises(ExecutorError):
            OrderExecutor(None, Keypair(), venue="carrier_pigeon")


class TestDefaults:
    """Новые дефолты конфигурации (без влияния .env)."""

    def test_risk_defaults(self) -> None:
        """Стоп 30%, холд 180 c, канал rpc, приоритетка по практике рынка."""
        assert Settings.model_fields["hard_stop_loss_pct"].default == 30.0
        assert Settings.model_fields["max_hold_seconds"].default == 180.0
        assert Settings.model_fields["execution_venue"].default == "rpc"
        assert Settings.model_fields["use_jupiter_fallback"].default is False
        assert Settings.model_fields["priority_fee_micro_lamports"].default == 1_000_000
        assert Settings.model_fields["buy_slippage_pct"].default == 15.0
        assert Settings.model_fields["min_real_pool_sol"].default == 30.0

    def test_operations_defaults(self) -> None:
        """Поведенческие значения выведены в конфиг (не захардкожены)."""
        assert Settings.model_fields["retry_cooldown_seconds"].default == 2.0
        assert Settings.model_fields["maintenance_interval_seconds"].default == 60.0
        assert Settings.model_fields["stale_pool_seconds"].default == 600.0
        assert Settings.model_fields["security_ttl_seconds"].default == 30.0
        assert (
            Settings.model_fields["portfolio_dry_db_path"].default
            == "data/bot_dry.db"
        )

    def test_venue_normalization(self) -> None:
        """Канал отправки нормализует регистр."""
        settings = Settings(execution_venue=" JITO ")
        assert settings.execution_venue == "jito"
