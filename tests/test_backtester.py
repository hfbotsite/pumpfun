"""Тесты бэктестера: CSV-раунд-трип и прогон сценария через живую стратегию."""

from __future__ import annotations

import pytest

from analytics.backtester import Backtester
from analytics.replay import (
    ReplayError,
    load_events_csv,
    scenario_from_csv,
    write_events_csv,
)
from models.trade_data import DEXType, TokenSecurity, TradeEvent, TradeSide

POOL = "Pool1111111111111111111111111111111111111111"
MINT = "Mint11111111111111111111111111111111111111111"


def _event(
    side: TradeSide,
    sol_amount: float,
    token_amount: float,
    timestamp: float,
    signature: str,
) -> TradeEvent:
    """Строит событие сделки для сценария.

    Args:
        side: Направление.
        sol_amount: Объём, SOL.
        token_amount: Количество токенов.
        timestamp: Время, unix ts.
        signature: Уникальная подпись.

    Returns:
        :class:`~models.trade_data.TradeEvent`.
    """
    return TradeEvent(
        signature=signature,
        pool_address=POOL,
        base_mint=MINT,
        dex=DEXType.PUMP_FUN,
        side=side,
        sol_amount=sol_amount,
        token_amount=token_amount,
        wallet="W11111111111111111111111111111111111111111",
        timestamp=timestamp,
    )


def build_pump_events() -> list[TradeEvent]:
    """Сценарий: разгон импульса мелкими покупками, затем серия крупных.

    Returns:
        Список событий в хронологическом порядке.
    """
    events: list[TradeEvent] = []
    # Разгон: 4 покупки по 0.6 SOL (velocity 2.4 SOL/5c → вход на 3-й).
    # Количество токенов согласовано с пулом 90 SOL/600M (цена ~1.5e-7).
    for index, timestamp in enumerate((1.0, 1.2, 1.4, 1.6)):
        events.append(
            _event(TradeSide.BUY, 0.6, 4_000_000, timestamp, f"seed-{index}")
        )
    # Крупные покупки: цена события (sol/token) растёт ступенями, чтобы
    # достичь TP1 (+30%) и TP2 (+50%) на пуле с виртуальным резервом 90 SOL.
    push_tokens = (36_000_000, 30_000_000, 25_000_000)
    for index, (timestamp, tokens) in enumerate(
        zip((1.8, 2.0, 2.2), push_tokens, strict=True)
    ):
        events.append(
            _event(TradeSide.BUY, 8.0, tokens, timestamp, f"push-{index}")
        )
    return events


class TestCsvRoundtrip:
    """Запись и чтение CSV событий."""

    def test_write_then_load(self, tmp_path) -> None:
        """Раунд-трип сохраняет все ключевые поля и порядок."""
        events = build_pump_events()
        path = tmp_path / "events.csv"
        written = write_events_csv(path, events)
        assert written == len(events)
        loaded = load_events_csv(path)
        assert len(loaded) == len(events)
        for original, restored in zip(events, loaded, strict=True):
            assert restored.signature == original.signature
            assert restored.side == original.side
            assert restored.sol_amount == pytest.approx(original.sol_amount, abs=1e-6)
            assert restored.timestamp == pytest.approx(original.timestamp, abs=1e-3)

    def test_scenario_requires_reserves(self, tmp_path) -> None:
        """Без резервов сценарий не собирается (ReplayError)."""
        path = tmp_path / "events.csv"
        write_events_csv(path, build_pump_events())
        with pytest.raises(ReplayError):
            scenario_from_csv(path)


class TestBacktestRun:
    """Прогон сценария через живые фильтры и стратегию."""

    async def test_pump_scenario_profits(self, tmp_path) -> None:
        """Импульсный разгон входит и закрывается по тейк-профиту."""
        path = tmp_path / "events.csv"
        write_events_csv(path, build_pump_events())
        scenario = scenario_from_csv(
            path,
            # 120 SOL виртуальных = 90 реальных ≥ MIN_REAL_POOL_SOL=80; цена
            # та же, что при 90/600M (0.15 SOL на 1M токенов).
            initial_sol_reserve=120.0,
            initial_token_reserve=800_000_000.0,
            security=TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0),
        )
        backtester = Backtester(
            trade_amount_sol=0.5,
            max_concurrent_positions=1,
            entry_window_seconds=5.0,
            exit_window_seconds=2.0,
        )
        report = await backtester.run(scenario)

        assert report.events == len(scenario.events)
        assert report.trades >= 1
        assert any(
            "take_profit" in reason for reason in report.exit_reasons
        ), f"ожидался тейк-профит, получены: {report.exit_reasons}"
        assert report.pnl_sol > 0
        assert report.duration_seconds == pytest.approx(1.2, abs=0.1)
        assert report.win_rate_pct == 100.0

    async def test_quiet_scenario_no_entry(self, tmp_path) -> None:
        """Вялый поток без импульса не открывает позиций."""
        path = tmp_path / "quiet.csv"
        events = [
            _event(TradeSide.BUY, 0.1, 2_000_000, 1.0, "q1"),
            _event(TradeSide.SELL, 0.1, 2_000_000, 3.0, "q2"),
        ]
        write_events_csv(path, events)
        scenario = scenario_from_csv(
            path,
            # 120 SOL виртуальных = 90 реальных ≥ MIN_REAL_POOL_SOL=80; цена
            # та же, что при 90/600M (0.15 SOL на 1M токенов).
            initial_sol_reserve=120.0,
            initial_token_reserve=800_000_000.0,
            security=TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0),
        )
        backtester = Backtester(trade_amount_sol=0.5)
        report = await backtester.run(scenario)
        assert report.trades == 0
        assert report.invested_sol == 0.0
        assert report.pnl_sol == 0.0

    async def test_report_format_contains_metrics(self, tmp_path) -> None:
        """Текстовый отчёт содержит ключевые метрики."""
        path = tmp_path / "events.csv"
        write_events_csv(path, build_pump_events())
        scenario = scenario_from_csv(
            path,
            # 120 SOL виртуальных = 90 реальных ≥ MIN_REAL_POOL_SOL=80; цена
            # та же, что при 90/600M (0.15 SOL на 1M токенов).
            initial_sol_reserve=120.0,
            initial_token_reserve=800_000_000.0,
            security=TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0),
        )
        report = await Backtester(trade_amount_sol=0.5).run(scenario)
        text = report.format()
        assert "PnL" in text
        assert str(report.trades) in text
