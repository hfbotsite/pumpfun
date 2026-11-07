"""Тесты потоковых индикаторов: скользящие окна и агрегаты."""

from __future__ import annotations

from analytics.indicators import RollingTradeWindow, StreamingIndicators
from models.trade_data import TradeSide
from tests.conftest import make_event


class TestRollingWindow:
    """Окно одного пула: обрезка, агрегаты, цены."""

    def test_prune_drops_old_events(self) -> None:
        """События старше глубины окна удаляются при добавлении."""
        window = RollingTradeWindow(max_window_seconds=5.0)
        window.add(make_event(timestamp=100.0, signature="a"))
        window.add(make_event(timestamp=110.0, signature="b"))
        assert len(window) == 1
        assert window.latest_event().signature == "b"

    def test_snapshot_counts_and_volumes(self) -> None:
        """Агрегаты: счётчики, объёмы, крупнейшая продажа."""
        window = RollingTradeWindow(max_window_seconds=10.0)
        window.add(
            make_event(
                TradeSide.BUY,
                sol_amount=1.0,
                token_amount=1e8,
                timestamp=100.0,
                signature="b1",
            )
        )
        window.add(
            make_event(
                TradeSide.BUY,
                sol_amount=1.5,
                token_amount=1.5e8,
                timestamp=101.0,
                signature="b2",
            )
        )
        window.add(
            make_event(
                TradeSide.SELL,
                sol_amount=2.0,
                token_amount=2e8,
                timestamp=102.0,
                signature="s1",
            )
        )
        metrics = window.snapshot(5.0, now=102.0)
        assert metrics.buy_count == 2
        assert metrics.sell_count == 1
        assert metrics.buy_volume_sol == 2.5
        assert metrics.sell_volume_sol == 2.0
        assert metrics.largest_sell_sol == 2.0
        assert metrics.total_count == 3
        assert abs(metrics.buy_sell_ratio - 2 / 3) < 1e-9

    def test_snapshot_window_filters_old_events(self) -> None:
        """Подынтервал 2 c не видит события 3-секундной давности."""
        window = RollingTradeWindow(max_window_seconds=10.0)
        window.add(make_event(sol_amount=1.0, timestamp=100.0, signature="old"))
        window.add(make_event(sol_amount=1.0, timestamp=103.0, signature="new"))
        metrics = window.snapshot(2.0, now=103.0)
        assert metrics.total_count == 1

    def test_last_price_from_buy_and_sell(self) -> None:
        """last_price берётся из последнего события с ненулевым объёмом."""
        window = RollingTradeWindow(max_window_seconds=10.0)
        window.add(make_event(sol_amount=1.0, token_amount=1e8, timestamp=100.0))
        assert window.last_price == 1e-8
        window.add(
            make_event(
                TradeSide.SELL,
                sol_amount=1.5,
                token_amount=1e8,
                timestamp=101.0,
                signature="s",
            )
        )
        assert window.last_price == 1.5e-8

    def test_zero_token_event_does_not_change_price(self) -> None:
        """Событие без количества токенов не портит last_price."""
        window = RollingTradeWindow(max_window_seconds=10.0)
        window.add(make_event(sol_amount=1.0, token_amount=1e8, timestamp=100.0))
        window.add(make_event(sol_amount=9.0, token_amount=0, timestamp=101.0))
        assert window.last_price == 1e-8


class TestStreamingIndicators:
    """Реестр окон: изоляция пулов, цены, очистка."""

    def test_pools_are_isolated(self) -> None:
        """Метрики одного пула не просачиваются в другой."""
        indicators = StreamingIndicators(10.0)
        indicators.record(
            make_event(pool_address="poolA", sol_amount=2.0, timestamp=100.0)
        )
        metrics_a = indicators.window_metrics("poolA", 5.0, now=100.0)
        metrics_b = indicators.window_metrics("poolB", 5.0, now=100.0)
        assert metrics_a.buy_volume_sol == 2.0
        assert metrics_b.total_count == 0
        assert metrics_b.last_price == 0.0

    def test_latest_price_tracked(self) -> None:
        """latest_price отдаёт последнюю цену и 0 для неизвестного пула."""
        indicators = StreamingIndicators(10.0)
        indicators.record(
            make_event(sol_amount=1.0, token_amount=2e8, timestamp=100.0)
        )
        assert indicators.latest_price("Pool1111111111111111111111111111111111111111") == 5e-9
        assert indicators.latest_price("unknown") == 0.0

    def test_prune_pool_forgets(self) -> None:
        """prune_pool удаляет и окно, и цену."""
        indicators = StreamingIndicators(10.0)
        indicators.record(make_event(sol_amount=1.0, timestamp=100.0))
        pool = "Pool1111111111111111111111111111111111111111"
        indicators.prune_pool(pool)
        assert indicators.window(pool) is None
        assert indicators.latest_price(pool) == 0.0

    def test_extend_batch_backtest(self) -> None:
        """Пакетное добавление сохраняет хронологию и цену."""
        window = RollingTradeWindow(max_window_seconds=10.0)
        events = [
            make_event(sol_amount=1.0, token_amount=1e8, timestamp=100.0, signature="e1"),
            make_event(sol_amount=2.0, token_amount=1e8, timestamp=101.0, signature="e2"),
        ]
        window.extend(events)
        assert len(window) == 2
        assert window.last_price == 2e-8
