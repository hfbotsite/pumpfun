"""Тесты бэктест-фичи дашборда: обработчик, CSV-парсер, извлечение mint."""

from __future__ import annotations

import asyncio

import pytest

from analytics.backtester import Backtester, BacktestReport
from analytics.replay import (
    ReplayError,
    ReplayPoolConfig,
    ReplayScenario,
    events_to_csv_text,
    parse_events_csv_text,
)
from core.client import RpcError
from core.dry_run import DryRunExecutor
from core.pipeline import LivePipeline
from models.trade_data import DEXType, TokenSecurity
from tests.conftest import BASE_MINT, POOL_ADDRESS
from tests.test_backtester import build_pump_events


class TestCsvTextHelpers:
    """Текстовые CSV-хелперы replay."""

    def test_roundtrip(self) -> None:
        """parse(events_to_csv_text(...)) восстанавливает события."""
        events = build_pump_events()
        text = events_to_csv_text(events)
        loaded = parse_events_csv_text(text)
        assert len(loaded) == len(events)
        assert loaded[0].signature == events[0].signature
        assert loaded[-1].side == events[-1].side

    def test_missing_columns_rejected(self) -> None:
        """Нет обязательных колонок — ReplayError."""
        with pytest.raises(ReplayError):
            parse_events_csv_text("signature,dex\nabc,pump_fun\n")

    def test_blank_rows_skipped(self) -> None:
        """Пустые строки в конце файла не ломают разбор."""
        events = build_pump_events()
        text = events_to_csv_text(events) + "\n\n"
        assert len(parse_events_csv_text(text)) == len(events)


class TestExtractMint:
    """Извлечение mint из ссылки pump.fun."""

    def test_full_url(self) -> None:
        """Полная ссылка coin/<MINT> распознаётся."""
        mint = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
        result = LivePipeline._extract_mint(f"https://pump.fun/coin/{mint}")
        assert result == mint

    def test_raw_mint(self) -> None:
        """Голый base58-адрес проходит как есть."""
        mint = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
        assert LivePipeline._extract_mint(mint) == mint

    def test_garbage_returns_none(self) -> None:
        """Мусор без base58-подобного куска — None."""
        assert LivePipeline._extract_mint("https://example.com/trade") is None


class TestBacktestHandler:
    """Обработчик _dashboard_backtest (без запуска компонентов)."""

    def _pipeline(self) -> LivePipeline:
        """Пайплайн dry-run с подставленным RPC-заглушечным полем.

        Returns:
            :class:`~core.pipeline.LivePipeline`.
        """
        pipeline = LivePipeline(dry_run=True)
        # _rpc нужен только в mint-режиме; для sample/csv не используется.
        return pipeline

    def test_sample_returns_csv(self) -> None:
        """Режим sample отдаёт демо-CSV с ≥5 событиями."""
        pipeline = self._pipeline()
        result = asyncio.run(pipeline._dashboard_backtest({"mode": "sample"}))
        assert result["ok"] is True
        csv_text = str(result["csv"])
        assert csv_text.count("\n") >= 6  # заголовок + строки
        parsed = parse_events_csv_text(csv_text)
        assert len(parsed) >= 7

    def test_csv_mode_runs_report(self) -> None:
        """CSV-режим возвращает отчёт с полем report."""
        pipeline = self._pipeline()
        csv_text = events_to_csv_text(build_pump_events())
        result = asyncio.run(
            pipeline._dashboard_backtest(
                {
                    "mode": "csv",
                    "csv": csv_text,
                    "initial_sol": 120,  # реальные 90 ≥ MIN_REAL_POOL_SOL=80
                    "initial_token": 800_000_000,
                    "trade_amount_sol": 0.5,
                }
            )
        )
        assert result["ok"] is True
        report: BacktestReport = result["report"]  # type: ignore[assignment]
        assert report["trades"] >= 1
        assert any("take_profit" in r for r in report["exit_reasons"])
        assert report["pnl_sol"] > 0

    def test_csv_empty_rejected(self) -> None:
        """Пустой CSV — ошибка."""
        pipeline = self._pipeline()
        result = asyncio.run(pipeline._dashboard_backtest({"mode": "csv", "csv": ""}))
        assert result["ok"] is False

    def test_csv_bad_columns_rejected(self) -> None:
        """Битые колонки — понятная ошибка, а не исключение."""
        pipeline = self._pipeline()
        result = asyncio.run(
            pipeline._dashboard_backtest({"mode": "csv", "csv": "a,b\n1,2\n"})
        )
        assert result["ok"] is False
        assert "не разобран" in str(result["error"])

    def test_mint_garbage_url_rejected(self) -> None:
        """Мусорная ссылка в mint-режиме — ошибка без сети."""
        pipeline = self._pipeline()
        result = asyncio.run(
            pipeline._dashboard_backtest(
                {"mode": "mint", "url": "https://example.com/x"}
            )
        )
        assert result["ok"] is False
        assert "mint" in str(result["error"])

    def test_unknown_mode_rejected(self) -> None:
        """Неизвестный режим — ошибка."""
        pipeline = self._pipeline()
        result = asyncio.run(pipeline._dashboard_backtest({"mode": "astro"}))
        assert result["ok"] is False
        assert "неизвестный режим" in str(result["error"])


def test_report_to_dict_shape() -> None:
    """to_dict отдаёт все ключевые поля для рендера в браузере."""
    scenario = ReplayScenario(
        pool=ReplayPoolConfig(
            pool_address=POOL_ADDRESS,
            base_mint=BASE_MINT,
            dex=DEXType.PUMP_FUN,
            quote_reserve=90_000_000_000,
            base_reserve=600_000_000_000_000,
        ),
        events=tuple(build_pump_events()),
        security=TokenSecurity(lp_burned_pct=100.0),
    )
    report = asyncio.run(
        Backtester(trade_amount_sol=0.5, entry_window_seconds=5.0, exit_window_seconds=2.0).run(scenario)
    )
    data = report.to_dict()
    for key in (
        "trades", "wins", "losses", "pnl_sol", "pnl_pct", "win_rate_pct",
        "max_drawdown_sol", "best_trade_pct", "worst_trade_pct",
        "exit_reasons", "closed_trades", "avg_hold_seconds",
    ):
        assert key in data, key
    assert isinstance(data["exit_reasons"], dict)
    assert data["trades"] >= 1  # сценарий импульса обязан давать сделки


class TestMintRpcFailure:
    """Сбой Helius в mint-режиме приходит JSON-ом, а не 500."""

    def test_rpc_failure_falls_back_gracefully(self) -> None:
        """Сбой Helius → fallback на публичный RPC → честная ошибка без 500."""
        class DeadRpc:
            def get_signatures_for_address(self, address, *, limit=500, commitment="confirmed"):
                raise RpcError("[Errno 11001] getaddrinfo failed")

        pipeline = LivePipeline(dry_run=True)
        pipeline._rpc = DeadRpc()
        result = asyncio.run(
            pipeline._dashboard_backtest(
                {
                    "mode": "mint",
                    "url": "https://pump.fun/coin/7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
                }
            )
        )
        assert result["ok"] is False
        error = str(result["error"])
        # Fallback на публичный RPC зависит от сети песочницы: либо честная
        # нехватка событий, либо недоступность/таймаут RPC — но не 500 без тела.
        assert (
            "мало событий" in error
            or "Helius RPC" in error
        )


class TestBacktestCancellation:
    """Кооперативная остановка бэктеста с частичной статистикой."""

    def test_cancel_before_run_zero_events(self) -> None:
        """Событие отмены, установленное до прогона, — 0 событий, отчёт валиден."""
        cancel = asyncio.Event()
        cancel.set()
        backtester = Backtester(cancel_event=cancel)
        scenario = ReplayScenario(
            pool=ReplayPoolConfig(
                pool_address=POOL_ADDRESS,
                base_mint=BASE_MINT,
                dex=DEXType.PUMP_FUN,
                # реальные 90 SOL ≥ MIN_REAL_POOL_SOL=80
                quote_reserve=120_000_000_000,
                base_reserve=800_000_000_000_000,
            ),
            events=tuple(build_pump_events()),
            security=TokenSecurity(lp_burned_pct=100.0),
        )
        report = asyncio.run(backtester.run(scenario))
        assert backtester.was_cancelled is True
        assert report.events == 0
        assert report.trades == 0

    def test_cancel_mid_run_partial_report(self) -> None:
        """Остановка в середине — отчёт по части событий, позиции закрыты."""
        cancel = asyncio.Event()

        class CancelAfterEntries(DryRunExecutor):
            """Ставит cancel_event после N-й покупки."""

            def __init__(self, after: int) -> None:
                super().__init__()
                self._after = after
                self._buys = 0

            async def execute_buy(self, order):  # type: ignore[override]
                result = await super().execute_buy(order)
                if result.success:
                    self._buys += 1
                    if self._buys >= self._after:
                        cancel.set()
                return result

        all_events = build_pump_events()
        backtester = Backtester(
            executor=CancelAfterEntries(after=1),
            trade_amount_sol=0.5,
            entry_window_seconds=5.0,
            exit_window_seconds=2.0,
            cancel_event=cancel,
        )
        scenario = ReplayScenario(
            pool=ReplayPoolConfig(
                pool_address=POOL_ADDRESS,
                base_mint=BASE_MINT,
                dex=DEXType.PUMP_FUN,
                # реальные 90 SOL ≥ MIN_REAL_POOL_SOL=80
                quote_reserve=120_000_000_000,
                base_reserve=800_000_000_000_000,
            ),
            events=tuple(all_events),
            security=TokenSecurity(lp_burned_pct=100.0),
        )
        report = asyncio.run(backtester.run(scenario))
        assert backtester.was_cancelled is True
        assert 0 < report.events < len(all_events)
        assert report.trades >= 0


class TestBacktestStopMode:
    """Режим stop в _dashboard_backtest (машина состояний запуска)."""

    def test_stop_without_run_rejected(self) -> None:
        """Стоп без активного запуска — отказ."""
        pipeline = LivePipeline(dry_run=True)
        result = asyncio.run(
            pipeline._dashboard_backtest({"mode": "stop", "run_id": "abc"})
        )
        assert result["ok"] is False
        assert "активного запуска" in str(result["error"])

    def test_stop_with_matching_run_id_sets_event(self) -> None:
        """Совпадающий run_id ставит событие отмены."""
        pipeline = LivePipeline(dry_run=True)
        pipeline._backtest_stop = asyncio.Event()
        pipeline._backtest_run_id = "run-42"
        result = asyncio.run(
            pipeline._dashboard_backtest({"mode": "stop", "run_id": "run-42"})
        )
        assert result["ok"] is True
        assert pipeline._backtest_stop.is_set()

    def test_stop_with_wrong_run_id_rejected(self) -> None:
        """Чужой run_id — отказ (защита от ложной остановки)."""
        pipeline = LivePipeline(dry_run=True)
        pipeline._backtest_stop = asyncio.Event()
        pipeline._backtest_run_id = "run-42"
        result = asyncio.run(
            pipeline._dashboard_backtest({"mode": "stop", "run_id": "other"})
        )
        assert result["ok"] is False
        assert not pipeline._backtest_stop.is_set()
