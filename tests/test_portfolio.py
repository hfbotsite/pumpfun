"""Тесты портфеля: лимиты, частичные продажи, статистика, write-through БД."""

from __future__ import annotations

import pytest

from core.persistence import PortfolioStore
from core.portfolio import PortfolioError, PortfolioManager
from models.trade_data import DEXType
from tests.conftest import BASE_MINT, POOL_ADDRESS, make_position


def open_default(manager: PortfolioManager, **overrides):
    """Открывает позицию фабричными параметрами.

    Args:
        manager: Менеджер портфеля.
        overrides: Переопределения аргументов open_position.

    Returns:
        Результат open_position.
    """
    args = {
        "base_mint": BASE_MINT,
        "pool_address": POOL_ADDRESS,
        "dex": DEXType.PUMP_FUN,
        "entry_price": 5e-9,
        "entry_sol": 0.5,
        "token_amount": 1e8,
        "signature": "sig-1",
        "opened_at": 1000.0,
    }
    args.update(overrides)
    return manager.open_position(**args)


class TestOpenLimits:
    """Лимиты позиций и идемпотентность открытия."""

    def test_open_and_count(self) -> None:
        """Успешное открытие учитывается в open_count и reserved_sol."""
        manager = PortfolioManager()
        assert open_default(manager) is not None
        assert manager.open_count == 1
        assert manager.stats is not None

    def test_duplicate_mint_ignored(self) -> None:
        """Повторное открытие по тому же mint возвращает None."""
        manager = PortfolioManager()
        open_default(manager)
        assert open_default(manager) is None
        assert manager.open_count == 1

    def test_multiple_mints_ok(self) -> None:
        """Разные mints открываются одновременно (лимит мятов убран)."""
        manager = PortfolioManager()
        assert open_default(manager) is not None
        assert open_default(manager, base_mint="Mint2", signature="sig-2") is not None
        assert manager.open_count == 2

    def test_capital_blocks(self) -> None:
        """Исчерпание капитала блокирует вход."""
        manager = PortfolioManager()
        assert manager.can_open(capital_sol=1.0)
        open_default(manager)  # резерв 0.5 SOL
        # Остаток 0.5-0.5 = 0 → вход закрыт; с запасом — открыт.
        assert not manager.can_open(capital_sol=0.5)
        assert manager.can_open(capital_sol=1.0)

    def test_invalid_args_raise(self) -> None:
        """Некорректные аргументы открытия поднимают PortfolioError."""
        manager = PortfolioManager()
        try:
            open_default(manager, entry_price=-1.0)
        except PortfolioError:
            pass
        else:
            raise AssertionError("ожидался PortfolioError")


class TestSells:
    """Частичные продажи, TP1-метка, force close."""

    def test_partial_sell_keeps_position(self) -> None:
        """Частичная продажа уменьшает остаток, но позиция открыта."""
        manager = PortfolioManager()
        open_default(manager)
        closed = manager.apply_sell(
            base_mint=BASE_MINT,
            sold_tokens=5e7,
            received_sol=0.4,
            exit_price=8e-9,
            exit_reason="take_profit_1",
        )
        assert closed is None
        position = manager.get(BASE_MINT)
        assert position.token_amount == 5e7
        assert position.realized_sol == 0.4

    def test_full_sell_closes_and_stats(self) -> None:
        """Полная продажа закрывает сделку и обновляет статистику."""
        manager = PortfolioManager()
        open_default(manager)
        closed = manager.apply_sell(
            base_mint=BASE_MINT,
            sold_tokens=1e8,
            received_sol=0.6,
            exit_price=6e-9,
            exit_reason="take_profit_2",
            sold_at=1010.0,
        )
        assert closed is not None
        assert closed.pnl_sol == pytest.approx(0.1, abs=1e-9)
        assert manager.open_count == 0
        assert manager.stats.trades == 1
        assert manager.stats.wins == 1
        assert manager.stats.win_rate_pct == 100.0

    def test_oversell_clamped_to_remaining(self) -> None:
        """Продажа больше остатка обрезается и закрывает позицию."""
        manager = PortfolioManager()
        open_default(manager)
        closed = manager.apply_sell(
            base_mint=BASE_MINT,
            sold_tokens=999e8,
            received_sol=0.5,
            exit_price=5e-9,
        )
        assert closed is not None
        assert closed.returned_sol == 0.5

    def test_tp1_marked(self) -> None:
        """mark_tp1_taken выставляет флаг позиции."""
        manager = PortfolioManager()
        open_default(manager)
        manager.mark_tp1_taken(BASE_MINT)
        assert manager.get(BASE_MINT).tp1_taken is True

    def test_mark_price_updates_peak(self) -> None:
        """mark_price растит пик и не опускает его."""
        manager = PortfolioManager()
        open_default(manager)
        manager.mark_price(BASE_MINT, 8e-9)
        assert manager.get(BASE_MINT).peak_price == 8e-9
        manager.mark_price(BASE_MINT, 6e-9)
        assert manager.get(BASE_MINT).peak_price == 8e-9

    def test_force_close_estimates_return(self) -> None:
        """force_close закрывает позицию с оценкой возврата по цене."""
        manager = PortfolioManager()
        open_default(manager)
        closed = manager.force_close(
            BASE_MINT, exit_price=4e-9, exit_reason="shutdown", closed_at=1050.0
        )
        assert closed is not None
        assert closed.exit_reason == "shutdown"
        assert closed.returned_sol == 0.4  # 1e8 * 4e-9
        assert manager.stats.losses == 1


class TestWriteThrough:
    """Write-through в SQLite и восстановление после «рестарта»."""

    def test_restart_restores_positions(self, tmp_path) -> None:
        """Открытая позиция переживает пересоздание менеджера."""
        db_path = tmp_path / "portfolio.db"
        store = PortfolioStore(db_path)
        manager = PortfolioManager(store=store)
        open_default(manager)
        store.close()

        store2 = PortfolioStore(db_path)
        manager2 = PortfolioManager(store=store2)
        assert manager2.load_open_positions() == 1
        restored = manager2.get(BASE_MINT)
        assert restored is not None
        assert restored.entry_sol == 0.5
        assert restored.token_amount == 1e8
        store2.close()

    def test_partial_sell_visible_after_reload(self, tmp_path) -> None:
        """Частичная продажа обновляет позицию в БД."""
        db_path = tmp_path / "portfolio.db"
        store = PortfolioStore(db_path)
        manager = PortfolioManager(store=store)
        open_default(manager)
        manager.apply_sell(
            base_mint=BASE_MINT,
            sold_tokens=4e7,
            received_sol=0.3,
            exit_price=7.5e-9,
        )
        assert store.count_open_positions() == 1
        loaded = store.load_open_positions()[0]
        assert loaded.token_amount == 6e7
        assert loaded.realized_sol == 0.3
        store.close()

    def test_close_removes_from_db_and_records_trade(self, tmp_path) -> None:
        """Полное закрытие удаляет позицию из БД и пишет в журнал сделок."""
        db_path = tmp_path / "portfolio.db"
        store = PortfolioStore(db_path)
        manager = PortfolioManager(store=store)
        open_default(manager)
        closed = manager.apply_sell(
            base_mint=BASE_MINT,
            sold_tokens=1e8,
            received_sol=0.6,
            exit_price=6e-9,
            exit_reason="take_profit_2",
        )
        assert closed is not None
        assert store.count_open_positions() == 0
        summary = store.closed_trades_summary()
        assert summary["closed_trades"] == 1
        assert summary["pnl_sol"] == 0.1
        store.close()

    def test_recovered_positions_manageable(self, tmp_path) -> None:
        """Восстановленные позиции управляются: можно докупать и закрывать."""
        db_path = tmp_path / "portfolio.db"
        store = PortfolioStore(db_path)
        manager = PortfolioManager(store=store)
        open_default(manager)
        store.close()

        store2 = PortfolioStore(db_path)
        manager2 = PortfolioManager(store=store2)
        manager2.load_open_positions()
        assert manager2.can_open()
        assert open_default(manager2, base_mint="MintX", signature="sx") is not None
        assert manager2.open_count == 2
        store2.close()


def test_position_factory_matches_manager() -> None:
    """Фабрика make_position согласована с ожиданиями менеджера."""
    position = make_position()
    assert position.remaining_fraction == 1.0
