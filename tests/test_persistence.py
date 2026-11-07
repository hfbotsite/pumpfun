"""Тесты SQLite-хранилища позиций: CRUD, раунд-трип, сводка."""

from __future__ import annotations

from core.persistence import PortfolioStore
from models.trade_data import DEXType
from tests.conftest import make_position


def make_store(tmp_path) -> PortfolioStore:
    """Создаёт хранилище во временной папке.

    Args:
        tmp_path: pytest-фикстура временного каталога.

    Returns:
        :class:`~core.persistence.PortfolioStore`.
    """
    return PortfolioStore(tmp_path / "bot.db")


class TestPositions:
    """CRUD открытых позиций."""

    def test_upsert_and_load_roundtrip(self, tmp_path) -> None:
        """Сохранённая позиция читается со всеми полями."""
        store = make_store(tmp_path)
        position = make_position(tp1_taken=True, realized_sol=0.25, peak_price=9e-9)
        store.upsert_position(position)
        loaded = store.load_open_positions()
        assert len(loaded) == 1
        same = loaded[0]
        assert same.base_mint == position.base_mint
        assert same.pool_address == position.pool_address
        assert same.dex == DEXType.PUMP_FUN
        assert same.entry_price == position.entry_price
        assert same.token_amount == position.token_amount
        assert same.tp1_taken is True
        assert same.realized_sol == 0.25
        assert same.peak_price == 9e-9
        assert same.signature == position.signature
        store.close()

    def test_upsert_updates_existing(self, tmp_path) -> None:
        """Повторный upsert обновляет строку, а не дублирует."""
        store = make_store(tmp_path)
        position = make_position()
        store.upsert_position(position)
        position.token_amount = 5e7
        store.upsert_position(position)
        assert store.count_open_positions() == 1
        assert store.load_open_positions()[0].token_amount == 5e7
        store.close()

    def test_remove_position(self, tmp_path) -> None:
        """Удаление убирает позицию из выборки."""
        store = make_store(tmp_path)
        store.upsert_position(make_position())
        assert store.count_open_positions() == 1
        store.remove_position(make_position().base_mint)
        assert store.count_open_positions() == 0
        assert store.load_open_positions() == []
        store.close()

    def test_corrupted_row_skipped(self, tmp_path) -> None:
        """Повреждённая строка пропускается, остальные читаются."""
        store = make_store(tmp_path)
        store.upsert_position(make_position(base_mint="MintA"))
        store.upsert_position(make_position(base_mint="MintB"))
        # Портим dex до недопустимого значения напрямую в SQLite.
        store._conn.execute("UPDATE positions SET dex = 'no_such_dex'")
        store._conn.commit()
        loaded = store.load_open_positions()
        assert loaded == []
        store.close()


class TestClosedTrades:
    """Журнал закрытых сделок и сводка."""

    def test_record_and_summary(self, tmp_path) -> None:
        """Сводка агрегирует вложено/возвращено/PnL по всем сделкам."""
        store = make_store(tmp_path)
        common = {
            "pool_address": "Pool1111111111111111111111111111111111111111",
            "dex": DEXType.PUMP_FUN,
            "entry_price": 5e-9,
            "exit_price": 6e-9,
            "opened_at": 1000.0,
            "closed_at": 1005.0,
            "peak_price": 7e-9,
        }
        store.record_closed_trade(
            base_mint="MintA", invested_sol=0.5, returned_sol=0.6,
            exit_reason="take_profit_2", **common,
        )
        store.record_closed_trade(
            base_mint="MintB", invested_sol=0.5, returned_sol=0.2,
            exit_reason="hard_stop_loss", **common,
        )
        summary = store.closed_trades_summary()
        assert summary["closed_trades"] == 2
        assert summary["invested_sol"] == 1.0
        assert summary["returned_sol"] == 0.8
        assert summary["pnl_sol"] == -0.2
        store.close()

    def test_empty_summary(self, tmp_path) -> None:
        """Пустая БД — нулевая сводка без исключений."""
        store = make_store(tmp_path)
        summary = store.closed_trades_summary()
        assert summary["closed_trades"] == 0
        assert summary["pnl_sol"] == 0.0
        store.close()


class TestLifecycle:
    """Жизненный цикл хранилища."""

    def test_close_is_idempotent(self, tmp_path) -> None:
        """Повторное close не падает."""
        store = make_store(tmp_path)
        store.close()
        store.close()

    def test_creates_parent_directory(self, tmp_path) -> None:
        """Хранилище само создаёт недостающие каталоги."""
        nested = tmp_path / "data" / "sub" / "bot.db"
        store = PortfolioStore(nested)
        assert nested.exists()
        store.close()

    def test_context_manager(self, tmp_path) -> None:
        """Работа через ``with`` закрывает соединение."""
        with PortfolioStore(tmp_path / "cm.db") as store:
            store.upsert_position(make_position())
            assert store.count_open_positions() == 1
