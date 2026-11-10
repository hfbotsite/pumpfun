"""SQLite-хранилище позиций: надёжный слой поверх memory-портфеля.

Модуль реализует write-through-персистентность для
:class:`~core.portfolio.PortfolioManager`: каждое открытие, частичная
продажа, обновление пика и закрытие позиции немедленно отражаются в
локальной SQLite-БД. Благодаря этому после перезапуска бот видит открытые
позиции и продолжает управлять их выходами (TP/trailing/SL/max-hold).

Хранилище не принимает решений и не знает о сети — это чистый CRUD над
двумя таблицами:

* ``positions`` — открытые позиции (первичный ключ ``base_mint``);
* ``closed_trades`` — журнал полностью закрытых сделок.

Режим журнала WAL + ``synchronous=NORMAL`` даёт быстрые записи при
сохранной durability на уровне процесса. Все вызовы синхронные и
предназначены для использования из одного event-loop-потока.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Self

from models.trade_data import DEXType, Position, ScreeningVerdict
from utils.logger import get_logger

logger = get_logger()

#: Схема БД (идемпотентная).
_SCHEMA: str = """
CREATE TABLE IF NOT EXISTS positions (
    base_mint            TEXT PRIMARY KEY,
    pool_address         TEXT    NOT NULL,
    dex                  TEXT    NOT NULL,
    entry_price          REAL    NOT NULL,
    entry_sol            REAL    NOT NULL,
    token_amount         REAL    NOT NULL,
    initial_token_amount REAL    NOT NULL,
    opened_at            REAL    NOT NULL,
    peak_price           REAL    NOT NULL,
    tp1_taken            INTEGER NOT NULL DEFAULT 0,
    realized_sol         REAL    NOT NULL DEFAULT 0.0,
    signature            TEXT,
    stop_loss_pct        REAL    NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS closed_trades (
    base_mint    TEXT NOT NULL,
    pool_address TEXT NOT NULL,
    dex          TEXT NOT NULL,
    entry_price  REAL NOT NULL,
    exit_price   REAL NOT NULL,
    invested_sol REAL NOT NULL,
    returned_sol REAL NOT NULL,
    opened_at    REAL NOT NULL,
    closed_at    REAL NOT NULL,
    peak_price   REAL NOT NULL,
    exit_reason  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_closed_trades_base_mint
    ON closed_trades(base_mint);

CREATE TABLE IF NOT EXISTS token_screening (
    base_mint        TEXT PRIMARY KEY,
    approved         INTEGER NOT NULL,
    reason           TEXT    NOT NULL,
    mint_authority   TEXT,
    freeze_authority TEXT,
    lp_burned_pct    REAL    NOT NULL,
    checked_at       REAL    NOT NULL
);
"""


class PortfolioStore:
    """CRUD-обёртка над SQLite для позиций и закрытых сделок.

    Args:
        path: Путь к файлу БД (каталог создаётся при необходимости).
    """

    def __init__(self, path: str | Path) -> None:
        """Открывает соединение и создаёт схему при первом запуске.

        Args:
            path: Путь к файлу БД.
        """
        path = Path(path)
        if path.parent and str(path.parent) not in ("", "."):
            path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._migrate_schema()
        self._conn.commit()
        logger.info("Хранилище позиций открыто: {}", path)

    def _migrate_schema(self) -> None:
        """Миграции существующей БД: добавляет недостающие колонки."""
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(positions)")}
        if "stop_loss_pct" not in columns:
            self._conn.execute(
                "ALTER TABLE positions ADD COLUMN stop_loss_pct REAL NOT NULL DEFAULT 0.0"
            )
            logger.info("Миграция БД: добавлена колонка positions.stop_loss_pct")

    # ------------------------------------------------------------------ Позиции
    def upsert_position(self, position: Position) -> None:
        """Сохраняет (вставляет или обновляет) открытую позицию.

        Args:
            position: Текущее состояние позиции.
        """
        self._conn.execute(
            """
            INSERT INTO positions (
                base_mint, pool_address, dex, entry_price, entry_sol,
                token_amount, initial_token_amount, opened_at, peak_price,
                tp1_taken, realized_sol, signature, stop_loss_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(base_mint) DO UPDATE SET
                pool_address         = excluded.pool_address,
                dex                  = excluded.dex,
                entry_price          = excluded.entry_price,
                entry_sol            = excluded.entry_sol,
                token_amount         = excluded.token_amount,
                initial_token_amount = excluded.initial_token_amount,
                opened_at            = excluded.opened_at,
                peak_price           = excluded.peak_price,
                tp1_taken            = excluded.tp1_taken,
                realized_sol         = excluded.realized_sol,
                signature            = excluded.signature,
                stop_loss_pct        = excluded.stop_loss_pct
            """,
            (
                position.base_mint,
                position.pool_address,
                position.dex.value,
                position.entry_price,
                position.entry_sol,
                position.token_amount,
                position.initial_token_amount,
                position.opened_at,
                position.peak_price,
                int(position.tp1_taken),
                position.realized_sol,
                position.signature,
                position.stop_loss_pct,
            ),
        )
        self._conn.commit()

    def remove_position(self, base_mint: str) -> None:
        """Удаляет позицию из таблицы открытых (после полного закрытия).

        Args:
            base_mint: Mint мемкоина.
        """
        self._conn.execute(
            "DELETE FROM positions WHERE base_mint = ?", (base_mint,)
        )
        self._conn.commit()

    def load_open_positions(self) -> list[Position]:
        """Читает все открытые позиции из БД.

        Returns:
            Список :class:`~models.trade_data.Position` (порядок не важен).
        """
        cursor = self._conn.execute(
            """
            SELECT base_mint, pool_address, dex, entry_price, entry_sol,
                   token_amount, initial_token_amount, opened_at, peak_price,
                   tp1_taken, realized_sol, signature, stop_loss_pct
            FROM positions
            """
        )
        positions: list[Position] = []
        for row in cursor:
            try:
                positions.append(
                    Position(
                        base_mint=str(row[0]),
                        pool_address=str(row[1]),
                        dex=DEXType(str(row[2])),
                        entry_price=float(row[3]),
                        entry_sol=float(row[4]),
                        token_amount=float(row[5]),
                        initial_token_amount=float(row[6]),
                        opened_at=float(row[7]),
                        peak_price=float(row[8]),
                        tp1_taken=bool(row[9]),
                        realized_sol=float(row[10]),
                        signature=str(row[11]) if row[11] is not None else None,
                        stop_loss_pct=float(row[12]),
                    )
                )
            except Exception as exc:  # повреждённая строка не роняет старт
                logger.warning("Пропущена повреждённая позиция из БД: {}", exc)
        return positions

    def count_open_positions(self) -> int:
        """Возвращает число открытых позиций в БД.

        Returns:
            Количество строк в таблице ``positions``.
        """
        row = self._conn.execute("SELECT COUNT(*) FROM positions").fetchone()
        return int(row[0]) if row is not None else 0

    # ------------------------------------------------------------ Закрытые сделки
    def record_closed_trade(
        self,
        *,
        base_mint: str,
        pool_address: str,
        dex: DEXType,
        entry_price: float,
        exit_price: float,
        invested_sol: float,
        returned_sol: float,
        opened_at: float,
        closed_at: float,
        peak_price: float,
        exit_reason: str,
    ) -> None:
        """Добавляет запись о полностью закрытой сделке.

        Args:
            base_mint: Mint мемкоина.
            pool_address: Адрес пула.
            dex: DEX/платформа.
            entry_price: Цена входа, SOL за токен.
            exit_price: Средняя цена выхода, SOL за токен.
            invested_sol: Вложенный объём, SOL.
            returned_sol: Возвращённый объём, SOL.
            opened_at: Время открытия, unix ts.
            closed_at: Время закрытия, unix ts.
            peak_price: Максимальная зарегистрированная цена.
            exit_reason: Причина закрытия.
        """
        self._conn.execute(
            """
            INSERT INTO closed_trades (
                base_mint, pool_address, dex, entry_price, exit_price,
                invested_sol, returned_sol, opened_at, closed_at,
                peak_price, exit_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                base_mint,
                pool_address,
                dex.value,
                entry_price,
                exit_price,
                invested_sol,
                returned_sol,
                opened_at,
                closed_at,
                peak_price,
                exit_reason,
            ),
        )
        self._conn.commit()

    # ---------------------------------------------------------------- Отбор токенов
    def save_screening(self, verdict: ScreeningVerdict) -> None:
        """Сохраняет (вставляет или обновляет) вердикт отбора токена.

        Args:
            verdict: Результат проверки контракта.
        """
        self._conn.execute(
            """
            INSERT INTO token_screening (
                base_mint, approved, reason, mint_authority,
                freeze_authority, lp_burned_pct, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(base_mint) DO UPDATE SET
                approved         = excluded.approved,
                reason           = excluded.reason,
                mint_authority   = excluded.mint_authority,
                freeze_authority = excluded.freeze_authority,
                lp_burned_pct    = excluded.lp_burned_pct,
                checked_at       = excluded.checked_at
            """,
            (
                verdict.base_mint,
                int(verdict.approved),
                verdict.reason,
                verdict.mint_authority,
                verdict.freeze_authority,
                verdict.lp_burned_pct,
                verdict.checked_at,
            ),
        )
        self._conn.commit()

    def load_screenings(self) -> list[ScreeningVerdict]:
        """Читает все сохранённые вердикты отбора.

        Returns:
            Список :class:`~models.trade_data.ScreeningVerdict`.
        """
        cursor = self._conn.execute(
            """
            SELECT base_mint, approved, reason, mint_authority,
                   freeze_authority, lp_burned_pct, checked_at
            FROM token_screening
            """
        )
        verdicts: list[ScreeningVerdict] = []
        for row in cursor:
            try:
                verdicts.append(
                    ScreeningVerdict(
                        base_mint=str(row[0]),
                        approved=bool(row[1]),
                        reason=str(row[2]),
                        mint_authority=str(row[3]) if row[3] is not None else None,
                        freeze_authority=str(row[4]) if row[4] is not None else None,
                        lp_burned_pct=float(row[5]),
                        checked_at=float(row[6]),
                    )
                )
            except Exception as exc:  # повреждённая строка не роняет старт
                logger.warning("Пропущен повреждённый вердикт отбора из БД: {}", exc)
        return verdicts

    # ---------------------------------------------------------------- Жизненный цикл
    def closed_trades_summary(self) -> dict[str, float | int]:
        """Агрегирует журнал закрытых сделок (для команды ``--status``).

        Returns:
            Словарь: число сделок, вложено/возвращено SOL и PnL.
        """
        row = self._conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(invested_sol), 0.0),
                   COALESCE(SUM(returned_sol), 0.0)
            FROM closed_trades
            """
        ).fetchone()
        if row is None:
            return {"closed_trades": 0, "invested_sol": 0.0, "returned_sol": 0.0, "pnl_sol": 0.0}
        invested = float(row[1])
        returned = float(row[2])
        return {
            "closed_trades": int(row[0]),
            "invested_sol": round(invested, 6),
            "returned_sol": round(returned, 6),
            "pnl_sol": round(returned - invested, 6),
        }

    def close(self) -> None:
        """Закрывает соединение с БД. Идемпотентно."""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                logger.info("Хранилище позиций закрыто")

    def __enter__(self) -> Self:
        """Вход в контекст (совместимость с ``with``)."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Выход из контекста — закрытие соединения."""
        self.close()
