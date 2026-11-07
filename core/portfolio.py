"""Менеджер торговых позиций (memory + опциональная SQLite-персистентность).

Модуль — единственный источник истины о том, какие позиции открыты, сколько
токенов осталось и какой PnL уже реализован. Его используют и live-пайплайн
(:mod:`core.trader`), и бэктестер (:mod:`analytics.backtester`), поэтому он не
делает сетевых вызовов и не принимает торговых решений — только учитывает
состояние.

Если передан :class:`~core.persistence.PortfolioStore`, менеджер работает в
режиме **write-through**: каждое изменение позиции немедленно пишется в БД, а
метод :meth:`PortfolioManager.load_open_positions` восстанавливает состояние
после перезапуска. Без store менеджер ведёт себя как прежде — чисто в памяти.

Ключевые инварианты:

* одновременно открыто не более ``max_concurrent_positions`` позиций;
* по одному ``base_mint`` не может быть двух позиций (скальпинг не усредняет);
* частичная продажа уменьшает ``token_amount`` и увеличивает ``realized_sol``;
* позиция удаляется, когда остаток токенов равен нулю.

Все суммы — в SOL, количества токенов — в человекочитаемых единицах.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from core.persistence import PortfolioStore
from models.trade_data import DEXType, Position
from utils.logger import get_logger

logger = get_logger()

#: Доля остатка, ниже которой позиция считается полностью закрытой.
DUST_FRACTION: float = 1e-6


class PortfolioError(RuntimeError):
    """Ошибка учёта позиции (нарушение инвариантов портфеля)."""


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    """Запись о полностью закрытой сделке (для статистики и логов).

    Attributes:
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
        exit_reason: Причина закрытия (человекочитаемая метка).
    """

    base_mint: str
    pool_address: str
    dex: DEXType
    entry_price: float
    exit_price: float
    invested_sol: float
    returned_sol: float
    opened_at: float
    closed_at: float
    peak_price: float
    exit_reason: str

    @property
    def pnl_sol(self) -> float:
        """Финансовый результат сделки, SOL."""
        return self.returned_sol - self.invested_sol

    @property
    def pnl_pct(self) -> float:
        """Финансовый результат сделки, % от вложенного объёма."""
        if self.invested_sol <= 0:
            return 0.0
        return self.pnl_sol / self.invested_sol * 100.0

    @property
    def hold_seconds(self) -> float:
        """Длительность удержания позиции, c."""
        return max(0.0, self.closed_at - self.opened_at)


@dataclass(slots=True)
class PortfolioStats:
    """Агрегированная статистика портфеля."""

    trades: int = 0
    wins: int = 0
    losses: int = 0
    invested_sol: float = 0.0
    returned_sol: float = 0.0
    hold_seconds_total: float = 0.0

    @property
    def pnl_sol(self) -> float:
        """Суммарный реализованный PnL, SOL."""
        return self.returned_sol - self.invested_sol

    @property
    def pnl_pct(self) -> float:
        """Суммарный реализованный PnL, % от оборота."""
        if self.invested_sol <= 0:
            return 0.0
        return self.pnl_sol / self.invested_sol * 100.0

    @property
    def win_rate_pct(self) -> float:
        """Доля прибыльных сделок, %."""
        if self.trades == 0:
            return 0.0
        return 100.0 * self.wins / self.trades

    @property
    def avg_hold_seconds(self) -> float:
        """Среднее время удержания, c."""
        if self.trades == 0:
            return 0.0
        return self.hold_seconds_total / self.trades


class PortfolioManager:
    """Учёт открытых позиций и накопленной статистики.

    Менеджер идемпотентен относительно повторных вызовов: попытка открыть
    позицию по уже занятому mint возвращает существующую запись (``None``
    при нарушении лимита), а продажа большего объёма, чем есть в позиции,
    обрезается до остатка.
    """

    __slots__ = (
        "_closed",
        "_max_positions",
        "_positions",
        "_reserved_sol",
        "_stats",
        "_store",
    )

    def __init__(
        self,
        *,
        max_concurrent_positions: int = 1,
        store: PortfolioStore | None = None,
    ) -> None:
        """Инициализирует пустой портфель.

        Args:
            max_concurrent_positions: Максимум одновременно открытых позиций.
            store: SQLite-хранилище write-through (``None`` — только память).

        Raises:
            PortfolioError: Если лимит меньше единицы.
        """
        if max_concurrent_positions < 1:
            raise PortfolioError("max_concurrent_positions должен быть >= 1")
        self._positions: dict[str, Position] = {}
        self._closed: list[ClosedTrade] = []
        self._stats = PortfolioStats()
        self._max_positions = max_concurrent_positions
        self._reserved_sol: float = 0.0
        self._store = store

    # ------------------------------------------------------------------ Свойства
    @property
    def max_concurrent_positions(self) -> int:
        """Максимум одновременно открытых позиций."""
        return self._max_positions

    @property
    def open_count(self) -> int:
        """Число открытых позиций."""
        return len(self._positions)

    @property
    def stats(self) -> PortfolioStats:
        """Накопленная статистика портфеля."""
        return self._stats

    @property
    def closed_trades(self) -> tuple[ClosedTrade, ...]:
        """Кортеж закрытых сделок в порядке закрытия."""
        return tuple(self._closed)

    def load_open_positions(self) -> int:
        """Восстанавливает открытые позиции из хранилища (после рестарта).

        Вызов до начала торговли: позиции из БД попадают в memory-слой и
        снова учитываются лимитами и стратегией выходов.

        Returns:
            Число восстановленных позиций.
        """
        if self._store is None:
            return 0
        restored = 0
        for position in self._store.load_open_positions():
            if position.base_mint in self._positions:
                continue
            self._positions[position.base_mint] = position
            self._reserved_sol += position.entry_sol
            restored += 1
        if restored:
            logger.warning(
                "Восстановлено открытых позиций из БД: {} ({}); "
                "бот продолжит управлять их выходами",
                restored,
                [p.base_mint for p in self.all_positions()],
            )
        return restored

    def can_open(self, *, capital_sol: float | None = None) -> bool:
        """Проверяет, можно ли открыть ещё одну позицию.

        Args:
            capital_sol: Свободный капитал, SOL (``None`` — не проверять).

        Returns:
            ``True``, если лимиты по числу позиций и капиталу не нарушены.
        """
        if len(self._positions) >= self._max_positions:
            return False
        if capital_sol is None:
            return True
        return capital_sol - self._reserved_sol > 0

    def get(self, base_mint: str) -> Position | None:
        """Возвращает открытую позицию по mint.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            :class:`~models.trade_data.Position` или ``None``.
        """
        return self._positions.get(base_mint)

    def all_positions(self) -> list[Position]:
        """Возвращает список открытых позиций.

        Returns:
            Новый список :class:`~models.trade_data.Position`.
        """
        return list(self._positions.values())

    # ------------------------------------------------------------------ Открытие
    def open_position(
        self,
        *,
        base_mint: str,
        pool_address: str,
        dex: DEXType,
        entry_price: float,
        entry_sol: float,
        token_amount: float,
        signature: str | None = None,
        opened_at: float | None = None,
    ) -> Position | None:
        """Открывает позицию.

        Args:
            base_mint: Mint мемкоина.
            pool_address: Адрес пула.
            dex: DEX/платформа.
            entry_price: Цена входа, SOL за токен.
            entry_sol: Вложенный объём, SOL.
            token_amount: Полученное количество токенов (человекочитаемое).
            signature: Подпись транзакции входа.
            opened_at: Время открытия, unix ts (по умолчанию — сейчас).

        Returns:
            Созданная :class:`~models.trade_data.Position` либо ``None``, если
            позиция уже открыта или нарушен лимит.

        Raises:
            PortfolioError: Если аргументы некорректны.
        """
        if base_mint in self._positions:
            logger.debug("Позиция по {} уже открыта — пропуск", base_mint)
            return None
        if len(self._positions) >= self._max_positions:
            return None
        if entry_price <= 0 or entry_sol <= 0 or token_amount <= 0:
            raise PortfolioError(
                "Некорректные параметры открытия позиции: "
                f"price={entry_price}, sol={entry_sol}, tokens={token_amount}"
            )

        position = Position(
            base_mint=base_mint,
            pool_address=pool_address,
            dex=dex,
            entry_price=entry_price,
            entry_sol=entry_sol,
            token_amount=token_amount,
            initial_token_amount=token_amount,
            opened_at=opened_at if opened_at is not None else time.time(),
            peak_price=entry_price,
            tp1_taken=False,
            realized_sol=0.0,
            signature=signature,
        )
        self._positions[base_mint] = position
        self._reserved_sol += entry_sol
        self._persist(position)
        logger.info(
            "Открыта позиция {}: price={:.9f} SOL, in={:.4f} SOL, tokens={:.4f}",
            base_mint,
            entry_price,
            entry_sol,
            token_amount,
        )
        return position

    # -------------------------------------------------------------- Обновление
    def add_to_position(
        self,
        base_mint: str,
        *,
        added_tokens: float,
        added_sol: float,
    ) -> Position | None:
        """Усредняет позицию: добавляет токены и пересчитывает цену входа.

        Новая цена входа — средневзвешенная: ``(entry_sol + added_sol) /
        (токены + added_tokens)``. ``initial_token_amount`` растёт вместе с
        ``token_amount``, чтобы ``remaining_fraction`` оставался согласованным
        (доля позиции к продаже считается от начального количества).

        Args:
            base_mint: Mint мемкоина.
            added_tokens: Добавленное количество токенов (человекочитаемое).
            added_sol: Потраченный объём, SOL.

        Returns:
            Обновлённая позиция или ``None``, если позиция не открыта.

        Raises:
            PortfolioError: Если добавление неположительное.
        """
        position = self._positions.get(base_mint)
        if position is None:
            return None
        if added_tokens <= 0 or added_sol <= 0:
            raise PortfolioError(
                f"Некорректное усреднение: tokens={added_tokens}, sol={added_sol}"
            )
        position.entry_sol += added_sol
        position.token_amount += added_tokens
        position.initial_token_amount = position.token_amount
        position.entry_price = position.entry_sol / position.token_amount
        self._persist(position)
        logger.info(
            "Позиция {} усреднена: +{:.4f} токенов за {:.4f} SOL → "
            "ср. цена входа {:.9f}, всего {:.4f} токенов",
            base_mint,
            added_tokens,
            added_sol,
            position.entry_price,
            position.token_amount,
        )
        return position

    def mark_price(self, base_mint: str, price: float) -> Position | None:
        """Обновляет зарегистрированный пик цены позиции.

        Args:
            base_mint: Mint мемкоина.
            price: Текущая цена, SOL за токен.

        Returns:
            Обновлённая позиция или ``None``, если позиция отсутствует.
        """
        position = self._positions.get(base_mint)
        if position is None or price <= 0:
            return position
        previous_peak = position.peak_price
        position.update_peak(price)
        if position.peak_price > previous_peak:
            self._persist(position)
        return position

    def mark_tp1_taken(self, base_mint: str) -> None:
        """Помечает, что первый тейк-профит забран.

        Args:
            base_mint: Mint мемкоина.
        """
        position = self._positions.get(base_mint)
        if position is not None:
            position.tp1_taken = True
            self._persist(position)

    # ------------------------------------------------------------------ Продажа
    def apply_sell(
        self,
        *,
        base_mint: str,
        sold_tokens: float,
        received_sol: float,
        exit_price: float,
        sold_at: float | None = None,
        exit_reason: str = "manual",
    ) -> ClosedTrade | None:
        """Учитывает исполненную продажу (частичную или полную).

        Args:
            base_mint: Mint мемкоина.
            sold_tokens: Проданное количество токенов (человекочитаемое).
            received_sol: Полученный объём, SOL.
            exit_price: Средняя цена исполнения продажи, SOL за токен.
            sold_at: Время продажи, unix ts (по умолчанию — сейчас).
            exit_reason: Причина выхода (для журнала).

        Returns:
            :class:`ClosedTrade`, если позиция закрыта полностью, иначе ``None``.

        Raises:
            PortfolioError: Если аргументы некорректны.
        """
        position = self._positions.get(base_mint)
        if position is None:
            return None
        if sold_tokens <= 0 or received_sol < 0:
            raise PortfolioError(
                f"Некорректная продажа: tokens={sold_tokens}, sol={received_sol}"
            )

        effective_tokens = min(sold_tokens, position.token_amount)
        position.token_amount = max(0.0, position.token_amount - effective_tokens)
        position.realized_sol += received_sol

        remaining_fraction = position.remaining_fraction
        if remaining_fraction > DUST_FRACTION:
            self._persist(position)
            return None

        position.token_amount = 0.0
        return self._finalize(
            position,
            exit_price=exit_price,
            sold_at=sold_at if sold_at is not None else time.time(),
            exit_reason=exit_reason,
        )

    def force_close(
        self,
        base_mint: str,
        *,
        exit_price: float,
        received_sol: float | None = None,
        closed_at: float | None = None,
        exit_reason: str = "manual",
    ) -> ClosedTrade | None:
        """Закрывает позицию целиком по указанной цене.

        Используется при отмене/сбое выхода, когда точный объём продажи
        неизвестен: расчёт возврата выполняется по ``exit_price``.

        Args:
            base_mint: Mint мемкоина.
            exit_price: Цена закрытия, SOL за токен.
            received_sol: Фактически полученный объём (``None`` — оценка).
            closed_at: Время закрытия, unix ts (по умолчанию — сейчас).
            exit_reason: Причина выхода.

        Returns:
            :class:`ClosedTrade` или ``None``, если позиции нет.
        """
        position = self._positions.get(base_mint)
        if position is None:
            return None
        estimated = (
            received_sol
            if received_sol is not None
            else position.token_amount * exit_price
        )
        position.realized_sol += max(0.0, estimated)
        position.token_amount = 0.0
        return self._finalize(
            position,
            exit_price=exit_price,
            sold_at=closed_at if closed_at is not None else time.time(),
            exit_reason=exit_reason,
        )

    # -------------------------------------------------------------- Внутреннее
    def _finalize(
        self,
        position: Position,
        *,
        exit_price: float,
        sold_at: float,
        exit_reason: str,
    ) -> ClosedTrade:
        """Удаляет позицию и регистрирует закрытую сделку.

        Args:
            position: Закрываемая позиция.
            exit_price: Цена закрытия, SOL за токен.
            sold_at: Время закрытия, unix ts.
            exit_reason: Причина закрытия.

        Returns:
            Запись :class:`ClosedTrade`.
        """
        self._positions.pop(position.base_mint, None)
        self._reserved_sol = max(0.0, self._reserved_sol - position.entry_sol)
        if self._store is not None:
            self._store.remove_position(position.base_mint)
            self._store.record_closed_trade(
                base_mint=position.base_mint,
                pool_address=position.pool_address,
                dex=position.dex,
                entry_price=position.entry_price,
                exit_price=exit_price,
                invested_sol=position.entry_sol,
                returned_sol=position.realized_sol,
                opened_at=position.opened_at,
                closed_at=sold_at,
                peak_price=position.peak_price,
                exit_reason=exit_reason,
            )

        trade = ClosedTrade(
            base_mint=position.base_mint,
            pool_address=position.pool_address,
            dex=position.dex,
            entry_price=position.entry_price,
            exit_price=exit_price,
            invested_sol=position.entry_sol,
            returned_sol=position.realized_sol,
            opened_at=position.opened_at,
            closed_at=sold_at,
            peak_price=position.peak_price,
            exit_reason=exit_reason,
        )
        self._closed.append(trade)
        self._stats.trades += 1
        self._stats.invested_sol += trade.invested_sol
        self._stats.returned_sol += trade.returned_sol
        self._stats.hold_seconds_total += trade.hold_seconds
        if trade.pnl_sol >= 0:
            self._stats.wins += 1
        else:
            self._stats.losses += 1

        logger.info(
            "Закрыта позиция {} ({}): PnL={:+.4f} SOL ({:+.2f}%), удержание {:.2f}c",
            trade.base_mint,
            exit_reason,
            trade.pnl_sol,
            trade.pnl_pct,
            trade.hold_seconds,
        )
        return trade

    def summary(self) -> dict[str, float | int]:
        """Возвращает сводку состояния портфеля для логов.

        Returns:
            Словарь с числом позиций и агрегированной статистикой.
        """
        return {
            "open_positions": self.open_count,
            "reserved_sol": round(self._reserved_sol, 6),
            "closed_trades": self._stats.trades,
            "wins": self._stats.wins,
            "losses": self._stats.losses,
            "win_rate_pct": round(self._stats.win_rate_pct, 2),
            "pnl_sol": round(self._stats.pnl_sol, 6),
            "pnl_pct": round(self._stats.pnl_pct, 4),
            "avg_hold_seconds": round(self._stats.avg_hold_seconds, 3),
        }

    # -------------------------------------------------------------- Персистентность
    def _persist(self, position: Position) -> None:
        """Пишет позицию в хранилище (write-through), если оно подключено.

        Сбой записи логируется, но не ломает торговлю: memory-слой остаётся
        источником истины в рамках процесса.

        Args:
            position: Текущее состояние позиции.
        """
        if self._store is None:
            return
        try:
            self._store.upsert_position(position)
        except Exception:
            logger.exception(
                "Не удалось сохранить позицию {} в БД", position.base_mint
            )
