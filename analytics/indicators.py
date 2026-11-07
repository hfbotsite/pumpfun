"""Потоковые индикаторы импульса.

Модуль вычисляет метрики сделок в скользящем временном окне без обращения к
сети. Он служит источником данных для
:class:`~strategies.momentum_pump.MomentumPumpStrategy` и для бэктестера.

Ключевые сущности:

* :class:`RollingTradeWindow` — окно событий одного пула с ленивой обрезкой
  устаревших сделок и подсчётом агрегатов за произвольный под-интервал;
* :class:`StreamingIndicators` — реестр окон по адресам пулов, принимающий
  события из листенера и отдающий :class:`~models.trade_data.MomentumMetrics`.

Все расчёты выполняются в одном event-loop без блокировок; контейнеры
(``deque``) безопасны для одиночного потока, а операции добавления и обрезки
имеют амортизированную сложность ``O(1)``.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterable

from models.trade_data import MomentumMetrics, TradeEvent, TradeSide

#: Ёмкость окна по умолчанию (число событий) — защита от роста памяти.
DEFAULT_MAX_EVENTS: int = 4096


class RollingTradeWindow:
    """Скользящее окно событий сделок одного пула.

    Окно хранит последние события в ``deque`` с ограниченной ёмкостью и
    обрезает записи старше ``max_window_seconds`` при каждом добавлении и
    запросе. Агрегаты считаются по подынтервалу, что позволяет использовать
    одно окно и для входа (окно 3–5 c), и для выхода (окно 2 c).
    """

    __slots__ = ("_events", "_last_price", "_max_events", "_max_window_seconds")

    def __init__(
        self,
        max_window_seconds: float,
        *,
        max_events: int = DEFAULT_MAX_EVENTS,
    ) -> None:
        """Инициализирует окно.

        Args:
            max_window_seconds: Максимальная глубина окна в секундах; события
                старше этого возраста удаляются.
            max_events: Максимальное число хранимых событий.

        Raises:
            ValueError: Если параметры некорректны.
        """
        if max_window_seconds <= 0:
            raise ValueError("max_window_seconds должен быть > 0")
        if max_events <= 0:
            raise ValueError("max_events должен быть > 0")
        self._events: deque[TradeEvent] = deque(maxlen=max_events)
        self._max_window_seconds = max_window_seconds
        self._max_events = max_events
        self._last_price: float = 0.0

    @property
    def max_window_seconds(self) -> float:
        """Максимальная глубина окна, c."""
        return self._max_window_seconds

    @property
    def max_events(self) -> int:
        """Максимальное число хранимых событий."""
        return self._max_events

    @property
    def last_price(self) -> float:
        """Последняя наблюдённая цена в SOL за токен."""
        return self._last_price

    def __len__(self) -> int:
        """Число событий, хранящихся в окне."""
        return len(self._events)

    def add(self, event: TradeEvent, *, now: float | None = None) -> None:
        """Добавляет событие и обрезает устаревшие записи.

        Args:
            event: Событие сделки.
            now: Текущее время (unix ts); по умолчанию берётся время события.
        """
        self._events.append(event)
        if event.token_amount > 0:
            self._last_price = event.price_sol_per_token
        self._prune(now if now is not None else event.timestamp)

    def extend(
        self, events: Iterable[TradeEvent], *, now: float | None = None
    ) -> None:
        """Добавляет несколько событий пачкой (для бэктеста).

        Args:
            events: Итерируемая последовательность событий в хронологическом
                порядке.
            now: Текущее время (unix ts) для финальной обрезки; по умолчанию —
                время последнего добавленного события.
        """
        last_timestamp = now
        for event in events:
            self._events.append(event)
            if event.token_amount > 0:
                self._last_price = event.price_sol_per_token
            last_timestamp = event.timestamp
        if last_timestamp is not None:
            self._prune(last_timestamp)

    def _prune(self, now: float) -> None:
        """Удаляет события старше глубины окна.

        Args:
            now: Текущее время (unix ts).
        """
        cutoff = now - self._max_window_seconds
        events = self._events
        while events and events[0].timestamp < cutoff:
            events.popleft()

    def latest_event(self) -> TradeEvent | None:
        """Возвращает самое свежее событие.

        Returns:
            Последнее событие или ``None``, если окно пусто.
        """
        return self._events[-1] if self._events else None

    def snapshot(
        self, window_seconds: float | None = None, *, now: float | None = None
    ) -> MomentumMetrics:
        """Считает метрики за подынтервал окна.

        Args:
            window_seconds: Длина подынтервала, c; ``None`` — вся глубина окна.
            now: Текущее время (unix ts); по умолчанию — время последнего
                события либо системное время.

        Returns:
            :class:`~models.trade_data.MomentumMetrics` с агрегатами окна.
        """
        effective_window = window_seconds or self._max_window_seconds
        if effective_window <= 0:
            effective_window = self._max_window_seconds

        if now is None:
            latest = self.latest_event()
            now = latest.timestamp if latest is not None else time.time()

        self._prune(now)
        cutoff = now - effective_window

        buy_count = 0
        sell_count = 0
        buy_volume_sol = 0.0
        sell_volume_sol = 0.0
        largest_sell_sol = 0.0

        for event in reversed(self._events):
            if event.timestamp < cutoff:
                break
            if event.side == TradeSide.BUY:
                buy_count += 1
                buy_volume_sol += event.sol_amount
            else:
                sell_count += 1
                sell_volume_sol += event.sol_amount
                largest_sell_sol = max(largest_sell_sol, event.sol_amount)

        return MomentumMetrics(
            window_seconds=effective_window,
            buy_count=buy_count,
            sell_count=sell_count,
            buy_volume_sol=buy_volume_sol,
            sell_volume_sol=sell_volume_sol,
            largest_sell_sol=largest_sell_sol,
            last_price=self._last_price,
        )

    def clear(self) -> None:
        """Очищает окно."""
        self._events.clear()
        self._last_price = 0.0


class StreamingIndicators:
    """Реестр потоковых индикаторов по пулам.

    Принимает события сделок из листенера, поддерживает отдельное окно на
    каждый пул и по запросу отдаёт агрегированные метрики. Также хранит
    последнюю наблюдаемую цену по каждому пулу.
    """

    __slots__ = ("_last_price", "_max_events", "_max_window_seconds", "_windows")

    def __init__(
        self,
        max_window_seconds: float,
        *,
        max_events_per_pool: int = DEFAULT_MAX_EVENTS,
    ) -> None:
        """Инициализирует реестр.

        Args:
            max_window_seconds: Глубина окна каждого пула, c.
            max_events_per_pool: Максимум событий на один пул.

        Raises:
            ValueError: Если параметры некорректны.
        """
        if max_window_seconds <= 0:
            raise ValueError("max_window_seconds должен быть > 0")
        self._windows: dict[str, RollingTradeWindow] = {}
        self._last_price: dict[str, float] = {}
        self._max_window_seconds = max_window_seconds
        self._max_events = max_events_per_pool

    @property
    def max_window_seconds(self) -> float:
        """Глубина окна каждого пула, c."""
        return self._max_window_seconds

    @property
    def tracked_pools(self) -> int:
        """Число пулов, по которым есть окна."""
        return len(self._windows)

    def window(self, pool_address: str) -> RollingTradeWindow | None:
        """Возвращает окно пула.

        Args:
            pool_address: Адрес пула.

        Returns:
            :class:`RollingTradeWindow` или ``None``, если пул не наблюдался.
        """
        return self._windows.get(pool_address)

    def _ensure_window(self, pool_address: str) -> RollingTradeWindow:
        """Возвращает окно пула, создавая его при необходимости.

        Args:
            pool_address: Адрес пула.

        Returns:
            Существующее или новое окно.
        """
        window = self._windows.get(pool_address)
        if window is None:
            window = RollingTradeWindow(
                self._max_window_seconds, max_events=self._max_events
            )
            self._windows[pool_address] = window
        return window

    def record(self, event: TradeEvent, *, now: float | None = None) -> None:
        """Регистрирует событие сделки.

        Args:
            event: Событие сделки.
            now: Текущее время (unix ts); по умолчанию — время события.
        """
        window = self._ensure_window(event.pool_address)
        window.add(event, now=now)
        if event.token_amount > 0:
            self._last_price[event.pool_address] = event.price_sol_per_token

    def window_metrics(
        self,
        pool_address: str,
        window_seconds: float | None = None,
        *,
        now: float | None = None,
    ) -> MomentumMetrics:
        """Возвращает метрики пула за подынтервал.

        Args:
            pool_address: Адрес пула.
            window_seconds: Длина подынтервала, c; ``None`` — вся глубина окна.
            now: Текущее время (unix ts).

        Returns:
            :class:`~models.trade_data.MomentumMetrics`; пустые метрики, если
            пул ещё не наблюдался.
        """
        window = self._windows.get(pool_address)
        if window is None:
            return MomentumMetrics(
                window_seconds=window_seconds or self._max_window_seconds
            )
        return window.snapshot(window_seconds, now=now)

    def latest_price(self, pool_address: str) -> float:
        """Возвращает последнюю наблюдаемую цену пула.

        Args:
            pool_address: Адрес пула.

        Returns:
            Цена в SOL за токен или ``0.0``, если данных нет.
        """
        return self._last_price.get(pool_address, 0.0)

    def prune_pool(self, pool_address: str) -> None:
        """Удаляет окно и цену пула.

        Args:
            pool_address: Адрес пула.
        """
        self._windows.pop(pool_address, None)
        self._last_price.pop(pool_address, None)

    def clear(self) -> None:
        """Полностью очищает реестр."""
        self._windows.clear()
        self._last_price.clear()
