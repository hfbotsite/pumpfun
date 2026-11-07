"""Микросекундные таймеры и статистика задержек.

Для HFT-скальпинга критично измерять задержки с микросекундной точностью:
время RPC-ответа, задержку отправки бандла, задержку между сигналом и выходом.
Модуль предоставляет:

* :func:`now_us` — монотонные микросекунды;
* :class:`LatencyStats` — скользящая статистика (avg/p50/p95/p99/max);
* :class:`LatencyRegistry` — реестр именованных метрик.
"""

from __future__ import annotations

import time
from collections import deque

#: Ёмкость скользящего окна статистики по умолчанию.
DEFAULT_WINDOW: int = 1024


def now_us() -> int:
    """Возвращает текущее монотонное время в микросекундах.

    Returns:
        Монотонные микросекунды (для расчёта задержек, не для wall-clock).
    """
    return time.perf_counter_ns() // 1_000


class LatencyStats:
    """Скользящая статистика задержек (в миллисекундах).

    Хранит последние ``window`` значений и считает агрегаты и перцентили.
    Класс не потокобезопасен и рассчитан на использование в одном event-loop;
    асинхронные операции добавляются без блокировок, так как ``deque.append``
    атомарен в рамках одного потока.
    """

    __slots__ = ("_count", "_max_ms", "_min_ms", "_name", "_samples", "_total_ms", "_window")

    def __init__(self, name: str, window: int = DEFAULT_WINDOW) -> None:
        """Инициализирует статистику.

        Args:
            name: Человекочитаемое имя метрики.
            window: Размер скользящего окна для перцентилей.
        """
        self._name = name
        self._window = max(1, window)
        self._samples: deque[float] = deque(maxlen=self._window)
        self._count = 0
        self._total_ms = 0.0
        self._max_ms = 0.0
        self._min_ms = float("inf")

    @property
    def name(self) -> str:
        """Имя метрики."""
        return self._name

    @property
    def count(self) -> int:
        """Общее число наблюдений."""
        return self._count

    @property
    def avg_ms(self) -> float:
        """Среднее по всем наблюдениям, мс."""
        if self._count == 0:
            return 0.0
        return self._total_ms / self._count

    @property
    def max_ms(self) -> float:
        """Максимум по всем наблюдениям, мс."""
        return self._max_ms

    @property
    def min_ms(self) -> float:
        """Минимум по всем наблюдениям, мс (0 при отсутствии данных)."""
        return 0.0 if self._count == 0 else self._min_ms

    def observe(self, value_ms: float) -> None:
        """Добавляет наблюдение.

        Args:
            value_ms: Измеренная задержка в миллисекундах.
        """
        if value_ms < 0:
            return
        self._samples.append(value_ms)
        self._count += 1
        self._total_ms += value_ms
        self._max_ms = max(self._max_ms, value_ms)
        self._min_ms = min(self._min_ms, value_ms)

    def observe_us(self, value_us: int) -> None:
        """Добавляет наблюдение в микросекундах.

        Args:
            value_us: Измеренная задержка в микросекундах.
        """
        self.observe(value_us / 1_000)

    def percentile(self, pct: float) -> float:
        """Считает перцентиль по скользящему окну.

        Args:
            pct: Перцентиль в диапазоне 0..100.

        Returns:
            Значение перцентиля в миллисекундах (0 при отсутствии данных).
        """
        if not self._samples:
            return 0.0
        ordered = sorted(self._samples)
        if pct <= 0:
            return ordered[0]
        if pct >= 100:
            return ordered[-1]
        rank = round((pct / 100.0) * (len(ordered) - 1))
        return ordered[rank]

    def as_dict(self) -> dict[str, float | int]:
        """Сводка метрик для логирования.

        Returns:
            Словарь с count/avg/p50/p95/p99/min/max.
        """
        return {
            "count": self._count,
            "avg_ms": round(self.avg_ms, 4),
            "p50_ms": round(self.percentile(50), 4),
            "p95_ms": round(self.percentile(95), 4),
            "p99_ms": round(self.percentile(99), 4),
            "min_ms": round(self.min_ms, 4),
            "max_ms": round(self.max_ms, 4),
        }


class LatencyRegistry:
    """Реестр именованных метрик задержек.

    Позволяет собирать статистику по разным участкам (RPC, WS, submit, parse)
    и выводить единую сводку.
    """

    __slots__ = ("_stats", "_window")

    def __init__(self, window: int = DEFAULT_WINDOW) -> None:
        """Инициализирует реестр.

        Args:
            window: Размер окна для каждой метрики.
        """
        self._stats: dict[str, LatencyStats] = {}
        self._window = window

    def get(self, name: str) -> LatencyStats:
        """Возвращает (создавая при необходимости) метрику по имени.

        Args:
            name: Имя метрики.

        Returns:
            Объект :class:`LatencyStats`.
        """
        stats = self._stats.get(name)
        if stats is None:
            stats = LatencyStats(name, window=self._window)
            self._stats[name] = stats
        return stats

    def observe(self, name: str, value_ms: float) -> None:
        """Регистрирует задержку для метрики.

        Args:
            name: Имя метрики.
            value_ms: Значение в миллисекундах.
        """
        self.get(name).observe(value_ms)

    def summary(self) -> dict[str, dict[str, float | int]]:
        """Возвращает сводку по всем метрикам.

        Returns:
            Словарь ``имя -> статистика``.
        """
        return {name: stats.as_dict() for name, stats in self._stats.items()}
