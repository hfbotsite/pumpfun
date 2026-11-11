"""Свечной агрегатор потока сделок для детекции откатов.

Строит свечи заданного таймфрейма (1 c / 15 c / 60 c) из потока
:class:`TradeEvent` и отвечает на вопрос стратегии «насколько глубокий
откат сейчас на выбранном таймфрейме»:

* **1s** — одна красная свеча с телом ≥ порога;
* **15s** — две подряд красные свечи с суммой тел ≥ порога;
* **60s** — три подряд красные свечи с суммой тел ≥ порога.

Порог (в % снижения) задаёт стратегия как долю стоп-лосса уровня пула —
модуль только измеряет снижение.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from models.trade_data import TradeEvent

#: Таймфреймы свечей в секундах (ключи режима входа).
DIP_TIMEFRAMES: dict[str, float] = {
    "1s": 1.0,
    "15s": 15.0,
    "60s": 60.0,
}

#: Сколько последних свечей держим в памяти.
_MAX_CANDLES = 40


@dataclass(slots=True)
class Candle:
    """Одна свеча таймфрейма.

    Attributes:
        open_ts: Время открытия свечи, unix ts.
        open: Цена открытия (первая сделка свечи), SOL за токен.
        close: Цена закрытия (последняя сделка), SOL за токен.
    """

    open_ts: float
    open: float
    close: float

    @property
    def is_red(self) -> bool:
        """Свеча красная (закрытие ниже открытия)?"""
        return self.close < self.open

    @property
    def decline_pct(self) -> float:
        """Снижение тела свечи, % (0 для зелёной/плоской)."""
        if self.open <= 0 or self.close >= self.open:
            return 0.0
        return (self.open - self.close) / self.open * 100.0


class CandleBuilder:
    """Агрегирует поток сделок одного пула в свечи таймфрейма.

    Args:
        timeframe_seconds: Длина свечи, c (см. ``DIP_TIMEFRAMES``).
    """

    __slots__ = ("_candles", "_timeframe")

    def __init__(self, timeframe_seconds: float) -> None:
        if timeframe_seconds <= 0:
            raise ValueError("timeframe_seconds должен быть > 0")
        self._timeframe = float(timeframe_seconds)
        self._candles: deque[Candle] = deque(maxlen=_MAX_CANDLES)

    @property
    def timeframe(self) -> float:
        """Длина свечи, c."""
        return self._timeframe

    def add(self, event: TradeEvent) -> None:
        """Добавляет событие в текущую/новую свечу.

        Args:
            event: Событие сделки с ценой > 0.
        """
        price = event.price_sol_per_token
        if price <= 0:
            return
        ts = event.timestamp
        bucket = int(ts // self._timeframe) * self._timeframe
        if self._candles and self._candles[-1].open_ts == bucket:
            self._candles[-1].close = price
            return
        self._candles.append(Candle(open_ts=bucket, open=price, close=price))

    def last_n(self, n: int) -> list[Candle]:
        """Последние ``n`` свечей (старые → новые).

        Args:
            n: Сколько свечей вернуть.

        Returns:
            Список :class:`Candle` (может быть короче ``n``).
        """
        return list(self._candles)[-n:]

    def dip_pct(self, mode: str) -> float | None:
        """Глубина текущего отката по режиму, % (``None`` — мало данных).

        Args:
            mode: ``1s`` (1 красная свеча), ``15s`` (2 подряд красные),
                ``60s`` (3 подряд красные).

        Returns:
            Суммарное снижение тел последовательных красных свечей, %;
            ``None``, если свечей недостаточно или режим неизвестен.
        """
        needed = {"1s": 1, "15s": 2, "60s": 3}.get(mode)
        if needed is None:
            return None
        candles = self.last_n(needed)
        if len(candles) < needed:
            return None
        tail = candles[-needed:]
        if not all(c.is_red for c in tail):
            return 0.0
        return sum(c.decline_pct for c in tail)
