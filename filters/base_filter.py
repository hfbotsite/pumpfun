"""Абстрактный базовый класс фильтра допуска сделки.

Фильтры образуют цепочку предторговых проверок. Каждый фильтр получает
контекст (снимок пула, метрики, безопасность контракта) и возвращает
:class:`~models.trade_data.FilterResult` с вердиктом и диагностикой.

Фильтры должны быть «чистыми» (без сетевых вызовов): сетевые данные
подготавливаются листенером заранее и передаются в :class:`FilterContext`.
Это делает всю цепочку детерминированной и пригодной для бэктестера.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from models.trade_data import (
    FilterResult,
    MomentumMetrics,
    PoolInfo,
    TokenSecurity,
)


@dataclass(slots=True)
class FilterContext:
    """Контекст, передаваемый во все фильтры по одному активу.

    Attributes:
        pool: Снимок состояния пула.
        security: Данные безопасности токен-контракта.
        metrics: Потоковые метрики импульса.
        trade_amount_sol: Размер планируемого входного ордера, SOL.
        timestamp: Время формирования контекста (unix ts), c.
    """

    pool: PoolInfo
    trade_amount_sol: float
    security: TokenSecurity | None = None
    metrics: MomentumMetrics | None = None
    timestamp: float = field(default_factory=time.time)
    #: Глубина отката на выбранном таймфрейме, % (``None`` — мало данных свечей).
    dip_pct: float | None = None
    #: Контекст усреднения (dip-стратегия) вместо нового входа (momentum).
    is_average: bool = False

    @property
    def base_mint(self) -> str:
        """Mint мемкоина из контекста."""
        return self.pool.base_mint

    @property
    def pool_address(self) -> str:
        """Адрес пула из контекста."""
        return self.pool.pool_address


class BaseFilter(ABC):
    """Базовый интерфейс предторгового фильтра.

    Наследники реализуют :meth:`evaluate` и задают :attr:`name`. Метод
    :meth:`__call__` позволяет использовать фильтр как функцию.
    """

    #: Человекочитаемое имя фильтра (используется в отчётах).
    name: str = "base_filter"

    def __call__(self, context: FilterContext) -> FilterResult:
        """Вызывает :meth:`evaluate`.

        Args:
            context: Контекст фильтрации.

        Returns:
            Результат проверки.
        """
        return self.evaluate(context)

    @abstractmethod
    def evaluate(self, context: FilterContext) -> FilterResult:
        """Оценивает контекст и возвращает вердикт фильтра.

        Args:
            context: Контекст фильтрации.

        Returns:
            :class:`~models.trade_data.FilterResult` с вердиктом.
        """
        raise NotImplementedError

    def _ok(self, **metrics: object) -> FilterResult:
        """Создаёт успешный результат от имени этого фильтра.

        Args:
            **metrics: Диагностические метрики.

        Returns:
            Успешный результат.
        """
        return FilterResult.ok(self.name, **metrics)

    def _reject(self, reason: str, **metrics: object) -> FilterResult:
        """Создаёт отрицательный результат от имени этого фильтра.

        Args:
            reason: Причина отказа.
            **metrics: Диагностические метрики.

        Returns:
            Отрицательный результат.
        """
        return FilterResult.reject(self.name, reason, **metrics)


class FilterChain:
    """Последовательная цепочка фильтров с коротким замыканием.

    Фильтры выполняются по порядку; первый же отказ прекращает дальнейшие
    проверки (fail-fast) — это экономит вычисления на «мусорных» токенах.
    """

    __slots__ = ("_filters",)

    def __init__(self, filters: list[BaseFilter] | None = None) -> None:
        """Инициализирует цепочку.

        Args:
            filters: Начальный список фильтров (в порядке применения).
        """
        self._filters: list[BaseFilter] = list(filters or [])

    def add(self, flt: BaseFilter) -> FilterChain:
        """Добавляет фильтр в конец цепочки.

        Args:
            flt: Фильтр для добавления.

        Returns:
            Сам объект цепочки (для чейнинга).
        """
        self._filters.append(flt)
        return self

    @property
    def filters(self) -> tuple[BaseFilter, ...]:
        """Кортеж фильтров цепочки."""
        return tuple(self._filters)

    def run(self, context: FilterContext) -> list[FilterResult]:
        """Прогоняет контекст через всю цепочку с коротким замыканием.

        Args:
            context: Контекст фильтрации.

        Returns:
            Список результатов до первого отказа включительно.
        """
        results: list[FilterResult] = []
        for flt in self._filters:
            result = flt.evaluate(context)
            results.append(result)
            if not result.passed:
                break
        return results
