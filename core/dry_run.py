"""Dry-run исполнитель: симуляция сделок без обращения к сети.

Модуль реализует контракт :class:`~core.orders.TraderExecutor` для режимов,
в которых отправлять транзакции нельзя или не нужно:

* ``--dry-run`` в live-пайплайне — поток событий настоящий, но ордера
  «исполняются» по уже посчитанной AMM-котировке;
* бэктестер (:mod:`analytics.backtester`) — историческая реконструкция пула.

Fill считается по котировке из заявки (:class:`~core.executor.BuyOrder`,
:class:`~core.executor.SellOrder`), то есть с учётом комиссии пула, ценового
воздействия собственного ордера и дисконта проскальзывания. Это делает
бумажную сделку консервативной: цена исполнения никогда не лучше реальной.

Модуль не хранит состояние позиции — учёт ведёт
:class:`~core.portfolio.PortfolioManager`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Self

from config.settings import LAMPORTS_PER_SOL
from core.executor import BuyOrder, SellOrder
from models.trade_data import OrderResult, TradeSide
from utils.latency import LatencyRegistry, now_us
from utils.logger import get_logger

logger = get_logger()


@dataclass(slots=True)
class DryRunStats:
    """Счётчики бумажных исполнений.

    Attributes:
        buys: Число исполненных покупок.
        sells: Число исполненных продаж.
        sol_spent: Суммарно потрачено SOL.
        sol_received: Суммарно получено SOL.
        rejected: Число отклонённых (некорректных) заявок.
    """

    buys: int = 0
    sells: int = 0
    sol_spent: float = 0.0
    sol_received: float = 0.0
    rejected: int = 0

    @property
    def net_sol(self) -> float:
        """Нетто-результат бумажной торговли, SOL."""
        return self.sol_received - self.sol_spent

    def as_dict(self) -> dict[str, float | int]:
        """Сводка счётчиков для логов и отчётов.

        Returns:
            Словарь с числом сделок и нетто-результатом.
        """
        return {
            "buys": self.buys,
            "sells": self.sells,
            "sol_spent": round(self.sol_spent, 6),
            "sol_received": round(self.sol_received, 6),
            "net_sol": round(self.net_sol, 6),
            "rejected": self.rejected,
        }


class DryRunExecutor:
    """Исполнитель, который «исполняет» заявки по AMM-котировке.

    Возвращает успешный :class:`~models.trade_data.OrderResult` с синтетической
    подписью, чтобы вызывающий код (трейдер, пайплайн, бэктестер) работал по
    тому же пути, что и с :class:`~core.executor.OrderExecutor`.
    """

    def __init__(
        self,
        *,
        latency: LatencyRegistry | None = None,
        simulated_latency_ms: float = 0.0,
        venue: str = "dry_run",
    ) -> None:
        """Инициализирует исполнитель.

        Args:
            latency: Реестр метрик латентности (создаётся при ``None``).
            simulated_latency_ms: Значение латентности, записываемое в
                результат и в метрики (имитация задержки отправки), мс.
            venue: Метка канала исполнения в :class:`OrderResult`.
        """
        self._latency = latency or LatencyRegistry()
        self._stats_ms = self._latency.get("dry_run")
        self._simulated_latency_ms = max(0.0, simulated_latency_ms)
        self._venue = venue
        self._counter = 0
        self.stats = DryRunStats()

    # --------------------------------------------------------------- Свойства
    @property
    def venue(self) -> str:
        """Метка канала исполнения."""
        return self._venue

    @property
    def simulated_latency_ms(self) -> float:
        """Имитируемая задержка исполнения, мс."""
        return self._simulated_latency_ms

    # ---------------------------------------------------------- Жизненный цикл
    async def start(self) -> None:
        """Совместимость с контрактом исполнителя. Ничего не делает."""

    async def close(self) -> None:
        """Совместимость с контрактом исполнителя. Ничего не делает."""

    async def __aenter__(self) -> Self:
        """Вход в контекст."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста."""
        await self.close()

    # --------------------------------------------------------------- Публичное
    async def execute_buy(self, order: BuyOrder) -> OrderResult:
        """Исполняет покупку по котировке заявки.

        Args:
            order: Заявка на покупку.

        Returns:
            Успешный результат с ожидаемым количеством токенов или неуспешный,
            если котировка вырождена.
        """
        decimals = order.pool.base_decimals
        sol_amount = order.sol_amount_lamports / LAMPORTS_PER_SOL
        token_amount = order.expected_tokens / float(10**decimals)
        if sol_amount <= 0 or token_amount <= 0:
            self.stats.rejected += 1
            logger.warning(
                "Dry-run: покупка {} отклонена (sol={:.6f}, tokens={:.6f})",
                order.pool.base_mint,
                sol_amount,
                token_amount,
            )
            return OrderResult.failed(
                side=TradeSide.BUY,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error="вырожденная котировка покупки",
                venue=self._venue,
            )

        started = now_us()
        self.stats.buys += 1
        self.stats.sol_spent += sol_amount
        elapsed_us = self._record_latency(started)
        logger.info(
            "Dry-run BUY {}: {:.6f} SOL → {:.6f} токенов (impact {:.2f}%)",
            order.pool.base_mint,
            sol_amount,
            token_amount,
            order.price_impact_pct,
        )
        return OrderResult(
            success=True,
            side=TradeSide.BUY,
            base_mint=order.pool.base_mint,
            pool_address=order.pool.pool_address,
            signature=self._next_signature("buy"),
            sol_amount=sol_amount,
            token_amount=token_amount,
            price_impact_pct=order.price_impact_pct,
            venue=self._venue,
            confirmed=True,
            submitted_at=time.time(),
            latency_ms=elapsed_us / 1000.0,
        )

    async def execute_sell(self, order: SellOrder) -> OrderResult:
        """Исполняет продажу по котировке заявки.

        Args:
            order: Заявка на продажу.

        Returns:
            Успешный результат с полученным объёмом SOL или неуспешный,
            если котировка вырождена.
        """
        decimals = order.pool.base_decimals
        token_amount = order.token_amount / float(10**decimals)
        sol_amount = order.quote.amount_out / LAMPORTS_PER_SOL
        if sol_amount <= 0 or token_amount <= 0:
            self.stats.rejected += 1
            logger.warning(
                "Dry-run: продажа {} отклонена (sol={:.6f}, tokens={:.6f})",
                order.pool.base_mint,
                sol_amount,
                token_amount,
            )
            return OrderResult.failed(
                side=TradeSide.SELL,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error="вырожденная котировка продажи",
                venue=self._venue,
            )

        started = now_us()
        self.stats.sells += 1
        self.stats.sol_received += sol_amount
        elapsed_us = self._record_latency(started)
        logger.info(
            "Dry-run SELL {}: {:.6f} токенов → {:.6f} SOL",
            order.pool.base_mint,
            token_amount,
            sol_amount,
        )
        return OrderResult(
            success=True,
            side=TradeSide.SELL,
            base_mint=order.pool.base_mint,
            pool_address=order.pool.pool_address,
            signature=self._next_signature("sell"),
            sol_amount=sol_amount,
            token_amount=token_amount,
            price_impact_pct=order.price_impact_pct,
            venue=self._venue,
            confirmed=True,
            submitted_at=time.time(),
            latency_ms=elapsed_us / 1000.0,
        )

    # -------------------------------------------------------------- Внутреннее
    def _next_signature(self, side: str) -> str:
        """Собирает синтетическую подпись сделки.

        Args:
            side: Направление (``buy``/``sell``).

        Returns:
            Строка вида ``dryrun-buy-000001``.
        """
        self._counter += 1
        return f"dryrun-{side}-{self._counter:06d}"

    def _record_latency(self, started_us: int) -> float:
        """Фиксирует метрику латентности бумажного исполнения.

        Args:
            started_us: Монотонное время старта, микросекунды.

        Returns:
            Измеренная (или имитируемая) задержка, микросекунды.
        """
        measured_us = float(now_us() - started_us)
        simulated_us = self._simulated_latency_ms * 1000.0
        elapsed_us = max(measured_us, simulated_us)
        self._stats_ms.observe_us(int(elapsed_us))
        return elapsed_us
