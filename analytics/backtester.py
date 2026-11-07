"""Бэктестер: прогон исторических событий через живые фильтры и стратегию.

Ключевой принцип модуля — **переиспользование, а не копия**. Бэктест не
содержит собственной торговой логики: он собирает тот же
:class:`~core.trader.MomentumTrader` с тем же
:class:`~strategies.momentum_pump.MomentumPumpStrategy`, той же цепочкой
фильтров и теми же расчётами объёмов (:mod:`core.orders`), подменяя только два
внешних интерфейса:

* исполнитель — :class:`~core.dry_run.DryRunExecutor` (fill по AMM-котировке);
* поставщик безопасности — статическая запись из сценария вместо RPC.

Состояние пула восстанавливается по потоку сделок
(:class:`~analytics.replay.ReplayPoolState`), поэтому котировки входа и выхода
считаются против реальных резервов на момент события.

Метрики отчёта: PnL, win-rate, среднее время удержания, максимальная просадка,
лучшая/худшая сделка и распределение по причинам выхода.
"""

from __future__ import annotations

from dataclasses import dataclass

from analytics.replay import ReplayPoolState, ReplayScenario
from config.settings import settings
from core.dry_run import DryRunExecutor
from core.orders import TraderExecutor
from core.portfolio import ClosedTrade, PortfolioManager, PortfolioStats
from core.trader import MomentumTrader
from models.trade_data import PoolInfo, TokenSecurity
from strategies.base_strategy import BaseStrategy
from utils.logger import get_logger

logger = get_logger()

#: Метка причины закрытия «позиция не успела закрыться до конца данных».
BACKTEST_END_REASON: str = "backtest_end"


@dataclass(frozen=True, slots=True)
class BacktestReport:
    """Итоговый отчёт прогона исторических данных.

    Attributes:
        pool_address: Адрес пула.
        base_mint: Mint мемкоина.
        events: Число обработанных событий.
        duration_seconds: Длительность сценария, c.
        trades: Число закрытых сделок.
        wins: Прибыльных сделок (безубыток считается прибыльной).
        losses: Убыточных сделок.
        invested_sol: Суммарно вложено, SOL.
        returned_sol: Суммарно возвращено, SOL.
        pnl_sol: Итоговый PnL, SOL.
        pnl_pct: Итоговый PnL, % от оборота.
        win_rate_pct: Доля прибыльных сделок, %.
        avg_hold_seconds: Среднее время удержания, c.
        max_drawdown_sol: Максимальная просадка реализованного PnL, SOL.
        max_drawdown_pct: Максимальная просадка, % от начального капитала.
        best_trade_pct: Лучшая сделка, %.
        worst_trade_pct: Худшая сделка, %.
        exit_reasons: Распределение причин выхода.
        closed_trades: Детали закрытых сделок.
    """

    pool_address: str
    base_mint: str
    events: int
    duration_seconds: float
    trades: int
    wins: int
    losses: int
    invested_sol: float
    returned_sol: float
    pnl_sol: float
    pnl_pct: float
    win_rate_pct: float
    avg_hold_seconds: float
    max_drawdown_sol: float
    max_drawdown_pct: float
    best_trade_pct: float
    worst_trade_pct: float
    exit_reasons: dict[str, int]
    closed_trades: tuple[ClosedTrade, ...]

    @classmethod
    def from_portfolio(
        cls,
        *,
        pool_address: str,
        base_mint: str,
        events: int,
        duration_seconds: float,
        stats: PortfolioStats,
        closed_trades: tuple[ClosedTrade, ...],
        initial_capital_sol: float,
    ) -> BacktestReport:
        """Собирает отчёт из состояния портфеля.

        Args:
            pool_address: Адрес пула.
            base_mint: Mint мемкоина.
            events: Число обработанных событий.
            duration_seconds: Длительность сценария, c.
            stats: Агрегированная статистика портфеля.
            closed_trades: Закрытые сделки в порядке закрытия.
            initial_capital_sol: Начальный капитал для расчёта просадки, SOL.

        Returns:
            Заполненный :class:`BacktestReport`.
        """
        max_drawdown_sol = _max_drawdown_sol(closed_trades)
        drawdown_pct = (
            max_drawdown_sol / initial_capital_sol * 100.0
            if initial_capital_sol > 0
            else 0.0
        )
        pnl_pcts = [trade.pnl_pct for trade in closed_trades]
        return cls(
            pool_address=pool_address,
            base_mint=base_mint,
            events=events,
            duration_seconds=duration_seconds,
            trades=stats.trades,
            wins=stats.wins,
            losses=stats.losses,
            invested_sol=stats.invested_sol,
            returned_sol=stats.returned_sol,
            pnl_sol=stats.pnl_sol,
            pnl_pct=stats.pnl_pct,
            win_rate_pct=stats.win_rate_pct,
            avg_hold_seconds=stats.avg_hold_seconds,
            max_drawdown_sol=max_drawdown_sol,
            max_drawdown_pct=drawdown_pct,
            best_trade_pct=max(pnl_pcts) if pnl_pcts else 0.0,
            worst_trade_pct=min(pnl_pcts) if pnl_pcts else 0.0,
            exit_reasons=_count_exit_reasons(closed_trades),
            closed_trades=closed_trades,
        )

    def to_dict(self) -> dict[str, object]:
        """JSON-совместимое представление отчёта (для веб-дашборда).

        Returns:
            Словарь со всеми метриками и деталями закрытых сделок.
        """
        return {
            "pool_address": self.pool_address,
            "base_mint": self.base_mint,
            "events": self.events,
            "duration_seconds": self.duration_seconds,
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "invested_sol": self.invested_sol,
            "returned_sol": self.returned_sol,
            "pnl_sol": self.pnl_sol,
            "pnl_pct": self.pnl_pct,
            "win_rate_pct": self.win_rate_pct,
            "avg_hold_seconds": self.avg_hold_seconds,
            "max_drawdown_sol": self.max_drawdown_sol,
            "max_drawdown_pct": self.max_drawdown_pct,
            "best_trade_pct": self.best_trade_pct,
            "worst_trade_pct": self.worst_trade_pct,
            "exit_reasons": dict(self.exit_reasons),
            "closed_trades": [
                {
                    "base_mint": trade.base_mint,
                    "exit_reason": trade.exit_reason,
                    "pnl_sol": trade.pnl_sol,
                    "pnl_pct": trade.pnl_pct,
                    "hold_seconds": trade.hold_seconds,
                    "entry_price": trade.entry_price,
                    "exit_price": trade.exit_price,
                    "invested_sol": trade.invested_sol,
                    "returned_sol": trade.returned_sol,
                    "opened_at": trade.opened_at,
                    "closed_at": trade.closed_at,
                }
                for trade in self.closed_trades
            ],
        }

    def format(self) -> str:
        """Человекочитаемый отчёт для консоли.

        Returns:
            Многострочный текст с ключевыми метриками.
        """
        reasons = ", ".join(
            f"{reason}={count}" for reason, count in sorted(self.exit_reasons.items())
        ) or "-"
        return "\n".join(
            [
                f"Пул:            {self.pool_address} ({self.base_mint})",
                f"Событий:        {self.events} за {self.duration_seconds:.1f}c",
                (
                    f"Сделок:         {self.trades} "
                    f"(прибыльных {self.wins}, убыточных {self.losses})"
                ),
                f"Win rate:       {self.win_rate_pct:.2f}%",
                f"PnL:            {self.pnl_sol:+.6f} SOL ({self.pnl_pct:+.2f}%)",
                (
                    f"Оборот:         вложено {self.invested_sol:.6f} SOL, "
                    f"возвращено {self.returned_sol:.6f} SOL"
                ),
                f"Средний холд:   {self.avg_hold_seconds:.2f}c",
                (
                    f"Просадка:       {self.max_drawdown_sol:.6f} SOL "
                    f"({self.max_drawdown_pct:.2f}% капитала)"
                ),
                (
                    f"Лучшая/худшая:  {self.best_trade_pct:+.2f}% / "
                    f"{self.worst_trade_pct:+.2f}%"
                ),
                f"Причины выхода: {reasons}",
            ]
        )


def _max_drawdown_sol(trades: tuple[ClosedTrade, ...]) -> float:
    """Считает максимальную просадку кумулятивного реализованного PnL.

    Args:
        trades: Закрытые сделки в порядке закрытия.

    Returns:
        Максимальная просадка от пика кумулятивного PnL, SOL (>= 0).
    """
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        equity += trade.pnl_sol
        peak = max(peak, equity)
        drawdown = peak - equity
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown


def _count_exit_reasons(trades: tuple[ClosedTrade, ...]) -> dict[str, int]:
    """Считает распределение причин выхода.

    Args:
        trades: Закрытые сделки.

    Returns:
        Словарь ``причина -> количество``.
    """
    counts: dict[str, int] = {}
    for trade in trades:
        counts[trade.exit_reason] = counts.get(trade.exit_reason, 0) + 1
    return counts


class Backtester:
    """Прогоняет исторический сценарий через живые фильтры и стратегию."""

    def __init__(
        self,
        *,
        strategy: BaseStrategy | None = None,
        executor: TraderExecutor | None = None,
        trade_amount_sol: float | None = None,
        max_concurrent_positions: int = 1,
        entry_window_seconds: float | None = None,
        exit_window_seconds: float | None = None,
        retry_cooldown_seconds: float = 0.0,
        initial_capital_sol: float | None = None,
    ) -> None:
        """Инициализирует бэктестер.

        Args:
            strategy: Торговая стратегия (по умолчанию моментум-скальпинг с
                настройками из окружения). Для сценарных тестов удобно передать
                стратегию с явными порогами.
            executor: Исполнитель (по умолчанию
                :class:`~core.dry_run.DryRunExecutor`).
            trade_amount_sol: Размер входа, SOL (по умолчанию из настроек).
            max_concurrent_positions: Максимум одновременно открытых позиций.
            entry_window_seconds: Окно анализа входа, c.
            exit_window_seconds: Окно анализа выхода, c.
            retry_cooldown_seconds: Пауза после неуспешного ордера, c. В бэктесте
                по умолчанию 0 — сбой ордера означает лишь отсутствие входа.
            initial_capital_sol: Капитал для расчёта просадки, SOL (по
                умолчанию ``trade_amount_sol * max_concurrent_positions``).
        """
        self._strategy = strategy
        self._executor: TraderExecutor = (
            executor if executor is not None else DryRunExecutor()
        )
        self._trade_amount_sol = (
            settings.trade_amount_sol
            if trade_amount_sol is None
            else trade_amount_sol
        )
        self._max_concurrent_positions = max(1, max_concurrent_positions)
        self._entry_window_seconds = (
            settings.entry_window_seconds
            if entry_window_seconds is None
            else entry_window_seconds
        )
        self._exit_window_seconds = (
            settings.exit_window_seconds
            if exit_window_seconds is None
            else exit_window_seconds
        )
        self._retry_cooldown_seconds = max(0.0, retry_cooldown_seconds)
        self._initial_capital_sol = (
            self._trade_amount_sol * self._max_concurrent_positions
            if initial_capital_sol is None
            else initial_capital_sol
        )

    # --------------------------------------------------------------- Свойства
    @property
    def trade_amount_sol(self) -> float:
        """Размер входного ордера, SOL."""
        return self._trade_amount_sol

    @property
    def initial_capital_sol(self) -> float:
        """Капитал, относительно которого считается просадка, SOL."""
        return self._initial_capital_sol

    # ------------------------------------------------------------- Публичное
    async def run(self, scenario: ReplayScenario) -> BacktestReport:
        """Прогоняет сценарий и возвращает отчёт.

        Args:
            scenario: Пул, события и данные безопасности.

        Returns:
            :class:`BacktestReport` с метриками прогона.
        """
        trader = self.build_trader(scenario.security)
        state = ReplayPoolState(scenario.pool)
        processed = 0

        for event in scenario.events:
            if event.pool_address != scenario.pool.pool_address:
                continue
            pool = state.apply(event)
            await trader.process_event(event, pool, creator=scenario.pool.creator)
            processed += 1

        trader.close_all(exit_reason=BACKTEST_END_REASON)
        return BacktestReport.from_portfolio(
            pool_address=scenario.pool.pool_address,
            base_mint=scenario.pool.base_mint,
            events=processed,
            duration_seconds=scenario.duration_seconds,
            stats=trader.portfolio.stats,
            closed_trades=trader.portfolio.closed_trades,
            initial_capital_sol=self._initial_capital_sol,
        )

    def build_trader(self, security: TokenSecurity) -> MomentumTrader:
        """Собирает движок решений с подменёнными внешними зависимостями.

        Args:
            security: Данные безопасности токена для всех пулов сценария.

        Returns:
            :class:`~core.trader.MomentumTrader`, настроенный на бэктест.
        """

        async def provider(_pool: PoolInfo) -> TokenSecurity | None:
            return security

        return MomentumTrader(
            executor=self._executor,
            portfolio=PortfolioManager(
                max_concurrent_positions=self._max_concurrent_positions
            ),
            strategy=self._strategy,
            security_provider=provider,
            trade_amount_sol=self._trade_amount_sol,
            entry_window_seconds=self._entry_window_seconds,
            exit_window_seconds=self._exit_window_seconds,
            retry_cooldown_seconds=self._retry_cooldown_seconds,
            capital_sol=self._initial_capital_sol,
        )
