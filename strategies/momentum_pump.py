"""Моментум-стратегия входа/выхода (удержание до `MAX_HOLD_SECONDS`, 180 c).

Стратегия объединяет три уровня принятия решения:

**Вход** выполняется только при последовательном прохождении:

1. :class:`~filters.liquidity_guard.LiquidityGuard` — ликвидность пула и
   допустимый price impact ордера;
2. :class:`~filters.security_guard.SecurityGuard` — отсутствие mint/freeze
   authority и достаточная доля сожжённого LP;
3. **Momentum Trigger** — :attr:`buy_sell_ratio` не ниже порога и
   :attr:`volume_velocity` не ниже порога в окне входа.

**Выход** срабатывает по первому из параллельных триггеров:

* **Hard Stop-Loss** — падение цены на ``hard_stop_loss_pct`` от входа;
* **Каскадный Take-Profit** — фиксация ``tp1_size_pct`` позиции при
  ``tp1_pct`` и остатка при ``tp2_pct``;
* **Trailing Stop** — откат от зарегистрированного пика на
  ``trailing_stop_pct`` (активируется после выхода цены выше входа);
* **Volume Exhaustion** — крупная продажа (``large_sell_sol``) или падение
  Buy/Sell Ratio ниже ``exit_ratio_threshold`` в окне выхода;
* **Max Hold Timeout** — принудительный выход по истечении
  ``max_hold_seconds``.

Стратегия не выполняет сетевых вызовов и не отправляет транзакции — она лишь
порождает :class:`~models.trade_data.TradeSignal`, который исполняет
:mod:`core.executor`.

Контракт по метрикам:
    :attr:`~filters.base_filter.FilterContext.metrics` должен содержать метрики
    в окне, соответствующем принимаемому решению: ``entry_window_seconds`` —
    при :meth:`evaluate_entry`, ``exit_window_seconds`` — при
    :meth:`evaluate_exit`. Формирование метрик — ответственность вызывающего
    слоя (``core.listener``/``main.py``) и бэктестера.
"""

from __future__ import annotations

from config.settings import settings
from filters.base_filter import FilterChain, FilterContext
from filters.liquidity_guard import LiquidityGuard
from filters.security_guard import SecurityGuard
from models.trade_data import (
    ExitReason,
    Position,
    TradeSignal,
)
from strategies.base_strategy import BaseStrategy


class MomentumPumpStrategy(BaseStrategy):
    """Стратегия моментум-скальпинга на импульсе покупок.

    Инкапсулирует пороги входа (Buy/Sell Ratio, Volume Velocity),
    каскадный тейк-профит, трейлинг-стоп, жёсткий стоп-лосс и детектор
    затухания импульса.
    """

    name = "momentum_pump"

    def __init__(
        self,
        *,
        entry_filters: FilterChain | None = None,
        buy_sell_ratio_threshold: float | None = None,
        volume_velocity_threshold: float | None = None,
        min_entry_buy_count: int | None = None,
        max_entry_price_gain_pct: float | None = None,
        tp1_pct: float | None = None,
        tp2_pct: float | None = None,
        hot_volume_sol: float | None = None,
        tp1_hot_pct: float | None = None,
        tp2_hot_pct: float | None = None,
        max_hold_hot_seconds: float | None = None,
        trailing_activation_hot_pct: float | None = None,
        tp1_size_pct: float | None = None,
        trailing_stop_pct: float | None = None,
        trailing_activation_pct: float | None = None,
        hard_stop_loss_pct: float | None = None,
        exit_ratio_threshold: float | None = None,
        exhaustion_grace_seconds: float | None = None,
        large_sell_sol: float | None = None,
        max_hold_seconds: float | None = None,
    ) -> None:
        """Инициализирует стратегию.

        Args:
            entry_filters: Цепочка предторговых фильтров; по умолчанию
                ``LiquidityGuard`` + ``SecurityGuard``.
            buy_sell_ratio_threshold: Минимальная доля покупок для входа (0..1).
            volume_velocity_threshold: Минимальный объём покупок в окне, SOL.
            min_entry_buy_count: Минимальное число покупок в окне входа.
            max_entry_price_gain_pct: Максимальный рост цены за окно, %
                (защита от погони за хвостом волны).
            tp1_pct: Первый тейк-профит, %.
            tp2_pct: Второй тейк-профит, %.
            hot_volume_sol: Объём торгов по пулу, SOL, при котором позиция
                считается «горячей» (расширенные тейки/холд/трейлинг).
            tp1_hot_pct: Первый тейк-профит «горячих» пулов, %.
            tp2_hot_pct: Второй тейк-профит «горячих» пулов, %.
            max_hold_hot_seconds: Максимальное удержание «горячей» позиции, c.
            trailing_activation_hot_pct: Активация трейлинга на «горячих»
                пулах, % прибыли до пика.
            tp1_size_pct: Доля позиции, фиксируемая на TP1, %.
            trailing_stop_pct: Откат от пика для трейлинг-стопа, %.
            trailing_activation_pct: Прибыль от входа до пика, %, после
                которой трейлинг включается.
            hard_stop_loss_pct: Жёсткий стоп-лосс, %.
            exit_ratio_threshold: Порог падения Buy/Sell Ratio для выхода (0..1).
            exhaustion_grace_seconds: Пауза после входа, в течение которой
                выход по затуханию объёма не срабатывает, c.
            large_sell_sol: Порог «крупной продажи», SOL.
            max_hold_seconds: Максимальное время удержания позиции, c.
        """
        self._entry_filters = entry_filters if entry_filters is not None else FilterChain(
            [LiquidityGuard(), SecurityGuard()]
        )
        self._buy_sell_ratio_threshold = (
            settings.buy_sell_ratio_threshold
            if buy_sell_ratio_threshold is None
            else buy_sell_ratio_threshold
        )
        self._volume_velocity_threshold = (
            settings.volume_velocity_threshold
            if volume_velocity_threshold is None
            else volume_velocity_threshold
        )
        self._min_entry_buy_count = (
            settings.min_entry_buy_count
            if min_entry_buy_count is None
            else min_entry_buy_count
        )
        self._max_entry_price_gain_pct = (
            settings.max_entry_price_gain_pct
            if max_entry_price_gain_pct is None
            else max_entry_price_gain_pct
        )
        self._tp1_pct = settings.tp1_pct if tp1_pct is None else tp1_pct
        self._tp2_pct = settings.tp2_pct if tp2_pct is None else tp2_pct
        self._hot_volume_sol = (
            settings.hot_volume_sol if hot_volume_sol is None else hot_volume_sol
        )
        self._tp1_hot_pct = (
            settings.tp1_hot_pct if tp1_hot_pct is None else tp1_hot_pct
        )
        self._tp2_hot_pct = (
            settings.tp2_hot_pct if tp2_hot_pct is None else tp2_hot_pct
        )
        self._max_hold_hot_seconds = (
            settings.max_hold_hot_seconds
            if max_hold_hot_seconds is None
            else max_hold_hot_seconds
        )
        self._trailing_activation_hot_pct = (
            settings.trailing_activation_hot_pct
            if trailing_activation_hot_pct is None
            else trailing_activation_hot_pct
        )
        self._tp1_size_pct = (
            settings.tp1_size_pct if tp1_size_pct is None else tp1_size_pct
        )
        self._trailing_stop_pct = (
            settings.trailing_stop_pct
            if trailing_stop_pct is None
            else trailing_stop_pct
        )
        self._trailing_activation_pct = (
            settings.trailing_activation_pct
            if trailing_activation_pct is None
            else trailing_activation_pct
        )
        self._hard_stop_loss_pct = (
            settings.hard_stop_loss_pct
            if hard_stop_loss_pct is None
            else hard_stop_loss_pct
        )
        self._exit_ratio_threshold = (
            settings.exit_ratio_threshold
            if exit_ratio_threshold is None
            else exit_ratio_threshold
        )
        self._exhaustion_grace_seconds = (
            settings.exhaustion_grace_seconds
            if exhaustion_grace_seconds is None
            else exhaustion_grace_seconds
        )
        self._large_sell_sol = (
            settings.large_sell_sol if large_sell_sol is None else large_sell_sol
        )
        self._max_hold_seconds = (
            settings.max_hold_seconds if max_hold_seconds is None else max_hold_seconds
        )
        self._tp1_size_fraction = max(0.0, min(1.0, self._tp1_size_pct / 100.0)) or 1.0

    # ------------------------------------------------------------- Свойства
    @property
    def entry_filters(self) -> FilterChain:
        """Цепочка фильтров допуска входа."""
        return self._entry_filters

    @property
    def buy_sell_ratio_threshold(self) -> float:
        """Порог Buy/Sell Ratio для входа."""
        return self._buy_sell_ratio_threshold

    @property
    def volume_velocity_threshold(self) -> float:
        """Порог объёма покупок для входа, SOL."""
        return self._volume_velocity_threshold

    # ------------------------------------------------------------- Вход
    def evaluate_entry(self, context: FilterContext) -> TradeSignal:
        """Принимает решение о входе по импульсу покупок.

        Порядок проверок: цепочка фильтров → Buy/Sell Ratio → Volume Velocity.

        Args:
            context: Контекст с пулом, метриками (окно входа) и безопасностью.

        Returns:
            Сигнал ``ENTER`` при выполнении всех условий, иначе ``HOLD``.
        """
        metrics = context.metrics
        if metrics is None:
            return self._hold_signal(context, "отсутствуют метрики импульса")

        for result in self._entry_filters.run(context):
            if not result.passed:
                return self._hold_signal(
                    context, f"{result.filter_name}: {result.reason}"
                )

        # Защита от погони: если цена уже улетела за окно, покупаем хвост
        # волны — продолжения обычно нет (статистика: пик ≤ 1.005 у 90%).
        gain = metrics.window_price_gain_pct
        if gain > self._max_entry_price_gain_pct:
            return self._hold_signal(
                context,
                f"погоня за ценой: рост {gain:.1f}% за окно "
                f"> {self._max_entry_price_gain_pct:.0f}%",
            )

        if metrics.buy_count < self._min_entry_buy_count:
            return self._hold_signal(
                context,
                f"мало покупок в окне: {metrics.buy_count} "
                f"< {self._min_entry_buy_count} (вход за одним китом)",
            )

        ratio = metrics.buy_sell_ratio
        if ratio < self._buy_sell_ratio_threshold:
            return self._hold_signal(
                context,
                "низкий Buy/Sell Ratio "
                f"{ratio:.3f} < {self._buy_sell_ratio_threshold:.2f}",
            )

        velocity = metrics.volume_velocity_sol
        if velocity < self._volume_velocity_threshold:
            return self._hold_signal(
                context,
                "низкая Volume Velocity "
                f"{velocity:.3f} < {self._volume_velocity_threshold:.2f} SOL",
            )

        return self._enter_signal(
            context,
            "импульс покупок: "
            f"ratio={ratio:.3f}, velocity={velocity:.3f} SOL/{metrics.window_seconds:.1f}s",
        )

    # ------------------------------------------------------------- Выход
    def evaluate_exit(
        self, position: Position, context: FilterContext
    ) -> TradeSignal | None:
        """Принимает решение о выходе из открытой позиции.

        Триггеры проверяются в порядке приоритета: жёсткий стоп-лосс →
        каскадный тейк-профит → трейлинг-стоп → затухание импульса →
        таймаут удержания.

        Args:
            position: Текущая открытая позиция.
            context: Свежий контекст актива (цена, метрики окна выхода).

        Returns:
            Сигнал ``EXIT`` с причиной либо ``None``, если позицию удерживать.
        """
        current_price = self._current_price(position, context)

        hard_stop = self._check_hard_stop(position, current_price, context)
        if hard_stop is not None:
            return hard_stop

        take_profit = self._check_take_profit(position, current_price, context)
        if take_profit is not None:
            return take_profit

        trailing = self._check_trailing_stop(position, current_price, context)
        if trailing is not None:
            return trailing

        exhaustion = self._check_volume_exhaustion(position, context)
        if exhaustion is not None:
            return exhaustion

        return self._check_max_hold(position, context)

    # ------------------------------------------------- Реализация триггеров
    def _check_hard_stop(
        self, position: Position, current_price: float, context: FilterContext
    ) -> TradeSignal | None:
        """Проверяет жёсткий стоп-лосс.

        Args:
            position: Открытая позиция.
            current_price: Текущая цена, SOL за токен.
            context: Контекст актива.

        Returns:
            Сигнал выхода при пробое стопа, иначе ``None``.
        """
        pnl_pct = position.unrealized_pnl_pct(current_price)
        if pnl_pct <= -self._hard_stop_loss_pct:
            return self._exit_signal(
                position,
                context,
                ExitReason.HARD_STOP_LOSS,
                1.0,
                f"жёсткий стоп-лосс: PnL={pnl_pct:.2f}%",
            )
        return None

    def _check_take_profit(
        self, position: Position, current_price: float, context: FilterContext
    ) -> TradeSignal | None:
        """Проверяет каскадный тейк-профит (TP2, затем TP1).

        Args:
            position: Открытая позиция.
            current_price: Текущая цена, SOL за токен.
            context: Контекст актива.

        Returns:
            Сигнал выхода при достижении цели, иначе ``None``.
        """
        pnl_pct = position.unrealized_pnl_pct(current_price)
        tp1, tp2 = self._tp_targets(context)

        if position.tp1_taken:
            if pnl_pct >= tp2:
                return self._exit_signal(
                    position,
                    context,
                    ExitReason.TAKE_PROFIT_2,
                    1.0,
                    f"второй тейк-профит: PnL={pnl_pct:.2f}% >= {tp2:.1f}%",
                )
            return None

        if pnl_pct >= tp1:
            return self._exit_signal(
                position,
                context,
                ExitReason.TAKE_PROFIT_1,
                self._tp1_size_fraction,
                f"первый тейк-профит: PnL={pnl_pct:.2f}% >= {tp1:.1f}%",
            )
        return None

    def _is_hot_pool(self, pool: object) -> bool:
        """«Горячий» пул: объём торгов с отслеживания >= порога.

        Args:
            pool: Снимок пула (или ``None``).

        Returns:
            ``True``, если объём достаточен для расширенных целей.
        """
        volume = getattr(pool, "volume_sol", 0.0)
        return volume >= self._hot_volume_sol

    def _tp_targets(self, context: FilterContext) -> tuple[float, float]:
        """Целевые уровни TP1/TP2 с учётом «горячести» пула.

        Args:
            context: Контекст актива.

        Returns:
            Пара ``(tp1_pct, tp2_pct)``.
        """
        if self._is_hot_pool(context.pool):
            return self._tp1_hot_pct, self._tp2_hot_pct
        return self._tp1_pct, self._tp2_pct

    def _check_trailing_stop(
        self, position: Position, current_price: float, context: FilterContext
    ) -> TradeSignal | None:
        """Проверяет трейлинг-стоп от зарегистрированного пика.

        Трейлинг активируется только после того, как цена поднималась выше
        входа (``peak_price > entry_price``), чтобы не дублировать жёсткий
        стоп-лосс на «плоской» позиции.

        Args:
            position: Открытая позиция.
            current_price: Текущая цена, SOL за токен.
            context: Контекст актива.

        Returns:
            Сигнал выхода при откате от пика, иначе ``None``.
        """
        if position.peak_price <= position.entry_price:
            return None
        # Порог активации: трейлинг включается только с прибыли — иначе
        # он дублирует жёсткий стоп и выходит «из шума» с малым минусом.
        # На «горячих» пулах порог выше: прибыли даём течь.
        activation = (
            self._trailing_activation_hot_pct
            if self._is_hot_pool(context.pool)
            else self._trailing_activation_pct
        )
        peak_gain_pct = (position.peak_price / position.entry_price - 1.0) * 100.0
        if peak_gain_pct < activation:
            return None
        drawdown = position.drawdown_from_peak_pct(current_price)
        if drawdown >= self._trailing_stop_pct:
            return self._exit_signal(
                position,
                context,
                ExitReason.TRAILING_STOP,
                1.0,
                f"трейлинг-стоп: откат {drawdown:.2f}% от пика "
                f"(>= {self._trailing_stop_pct:.1f}%)",
            )
        return None

    def _check_volume_exhaustion(
        self, position: Position, context: FilterContext
    ) -> TradeSignal | None:
        """Проверяет затухание импульса и неблагоприятный поток.

        Триггеры: крупная продажа (``large_sell_sol``) либо падение
        Buy/Sell Ratio ниже ``exit_ratio_threshold`` в окне выхода.

        Args:
            position: Открытая позиция.
            context: Контекст актива.

        Returns:
            Сигнал выхода при затухании импульса, иначе ``None``.
        """
        metrics = context.metrics
        if metrics is None:
            return None

        # Grace-period: сразу после входа поток продаж — норма, а не затухание;
        # без паузы позиция умирает на первой же продаже (hold 0 c в статистике).
        age = context.timestamp - position.opened_at
        if age < self._exhaustion_grace_seconds:
            return None

        if metrics.largest_sell_sol >= self._large_sell_sol:
            return self._exit_signal(
                position,
                context,
                ExitReason.VOLUME_EXHAUSTION,
                1.0,
                "крупная продажа "
                f"{metrics.largest_sell_sol:.3f} SOL >= {self._large_sell_sol:.2f} SOL",
            )

        if (
            metrics.total_count > 0
            and metrics.buy_sell_ratio < self._exit_ratio_threshold
        ):
            return self._exit_signal(
                position,
                context,
                ExitReason.VOLUME_EXHAUSTION,
                1.0,
                "падение Buy/Sell Ratio "
                f"{metrics.buy_sell_ratio:.3f} < {self._exit_ratio_threshold:.2f}",
            )
        return None

    def _check_max_hold(
        self, position: Position, context: FilterContext
    ) -> TradeSignal | None:
        """Проверяет принудительный выход по таймауту удержания.

        Args:
            position: Открытая позиция.
            context: Контекст актива.

        Returns:
            Сигнал выхода при истечении времени, иначе ``None``.
        """
        held_seconds = context.timestamp - position.opened_at
        hot = self._is_hot_pool(context.pool)
        max_hold = self._max_hold_hot_seconds if hot else self._max_hold_seconds
        if held_seconds >= max_hold:
            return self._exit_signal(
                position,
                context,
                ExitReason.MAX_HOLD_TIMEOUT,
                1.0,
                f"таймаут удержания: {held_seconds:.2f}c >= {max_hold:.1f}c",
            )
        return None

    # ----------------------------------------------------------- Утилиты
    @staticmethod
    def _current_price(position: Position, context: FilterContext) -> float:
        """Определяет актуальную цену токена для оценки позиции.

        Предпочитается последняя цена сделки из метрик окна выхода; при её
        отсутствии используется спот-цена пула.

        Args:
            position: Открытая позиция.
            context: Контекст актива.

        Returns:
            Текущая цена в SOL за токен (не ниже спот-цены пула при нуле).
        """
        metrics = context.metrics
        price = metrics.last_price if metrics is not None else 0.0
        if price <= 0:
            price = context.pool.price_sol_per_token()
        if price <= 0:
            price = position.entry_price
        return price
