"""Тесты стратегии: триггеры входа и все пять триггеров выхода."""

from __future__ import annotations

from filters.base_filter import FilterChain, FilterContext
from filters.liquidity_guard import LiquidityGuard
from models.trade_data import MomentumMetrics, TokenSecurity
from strategies.momentum_pump import MomentumPumpStrategy
from tests.conftest import BASE_MINT, POOL_ADDRESS, make_pool, make_position


def clean_security() -> TokenSecurity:
    """Безопасный контракт для входа."""
    return TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0)


def hot_metrics(**overrides) -> MomentumMetrics:
    """Метрики «горячего» импульса: ratio 0.8, velocity 2.5 SOL."""
    fields = {
        "window_seconds": 5.0,
        "buy_count": 8,
        "sell_count": 2,
        "buy_volume_sol": 2.5,
        "sell_volume_sol": 0.5,
        "largest_sell_sol": 0.4,
        "last_price": 5e-9,
    }
    fields.update(overrides)
    return MomentumMetrics(**fields)


def make_entry_context(metrics: MomentumMetrics | None = None) -> FilterContext:
    """Контекст входа: хороший пул, чистый контракт, горячие метрики."""
    return FilterContext(
        pool=make_pool(),
        trade_amount_sol=0.5,
        security=clean_security(),
        metrics=metrics if metrics is not None else hot_metrics(),
        timestamp=1000.0,
    )


def make_exit_context(
    metrics: MomentumMetrics,
    timestamp: float = 1001.0,
) -> FilterContext:
    """Контекст выхода (security не проверяется)."""
    return FilterContext(
        pool=make_pool(),
        trade_amount_sol=0.5,
        security=None,
        metrics=metrics,
        timestamp=timestamp,
    )


class TestEntry:
    """Вход по импульсу: фильтры, ratio, velocity."""

    def test_hot_momentum_enters(self) -> None:
        """Ratio 0.8 + velocity 2.5 SOL дают ENTER."""
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(make_entry_context())
        assert signal.action.value == "enter"
        assert "ratio" in signal.reason

    def test_low_ratio_holds(self) -> None:
        """Ratio ниже 0.70 — HOLD даже при горячем объёме."""
        metrics = hot_metrics(buy_count=4, sell_count=6, buy_volume_sol=2.5)
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(make_entry_context(metrics))
        assert signal.action.value == "hold"
        assert "ratio" in signal.reason.lower()

    def test_low_velocity_holds(self) -> None:
        """Velocity ниже 1.5 SOL — HOLD при хорошем ratio."""
        metrics = hot_metrics(buy_volume_sol=0.5)
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(make_entry_context(metrics))
        assert signal.action.value == "hold"
        assert "velocity" in signal.reason.lower()

    def test_no_metrics_holds(self) -> None:
        """Без метрик решения нет."""
        strategy = MomentumPumpStrategy()
        context = FilterContext(
            pool=make_pool(),
            trade_amount_sol=0.5,
            security=clean_security(),
            metrics=None,
            timestamp=1000.0,
        )
        signal = strategy.evaluate_entry(context)
        assert signal.action.value == "hold"
        assert "метрики" in signal.reason.lower()

    def test_failed_filter_holds(self) -> None:
        """Отказ цепочки фильтров блокирует вход даже на горячем импульсе."""
        poor_pool = make_pool(quote_reserve=5_000_000_000)
        context = FilterContext(
            pool=poor_pool,
            trade_amount_sol=0.5,
            security=clean_security(),
            metrics=hot_metrics(),
            timestamp=1000.0,
        )
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(context)
        assert signal.action.value == "hold"
        assert "liquidity_guard" in signal.reason

    def test_explicit_thresholds_override(self) -> None:
        """Явные пороги стратегии перекрывают настройки окружения."""
        strategy = MomentumPumpStrategy(
            buy_sell_ratio_threshold=0.95, volume_velocity_threshold=10.0
        )
        signal = strategy.evaluate_entry(make_entry_context())
        assert signal.action.value == "hold"


class TestExitHardStop:
    """Жёсткий стоп-лосс (-30% по умолчанию)."""

    def test_price_below_stop_exits_full(self) -> None:
        """Падение на 30% (порог по умолчанию) выводит всю позицию."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 0.70)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "hard_stop_loss"
        assert signal.sell_fraction == 1.0

    def test_small_drawdown_holds(self) -> None:
        """Падение на 5% не пробивает стоп."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 0.95)
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitTakeProfit:
    """Каскадный тейк-профит."""

    def test_tp1_sells_half(self) -> None:
        """+35% фиксирует 50% позиции как TP1."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 1.35)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "take_profit_1"
        assert signal.sell_fraction == 0.5

    def test_tp2_after_tp1_sells_rest(self) -> None:
        """После TP1 достижение +55% закрывает остаток как TP2."""
        strategy = MomentumPumpStrategy()
        position = make_position(tp1_taken=True)
        metrics = hot_metrics(last_price=position.entry_price * 1.55)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "take_profit_2"
        assert signal.sell_fraction == 1.0

    def test_tp1_level_after_tp1_taken_waits_tp2(self) -> None:
        """После TP1 уровень +35% не является сигналом (ждём +50%)."""
        strategy = MomentumPumpStrategy()
        position = make_position(tp1_taken=True)
        metrics = hot_metrics(last_price=position.entry_price * 1.35)
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitTrailing:
    """Трейлинг-стоп от пика (10%)."""

    def test_drawdown_from_peak_exits(self) -> None:
        """Откат 10%+ от пика выше входа — выход."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=1.3)
        metrics = hot_metrics(last_price=1.16)  # откат 10.77%, PnL +16%
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "trailing_stop"

    def test_no_peak_above_entry_no_trailing(self) -> None:
        """Пик не выше входа — трейлинг не активен (это работа hard SL)."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=0.9)
        metrics = hot_metrics(last_price=0.70)  # -30% → сработает hard SL
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "hard_stop_loss"

    def test_mild_pullback_holds(self) -> None:
        """Откат ~4% от пика при PnL +10% — удерживаем."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=1.15)
        metrics = hot_metrics(last_price=1.10)  # откат 4.35%, PnL +10%
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitExhaustion:
    """Затухание импульса: крупная продажа, падение ratio."""

    def test_large_sell_triggers_exit(self) -> None:
        """Продажа >= 1 SOL в окне выхода закрывает позицию."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(
            last_price=position.entry_price, largest_sell_sol=1.2
        )
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "volume_exhaustion"
        assert "крупная продажа" in signal.reason

    def test_ratio_drop_triggers_exit(self) -> None:
        """Ratio ниже 0.40 — выход по затуханию."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(
            last_price=position.entry_price, buy_count=1, sell_count=9
        )
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "volume_exhaustion"
        assert "ratio" in signal.reason.lower()

    def test_healthy_window_holds(self) -> None:
        """Спокойное окно (нет крупных продаж, ratio высокий) — удерживаем."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(
            last_price=position.entry_price, buy_count=5, sell_count=2
        )
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitMaxHold:
    """Таймаут удержания (30 c)."""

    def test_timeout_exits(self) -> None:
        """Позиция старше max_hold_seconds закрывается принудительно."""
        strategy = MomentumPumpStrategy(max_hold_seconds=30.0)
        position = make_position(opened_at=1000.0)
        metrics = hot_metrics(last_price=position.entry_price)
        context = make_exit_context(metrics, timestamp=1031.0)
        signal = strategy.evaluate_exit(position, context)
        assert signal.exit_reason.value == "max_hold_timeout"

    def test_within_timeout_holds(self) -> None:
        """До таймаута при спокойном окне — удерживаем."""
        strategy = MomentumPumpStrategy(max_hold_seconds=30.0)
        position = make_position(opened_at=1000.0)
        metrics = hot_metrics(last_price=position.entry_price)
        context = make_exit_context(metrics, timestamp=1020.0)
        assert strategy.evaluate_exit(position, context) is None


class TestDefaults:
    """Пороги по умолчанию соответствуют ТЗ."""

    def test_default_thresholds(self) -> None:
        """Проверка значений по умолчанию из настроек окружения."""
        strategy = MomentumPumpStrategy()
        assert strategy.buy_sell_ratio_threshold == 0.70
        assert strategy.volume_velocity_threshold == 1.5

    def test_custom_filter_chain_used(self) -> None:
        """Явная цепочка фильтров перекрывает дефолтную."""
        chain = FilterChain([LiquidityGuard()])
        strategy = MomentumPumpStrategy(entry_filters=chain)
        assert strategy.entry_filters is chain
        assert [type(f) for f in strategy.entry_filters.filters] == [LiquidityGuard]


def test_position_factories_agree() -> None:
    """Фабрика позиции согласована с константами тестов."""
    position = make_position()
    assert position.base_mint == BASE_MINT
    assert position.pool_address == POOL_ADDRESS
