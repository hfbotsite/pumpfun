"""Тесты стратегии: триггеры входа и все пять триггеров выхода."""

from __future__ import annotations

from dataclasses import replace

import pytest

from config.settings import settings
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


def make_entry_context(
    metrics: MomentumMetrics | None = None,
    dip_pct: float | None = None,
) -> FilterContext:
    """Контекст входа: хороший пул, чистый контракт, горячие метрики."""
    return FilterContext(
        pool=make_pool(),
        trade_amount_sol=0.5,
        security=clean_security(),
        metrics=metrics if metrics is not None else hot_metrics(),
        timestamp=1000.0,
        dip_pct=dip_pct,
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


class TestDipEntry:
    """Вход на откате (dip): порог = доля стопа уровня пула."""

    def test_dip_enters(self) -> None:
        """Откат глубже порога — вход."""
        strategy = MomentumPumpStrategy(hard_stop_cold_pct=8.0)
        signal = strategy.evaluate_entry(make_entry_context(dip_pct=3.0))
        assert signal.action.value == "enter"

    def test_dip_below_threshold_holds(self) -> None:
        """Откат мельче порога — удержание."""
        strategy = MomentumPumpStrategy(hard_stop_cold_pct=8.0)
        signal = strategy.evaluate_entry(make_entry_context(dip_pct=1.0))
        assert signal.action.value == "hold"
        assert "нет отката" in signal.reason

    def test_no_candle_data_holds(self) -> None:
        """Свечи ещё не набраны — удержание."""
        strategy = MomentumPumpStrategy()
        signal = strategy.evaluate_entry(make_entry_context(dip_pct=None))
        assert signal.action.value == "hold"
        assert "мало данных свечей" in signal.reason

    def test_tier_thresholds(self) -> None:
        """Порог масштабируется стопом уровня пула (×0.33)."""
        strategy = MomentumPumpStrategy(
            hard_stop_cold_pct=8.0,
            hard_stop_loss_pct=40.0,
            hard_stop_hot_pct=50.0,
        )
        # cold (объём 0): порог 8×0.33=2.64
        assert (
            strategy.evaluate_entry(make_entry_context(dip_pct=3.0)).action.value
            == "enter"
        )
        assert (
            strategy.evaluate_entry(make_entry_context(dip_pct=2.0)).action.value
            == "hold"
        )
        # hot (объём 500): порог 50×0.33=16.5
        hot_ctx = replace(
            make_entry_context(dip_pct=17.0),
            pool=make_pool(volume_sol=500.0),
            dip_pct=17.0,
        )
        assert strategy.evaluate_entry(hot_ctx).action.value == "enter"
        hot_low = replace(hot_ctx, dip_pct=10.0)
        assert strategy.evaluate_entry(hot_low).action.value == "hold"


class TestExitHardStop:
    """Жёсткий стоп-лосс (порог задан явно)."""

    def test_price_below_stop_exits_full(self) -> None:
        """Падение на 30% (порог задан) выводит всю позицию."""
        strategy = MomentumPumpStrategy(hard_stop_loss_pct=30.0)
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 0.70)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "hard_stop_loss"
        assert signal.sell_fraction == 1.0

    def test_small_drawdown_holds(self) -> None:
        """Падение на 5% не пробивает стоп (порог задан)."""
        strategy = MomentumPumpStrategy(hard_stop_loss_pct=30.0)
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 0.95)
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitTakeProfit:
    """Каскадный тейк-профит (холодные уровни заданы явно)."""

    def test_tp1_sells_half(self) -> None:
        """+35% фиксирует 50% позиции как TP1."""
        strategy = MomentumPumpStrategy(tp1_cold_pct=10.0, tp2_cold_pct=15.0)
        position = make_position()
        metrics = hot_metrics(last_price=position.entry_price * 1.35)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "take_profit_1"
        # Доля фиксации берётся из настроек (TP1_SIZE_PCT).
        assert signal.sell_fraction == pytest.approx(settings.tp1_size_pct / 100.0)

    def test_tp2_after_tp1_sells_rest(self) -> None:
        """После TP1 достижение +55% закрывает остаток как TP2."""
        strategy = MomentumPumpStrategy(tp2_cold_pct=15.0)
        position = make_position(tp1_taken=True)
        metrics = hot_metrics(last_price=position.entry_price * 1.55)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "take_profit_2"
        assert signal.sell_fraction == 1.0

    def test_tp1_level_after_tp1_taken_waits_tp2(self) -> None:
        """После TP1 уровень +12% не является сигналом (холодный TP2 = 15%)."""
        strategy = MomentumPumpStrategy(tp2_cold_pct=15.0)
        position = make_position(tp1_taken=True)
        metrics = hot_metrics(last_price=position.entry_price * 1.12)
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitTrailing:
    """Трейлинг-стоп от пика (10%)."""

    def test_drawdown_from_peak_exits(self) -> None:
        """Откат 20%+ от пика выше входа — выход (активация с +15%)."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=1.3)
        metrics = hot_metrics(last_price=1.03)  # откат 20.77%, PnL +3%
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "trailing_stop"

    def test_no_peak_above_entry_no_trailing(self) -> None:
        """Пик не выше входа — трейлинг не активен (это работа hard SL)."""
        strategy = MomentumPumpStrategy(
            hard_stop_loss_pct=30.0,
            tp1_cold_pct=10.0,
            tp2_cold_pct=15.0,
        )
        position = make_position(entry_price=1.0, peak_price=0.9)
        metrics = hot_metrics(last_price=0.70)  # -30% → сработает hard SL
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "hard_stop_loss"

    def test_mild_pullback_holds(self) -> None:
        """Откат 6% от пика при PnL +8% — удерживаем (холодный уровень)."""
        strategy = MomentumPumpStrategy(
            tp1_cold_pct=10.0,
            trailing_stop_cold_pct=8.0,
            trailing_activation_cold_pct=5.0,
        )
        position = make_position(entry_price=1.0, peak_price=1.15)
        metrics = hot_metrics(last_price=1.08)  # откат 6.1%, PnL +8% < TP1 10%
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None


class TestExitExhaustion:
    """Затухание импульса: крупная продажа, падение ratio."""

    def test_large_sell_triggers_exit(self) -> None:
        """Продажа >= 3 SOL в окне выхода после grace-паузы закрывает позицию."""
        strategy = MomentumPumpStrategy(exhaustion_grace_seconds=0.0)
        position = make_position()
        metrics = hot_metrics(
            last_price=position.entry_price, largest_sell_sol=3.2
        )
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal.exit_reason.value == "volume_exhaustion"
        assert "крупная продажа" in signal.reason

    def test_ratio_drop_triggers_exit(self) -> None:
        """Ratio ниже 0.25 после grace-паузы — выход по затуханию."""
        strategy = MomentumPumpStrategy(exhaustion_grace_seconds=0.0)
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
        """Пороги берутся из настроек окружения (не захардкожены)."""
        strategy = MomentumPumpStrategy()
        assert strategy.buy_sell_ratio_threshold == settings.buy_sell_ratio_threshold
        assert strategy.volume_velocity_threshold == settings.volume_velocity_threshold

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


class TestExhaustionGrace:
    """Grace-period: затухание не срабатывает сразу после входа."""

    def test_exhaustion_ignored_during_grace(self) -> None:
        """Продажа 5 SOL через 1 c после входа — удерживаем (grace 10 c)."""
        strategy = MomentumPumpStrategy()  # grace 10 c
        position = make_position()  # opened_at=1000
        metrics = hot_metrics(
            last_price=position.entry_price, largest_sell_sol=5.0
        )
        # make_exit_context: timestamp=1001 → возраст позиции 1 c < grace.
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None

    def test_exhaustion_works_after_grace(self) -> None:
        """Тот же поток продаж через 11 c — выход по затуханию."""
        strategy = MomentumPumpStrategy()
        position = make_position()
        metrics = hot_metrics(
            last_price=position.entry_price, largest_sell_sol=5.0
        )
        context = make_exit_context(metrics, timestamp=1011.0)
        signal = strategy.evaluate_exit(position, context)
        assert signal is not None
        assert signal.exit_reason.value == "volume_exhaustion"


class TestTrailingActivation:
    """Трейлинг включается только с прибыли >= 15% от входа."""

    def test_trailing_inactive_below_activation(self) -> None:
        """Пик +4% от входа (< холодной активации 5%): трейлинг не активен."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=1.04)
        metrics = hot_metrics(last_price=0.90)  # откат 13.5% от пика, PnL -10%
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None

    def test_trailing_active_above_activation(self) -> None:
        """Пик +20% от входа, откат 20% — выход по трейлингу."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0, peak_price=1.20)
        metrics = hot_metrics(last_price=0.95)  # откат 20.8% от пика, PnL -5%
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "trailing_stop"


class TestHotPool:
    """«Горячий» режим: объём >= порога расширяет тейки/холд/трейлинг."""

    def hot_context(self, metrics: MomentumMetrics, timestamp: float = 1001.0):
        """Контекст выхода с «горячим» пулом (объём 500 SOL).

        Args:
            metrics: Метрики окна выхода.
            timestamp: Unix ts события.

        Returns:
            :class:`FilterContext`.
        """
        return FilterContext(
            pool=make_pool(volume_sol=500.0),
            trade_amount_sol=0.5,
            security=None,
            metrics=metrics,
            timestamp=timestamp,
        )

    def test_hot_tp1_raises_target(self) -> None:
        """Горячий пул: TP1 = 20%; тёплый = 15%; холодный = 10% (явные уровни)."""
        strategy = MomentumPumpStrategy(
            tp1_cold_pct=10.0,
            tp2_cold_pct=15.0,
            tp1_warm_pct=15.0,
            tp2_warm_pct=30.0,
            tp1_hot_pct=20.0,
            tp2_hot_pct=40.0,
        )
        position = make_position(entry_price=1.0)
        metrics = hot_metrics(last_price=1.16)  # PnL +16%
        # Холодный пул (объём 0): TP1 10% — выход.
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is not None
        # Тёплый (объём 200): TP1 15% — выход.
        warm = FilterContext(
            pool=make_pool(volume_sol=200.0), trade_amount_sol=0.5,
            security=None, metrics=metrics, timestamp=1001.0,
        )
        assert strategy.evaluate_exit(position, warm) is not None
        # Горячий (объём 500): цель 20% — удерживаем.
        assert strategy.evaluate_exit(position, self.hot_context(metrics)) is None

    def test_hot_tp2_at_70(self) -> None:
        """Горячий пул: после TP1 второй тейк на 40%."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0)
        position.tp1_taken = True
        metrics = hot_metrics(last_price=1.45)
        signal = strategy.evaluate_exit(position, self.hot_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "take_profit_2"

    def test_hot_trailing_activation_higher(self) -> None:
        """Горячий пул: пик +20% (< горячей активации 25) — трейлинг молчит."""
        strategy = MomentumPumpStrategy(
            trailing_activation_hot_pct=25.0,
            trailing_stop_pct=30.0,
            tp1_hot_pct=99.0,
            tp1_pct=99.0,
            tp1_cold_pct=99.0,
        )
        position = make_position(entry_price=1.0, peak_price=1.20)
        metrics = hot_metrics(last_price=0.94)  # откат 21.7% от пика
        # Обычный пул: трейлинг сработал бы.
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is not None
        # Горячий: активация 25% — удерживаем (PnL -6% < hard SL -30%).
        assert strategy.evaluate_exit(position, self.hot_context(metrics)) is None

    def test_hot_max_hold_longer(self) -> None:
        """Горячий пул: холд 200 c — обычный таймаут (180) вышел бы, горячий (600) нет."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0)
        metrics = hot_metrics(last_price=1.0)
        late = make_exit_context(metrics, timestamp=1200.0)  # возраст 200 c
        signal = strategy.evaluate_exit(position, late)
        assert signal is not None
        assert signal.exit_reason.value == "max_hold_timeout"
        hot_late = self.hot_context(metrics, timestamp=1200.0)
        assert strategy.evaluate_exit(position, hot_late) is None


class TestTieredHardStop:
    """Стоп позиции перекрывает базовый порог стратегии."""

    def test_cold_stop_exits_earlier(self) -> None:
        """Стоп 22% (холодный): PnL -25% — выход раньше базовых 30%."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0)
        position.stop_loss_pct = 22.0
        metrics = hot_metrics(last_price=0.75)
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "hard_stop_loss"

    def test_hot_stop_holds_longer(self) -> None:
        """Стоп 40% (горячий): PnL -30% (базовый уровень) — удерживаем."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0)
        position.stop_loss_pct = 40.0
        metrics = hot_metrics(last_price=0.70)
        assert strategy.evaluate_exit(position, make_exit_context(metrics)) is None

    def test_zero_stop_falls_back_to_base(self) -> None:
        """stop_loss_pct=0 (бэктест/реплей) — базовый стоп из настроек."""
        strategy = MomentumPumpStrategy()
        position = make_position(entry_price=1.0)
        metrics = hot_metrics(
            last_price=position.entry_price
            * (1.0 - settings.hard_stop_loss_pct / 100.0 - 0.01)
        )
        signal = strategy.evaluate_exit(position, make_exit_context(metrics))
        assert signal is not None
        assert signal.exit_reason.value == "hard_stop_loss"
