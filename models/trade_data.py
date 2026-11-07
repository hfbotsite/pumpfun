"""Pydantic-схемы данных бота.

Модуль описывает все структуры, циркулирующие между слоями приложения:
состояние пула, события сделок, результаты фильтров, метрики импульса,
торговый сигнал, результат исполнения и состояние позиции.

Все публичные ключи Solana представлены строками base58 — это упрощает
сериализацию, логирование и тестирование. Конвертация в ``solders.Pubkey``
выполняется на границе сборки транзакций (``core/tx_builder.py``).
"""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field

from config.settings import LAMPORTS_PER_SOL


class DEXType(StrEnum):
    """Поддерживаемые DEX/платформы."""

    PUMP_FUN = "pump_fun"
    RAYDIUM = "raydium"


class TradeSide(StrEnum):
    """Направление сделки в потоке."""

    BUY = "buy"
    SELL = "sell"


class SignalAction(StrEnum):
    """Действие, рекомендуемое стратегией."""

    ENTER = "enter"
    EXIT = "exit"
    HOLD = "hold"


class ExitReason(StrEnum):
    """Причина выхода из позиции."""

    TAKE_PROFIT_1 = "take_profit_1"
    TAKE_PROFIT_2 = "take_profit_2"
    TRAILING_STOP = "trailing_stop"
    HARD_STOP_LOSS = "hard_stop_loss"
    VOLUME_EXHAUSTION = "volume_exhaustion"
    MAX_HOLD_TIMEOUT = "max_hold_timeout"
    MANUAL = "manual"


# --------------------------------------------------------------------------- #
#  Пул
# --------------------------------------------------------------------------- #
class PoolInfo(BaseModel):
    """Снимок состояния AMM-пула.

    Резервы хранятся в «сырых» единицах (лампорты для quote, base units для base).
    Ликвидность в SOL и цена рассчитываются через свойства.
    """

    model_config = ConfigDict(frozen=False)

    dex: DEXType = Field(description="DEX/платформа пула.")
    pool_address: str = Field(description="Адрес пула (AMM account).")
    base_mint: str = Field(description="Mint мемкоина.")
    quote_mint: str = Field(description="Mint котируемого актива (обычно WSOL).")
    base_decimals: int = Field(default=6, ge=0, le=18, description="Десятичные base-токена.")
    quote_decimals: int = Field(default=9, ge=0, le=18, description="Десятичные quote (SOL = 9).")
    base_reserve: int = Field(default=0, ge=0, description="Резерв base-токенов (base units).")
    quote_reserve: int = Field(default=0, ge=0, description="Резерв quote (лампорты).")
    lp_mint: str | None = Field(default=None, description="Mint LP-токенов (если есть).")
    lp_supply: int | None = Field(default=None, ge=0, description="Общий объём LP (base units).")
    lp_burned: int | None = Field(default=None, ge=0, description="Сожжённый/выведенный LP (base units).")
    created_at: float = Field(default=0.0, description="Время создания пула, unix ts.")
    updated_at: float = Field(default=0.0, description="Время последнего обновления, unix ts.")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def pool_sol(self) -> float:
        """Текущая ликвидность пула в SOL."""
        return self.quote_reserve / LAMPORTS_PER_SOL

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_ui_amount(self) -> float:
        """Количество base-токенов в человекочитаемом виде."""
        return self.base_reserve / float(10**self.base_decimals)

    def price_sol_per_token(self) -> float:
        """Спот-цена одного base-токена в SOL.

        Returns:
            Цена в SOL за токен; ``0.0``, если резервы пусты.
        """
        base_ui = self.base_reserve / float(10**self.base_decimals)
        quote_ui = self.quote_reserve / float(10**self.quote_decimals)
        if base_ui <= 0:
            return 0.0
        return quote_ui / base_ui

    def lp_burned_pct(self) -> float:
        """Доля сожжённого/залоченного LP в процентах.

        Returns:
            Процент от 0 до 100; ``0.0``, если данных о LP нет.
        """
        if not self.lp_supply:
            return 0.0
        burned = self.lp_burned or 0
        return min(100.0, 100.0 * burned / self.lp_supply)


# --------------------------------------------------------------------------- #
#  Поток сделок
# --------------------------------------------------------------------------- #
class TradeEvent(BaseModel):
    """Событие одной сделки, извлечённое из потока транзакций.

    Служит «сырьём» для потоковых индикаторов. Поля ``sol_amount`` и
    ``token_amount`` должны быть в человекочитаемых единицах.
    """

    model_config = ConfigDict(frozen=True)

    signature: str = Field(description="Подпись транзакции.")
    pool_address: str = Field(description="Адрес пула.")
    base_mint: str = Field(description="Mint мемкоина.")
    dex: DEXType = Field(description="DEX/платформа.")
    side: TradeSide = Field(description="Направление сделки.")
    sol_amount: float = Field(ge=0, description="Объём в SOL.")
    token_amount: float = Field(ge=0, description="Объём в токенах.")
    wallet: str | None = Field(default=None, description="Кошелёк трейдера, если известен.")
    timestamp: float = Field(description="Время события, unix ts (секунды с дробной частью).")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def price_sol_per_token(self) -> float:
        """Цена сделки в SOL за токен."""
        if self.token_amount <= 0:
            return 0.0
        return self.sol_amount / self.token_amount


# --------------------------------------------------------------------------- #
#  Фильтры
# --------------------------------------------------------------------------- #
class FilterResult(BaseModel):
    """Результат проверки одного фильтра допуска."""

    passed: bool = Field(description="Прошёл ли актив проверку.")
    filter_name: str = Field(description="Имя фильтра.")
    reason: str = Field(default="", description="Причина отказа (или пояснение).")
    metrics: dict[str, Any] = Field(default_factory=dict, description="Диагностические метрики.")

    @classmethod
    def ok(cls, filter_name: str, **metrics: Any) -> FilterResult:
        """Создаёт успешный результат.

        Args:
            filter_name: Имя фильтра.
            **metrics: Диагностические метрики.

        Returns:
            Успешный :class:`FilterResult`.
        """
        return cls(passed=True, filter_name=filter_name, reason="", metrics=metrics)

    @classmethod
    def reject(cls, filter_name: str, reason: str, **metrics: Any) -> FilterResult:
        """Создаёт отрицательный результат.

        Args:
            filter_name: Имя фильтра.
            reason: Причина отказа.
            **metrics: Диагностические метрики.

        Returns:
            Отрицательный :class:`FilterResult`.
        """
        return cls(passed=False, filter_name=filter_name, reason=reason, metrics=metrics)


class TokenSecurity(BaseModel):
    """Данные безопасности токен-контракта (из данных mint-аккаунта)."""

    mint_authority: str | None = Field(default=None, description="Mint authority, если есть.")
    freeze_authority: str | None = Field(default=None, description="Freeze authority, если есть.")
    lp_burned_pct: float = Field(default=0.0, ge=0, le=100, description="Доля сожжённого/залоченного LP, %.")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def mint_authority_is_null(self) -> bool:
        """Снят ли mint authority."""
        return self.mint_authority is None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def freeze_authority_is_null(self) -> bool:
        """Снят ли freeze authority."""
        return self.freeze_authority is None


# --------------------------------------------------------------------------- #
#  Метрики импульса
# --------------------------------------------------------------------------- #
class MomentumMetrics(BaseModel):
    """Потоковые метрики импульса за скользящее окно."""

    window_seconds: float = Field(gt=0, description="Размер окна анализа, c.")
    buy_count: int = Field(default=0, ge=0, description="Число покупок в окне.")
    sell_count: int = Field(default=0, ge=0, description="Число продаж в окне.")
    buy_volume_sol: float = Field(default=0.0, ge=0, description="Объём покупок, SOL.")
    sell_volume_sol: float = Field(default=0.0, ge=0, description="Объём продаж, SOL.")
    largest_sell_sol: float = Field(default=0.0, ge=0, description="Крупнейшая продажа, SOL.")
    last_price: float = Field(default=0.0, ge=0, description="Последняя цена, SOL за токен.")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_count(self) -> int:
        """Общее число сделок в окне."""
        return self.buy_count + self.sell_count

    @computed_field  # type: ignore[prop-decorator]
    @property
    def buy_sell_ratio(self) -> float:
        """Доля покупок среди всех сделок (0..1)."""
        if self.total_count == 0:
            return 0.0
        return self.buy_count / self.total_count

    @computed_field  # type: ignore[prop-decorator]
    @property
    def volume_velocity_sol(self) -> float:
        """Суммарный объём покупок в окне (SOL) — «скорость объёма»."""
        return self.buy_volume_sol

    @computed_field  # type: ignore[prop-decorator]
    @property
    def net_flow_sol(self) -> float:
        """Нетто-поток: покупки минус продажи, SOL."""
        return self.buy_volume_sol - self.sell_volume_sol


# --------------------------------------------------------------------------- #
#  Сигнал
# --------------------------------------------------------------------------- #
#: Минимальная доля позиции, которую имеет смысл продавать.
MIN_SELL_FRACTION: float = 1e-6

#: Максимальная доля позиции к продаже (вся позиция).
MAX_SELL_FRACTION: float = 1.0


def clamp_sell_fraction(fraction: float) -> float:
    """Приводит долю продажи к допустимому диапазону ``(0, 1]``.

    Доля ``0`` или отрицательное значение не означают «продать всё»: они
    поднимаются до :data:`MIN_SELL_FRACTION` (пылевой остаток), а значения
    выше единицы срезаются до :data:`MAX_SELL_FRACTION`.

    Args:
        fraction: Запрошенная стратегией доля продажи.

    Returns:
        Доля в диапазоне ``(0, 1]``, пригодная для ``TradeSignal.sell_fraction``.
    """
    if math.isnan(fraction):
        return MAX_SELL_FRACTION
    return min(max(fraction, MIN_SELL_FRACTION), MAX_SELL_FRACTION)


class TradeSignal(BaseModel):
    """Торговый сигнал, порождённый стратегией."""

    action: SignalAction = Field(description="Рекомендуемое действие.")
    base_mint: str = Field(description="Mint мемкоина.")
    pool_address: str = Field(description="Адрес пула.")
    dex: DEXType = Field(description="DEX/платформа.")
    price_sol_per_token: float = Field(default=0.0, ge=0, description="Цена в момент сигнала.")
    reason: str = Field(default="", description="Обоснование сигнала.")
    exit_reason: ExitReason | None = Field(default=None, description="Причина выхода (для EXIT).")
    sell_fraction: float = Field(
        default=1.0, gt=0, le=1, description="Доля позиции к продаже (для EXIT)."
    )
    metrics: MomentumMetrics | None = Field(default=None, description="Метрики на момент сигнала.")
    timestamp: float = Field(description="Время генерации сигнала, unix ts.")


# --------------------------------------------------------------------------- #
#  Исполнение
# --------------------------------------------------------------------------- #
class OrderResult(BaseModel):
    """Результат отправки ордера."""

    success: bool = Field(description="Успешна ли отправка.")
    side: TradeSide = Field(description="Направление ордера.")
    base_mint: str = Field(description="Mint мемкоина.")
    pool_address: str = Field(description="Адрес пула.")
    signature: str | None = Field(default=None, description="Подпись транзакции.")
    bundle_id: str | None = Field(default=None, description="ID Jito-бандла.")
    sol_amount: float = Field(default=0.0, ge=0, description="Объём в SOL.")
    token_amount: float = Field(default=0.0, ge=0, description="Объём в токенах.")
    price_impact_pct: float = Field(default=0.0, ge=0, description="Оценённый Price Impact, %.")
    venue: str = Field(default="jito", description="Канал отправки (jito/jupiter).")
    error: str | None = Field(default=None, description="Текст ошибки при неуспехе.")
    submitted_at: float = Field(default=0.0, description="Время отправки, unix ts.")
    latency_ms: float = Field(default=0.0, ge=0, description="Задержка отправки, мс.")

    @classmethod
    def failed(
        cls,
        *,
        side: TradeSide,
        base_mint: str,
        pool_address: str,
        error: str,
        venue: str = "jito",
    ) -> OrderResult:
        """Создаёт результат неуспешного ордера.

        Args:
            side: Направление ордера.
            base_mint: Mint мемкоина.
            pool_address: Адрес пула.
            error: Текст ошибки.
            venue: Канал отправки.

        Returns:
            Неуспешный :class:`OrderResult`.
        """
        return cls(
            success=False,
            side=side,
            base_mint=base_mint,
            pool_address=pool_address,
            error=error,
            venue=venue,
        )


# --------------------------------------------------------------------------- #
#  Jito
# --------------------------------------------------------------------------- #
class JitoBundle(BaseModel):
    """Описание отправленного Jito-бандла."""

    bundle_id: str | None = Field(default=None, description="ID бандла (SHA-256 подписей).")
    transaction_signatures: list[str] = Field(
        default_factory=list, description="Подписи транзакций бандла."
    )
    tip_lamports: int = Field(default=0, ge=0, description="Чаевые в лампортах.")


class JitoBundleStatus(BaseModel):
    """Статус Jito-бандла."""

    bundle_id: str = Field(description="ID бандла.")
    status: str = Field(default="Pending", description="Pending/Landed/Failed/Invalid.")
    landed_slot: int | None = Field(default=None, description="Слот приземления.")
    error: str | None = Field(default=None, description="Ошибка (если retryable/недоступно).")


# --------------------------------------------------------------------------- #
#  Позиция
# --------------------------------------------------------------------------- #
class Position(BaseModel):
    """Состояние открытой позиции по одному токену."""

    base_mint: str = Field(description="Mint мемкоина.")
    pool_address: str = Field(description="Адрес пула.")
    dex: DEXType = Field(description="DEX/платформа.")
    entry_price: float = Field(gt=0, description="Цена входа, SOL за токен.")
    entry_sol: float = Field(gt=0, description="Вложенный объём, SOL.")
    token_amount: float = Field(gt=0, description="Текущее количество токенов.")
    initial_token_amount: float = Field(gt=0, description="Начальное количество токенов.")
    opened_at: float = Field(description="Время открытия, unix ts.")
    peak_price: float = Field(default=0.0, ge=0, description="Максимальная зарегистрированная цена.")
    tp1_taken: bool = Field(default=False, description="Взят ли первый тейк-профит.")
    realized_sol: float = Field(default=0.0, description="Уже реализованный объём, SOL.")
    signature: str | None = Field(default=None, description="Подпись входа.")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def remaining_fraction(self) -> float:
        """Доля позиции, оставшаяся к продаже (0..1)."""
        if self.initial_token_amount <= 0:
            return 0.0
        return self.token_amount / self.initial_token_amount

    def unrealized_pnl_pct(self, current_price: float) -> float:
        """Нереализованный PnL в процентах от цены входа.

        Args:
            current_price: Текущая цена, SOL за токен.

        Returns:
            Процент прибыли/убытка относительно входа.
        """
        if self.entry_price <= 0:
            return 0.0
        return (current_price - self.entry_price) / self.entry_price * 100.0

    def update_peak(self, current_price: float) -> None:
        """Обновляет зарегистрированный пик цены.

        Args:
            current_price: Текущая цена, SOL за токен.
        """
        self.peak_price = max(self.peak_price, current_price)

    def drawdown_from_peak_pct(self, current_price: float) -> float:
        """Откат текущей цены от пика в процентах.

        Args:
            current_price: Текущая цена, SOL за токен.

        Returns:
            Откат от пика в процентах (положительное число).
        """
        if self.peak_price <= 0:
            return 0.0
        return (self.peak_price - current_price) / self.peak_price * 100.0
