"""Фильтр безопасности токен-контракта.

Проверяет долю сожжённых/залоченных LP-токенов против порога
:data:`~config.settings.settings.min_lp_burned_pct`. Проверки mint/freeze
authority убраны по решению от 2026-11-09: для bonding curve Pump.fun
authorities отозваны протокольно, а «нет данных безопасности» больше не
блокирует токен — его оценивают остальные фильтры входа.

Данные безопасности подготавливаются листенером (чтение mint-аккаунта и
LP-mint пула) и передаются в :class:`~filters.base_filter.FilterContext`.
Фильтр не выполняет сетевых вызовов и полностью детерминирован.
"""

from __future__ import annotations

from config.settings import settings
from filters.base_filter import BaseFilter, FilterContext
from models.trade_data import FilterResult


class SecurityGuard(BaseFilter):
    """Фильтр допуска по безопасности контракта (доля сожжённого LP)."""

    name = "security_guard"

    def __init__(
        self,
        *,
        min_lp_burned_pct: float | None = None,
    ) -> None:
        """Инициализирует фильтр.

        Args:
            min_lp_burned_pct: Минимальная доля сожжённого/залоченного LP, %.
        """
        self._min_lp_burned_pct = (
            settings.min_lp_burned_pct
            if min_lp_burned_pct is None
            else min_lp_burned_pct
        )

    @property
    def min_lp_burned_pct(self) -> float:
        """Минимальная доля сожжённого/залоченного LP, %."""
        return self._min_lp_burned_pct

    # ------------------------------------------------------------ Публичное
    def evaluate(self, context: FilterContext) -> FilterResult:
        """Проверяет безопасность контракта.

        Args:
            context: Контекст фильтрации с данными безопасности.

        Returns:
            Успех при пройденных проверках, иначе отказ с причиной.
        """
        security = context.security
        if security is None:
            return self._reject("отсутствуют данные безопасности токена")

        if security.lp_burned_pct < self._min_lp_burned_pct:
            return self._reject(
                "недостаточная доля сожжённого/залоченного LP",
                lp_burned_pct=round(security.lp_burned_pct, 4),
                min_lp_burned_pct=self._min_lp_burned_pct,
            )

        return self._ok(
            mint_authority=security.mint_authority,
            freeze_authority=security.freeze_authority,
            lp_burned_pct=round(security.lp_burned_pct, 4),
        )
