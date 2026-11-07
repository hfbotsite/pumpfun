"""Фильтр безопасности токен-контракта.

Фильтр отсекает токены с «ручными» правами эмитента:

* **Mint Authority** — если не ``None``, эмитент может допечатать токены и
  обвалить цену;
* **Freeze Authority** — если не ``None``, эмитент может заморозить наши
  token-аккаунты и заблокировать выход;
* **LP Burn / Lock** — доля сожжённых/залоченных LP-токенов должна превышать
  порог :data:`~config.settings.settings.min_lp_burned_pct`.

Данные безопасности подготавливаются листенером (чтение mint-аккаунта и
LP-mint пула) и передаются в :class:`~filters.base_filter.FilterContext`.
Фильтр не выполняет сетевых вызовов и полностью детерминирован.
"""

from __future__ import annotations

from config.settings import settings
from filters.base_filter import BaseFilter, FilterContext
from models.trade_data import FilterResult


class SecurityGuard(BaseFilter):
    """Фильтр допуска по безопасности контракта.

    Проверяет отсутствие mint/freeze authority и минимальную долю
    сожжённого/залоченного LP.
    """

    name = "security_guard"

    def __init__(
        self,
        *,
        require_mint_authority_null: bool | None = None,
        require_freeze_authority_null: bool | None = None,
        min_lp_burned_pct: float | None = None,
    ) -> None:
        """Инициализирует фильтр.

        Args:
            require_mint_authority_null: Требовать снятия mint authority.
            require_freeze_authority_null: Требовать снятия freeze authority.
            min_lp_burned_pct: Минимальная доля сожжённого/залоченного LP, %.
        """
        self._require_mint_null = (
            settings.require_mint_authority_null
            if require_mint_authority_null is None
            else require_mint_authority_null
        )
        self._require_freeze_null = (
            settings.require_freeze_authority_null
            if require_freeze_authority_null is None
            else require_freeze_authority_null
        )
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

        if self._require_mint_null and not security.mint_authority_is_null:
            return self._reject(
                "mint authority не снят",
                mint_authority=security.mint_authority,
            )

        if self._require_freeze_null and not security.freeze_authority_is_null:
            return self._reject(
                "freeze authority не снят",
                freeze_authority=security.freeze_authority,
            )

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
