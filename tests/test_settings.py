"""Тесты конфигурации: переключатель включённых DEX."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from config.settings import Settings
from models.trade_data import DEXType


class TestEnabledDex:
    """ENABLED_DEX: парсинг, валидация, гейты."""

    def test_default_is_pump_fun_only(self) -> None:
        """По умолчанию включён только Pump.fun."""
        settings = Settings(enabled_dex="pump_fun")
        assert settings.enabled_dex_types == ["pump_fun"]
        assert settings.dex_enabled(DEXType.PUMP_FUN)
        assert not settings.dex_enabled(DEXType.RAYDIUM)

    def test_normalizes_case_and_spaces(self) -> None:
        """Регистр и пробелы нормализуются."""
        settings = Settings(enabled_dex=" PUMP_FUN , Raydium ")
        assert settings.enabled_dex_types == ["pump_fun", "raydium"]
        assert settings.dex_enabled("raydium")

    def test_empty_falls_back_to_pump_fun(self) -> None:
        """Пустое значение откатывается к pump_fun."""
        settings = Settings(enabled_dex="")
        assert settings.enabled_dex_types == ["pump_fun"]

    def test_unknown_dex_rejected(self) -> None:
        """Неизвестный DEX — ошибка валидации (fail-fast)."""
        with pytest.raises(ValidationError):
            Settings(enabled_dex="pump_fun,uniswap")

    def test_dex_enabled_accepts_enum_and_string(self) -> None:
        """Проверка принимает и DEXType, и строку."""
        settings = Settings(enabled_dex="raydium")
        assert settings.dex_enabled(DEXType.RAYDIUM)
        assert settings.dex_enabled("raydium")
        assert not settings.dex_enabled(DEXType.PUMP_FUN)
