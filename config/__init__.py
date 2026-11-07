"""Пакет конфигурации.

Экспортирует единый синглтон настроек :data:`settings`, загружаемый из окружения
и файла ``.env`` через ``pydantic-settings``.
"""

from config.settings import Settings, get_settings, settings

__all__ = ["Settings", "get_settings", "settings"]
