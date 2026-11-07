"""Настройка логирования на базе ``loguru``.

Модуль настраивает два стока: цветной вывод в stderr и файл с ротацией,
а также перехватывает стандартный модуль :mod:`logging` (используется
``aiohttp``/``websockets``), чтобы все сообщения шли в едином формате.

Функция :func:`setup_logger` идемпотентна — повторный вызов перенастраивает
стоки без дублирования обработчиков.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING

from loguru import logger

from config.settings import settings

if TYPE_CHECKING:
    # В стабах ``loguru`` публичный тип логгера — ``loguru.Logger``;
    # рантайм-класс ``loguru._logger.Logger`` с ним не совпадает.
    from loguru import Logger

#: Формат для консоли (цветной, компактный).
_CONSOLE_FORMAT = (
    "<green>{time:HH:mm:ss.SSSSSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

#: Формат для файла (без цветов, с полной датой).
_FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSSSSS} | {level: <8} | "
    "{name}:{function}:{line} - {message}"
)

#: Подавляем слишком шумные сторонние логгеры.
_NOISY_LOGGERS: tuple[str, ...] = ("websockets", "asyncio", "aiohttp.access")

_configured: bool = False


class _InterceptHandler(logging.Handler):
    """Перенаправляет сообщения stdlib :mod:`logging` в ``loguru``."""

    def emit(self, record: logging.LogRecord) -> None:
        """Публикует запись stdlib в loguru с сохранением уровня.

        Args:
            record: Запись стандартного логгера.
        """
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logger(
    *,
    level: str | None = None,
    log_dir: str | Path | None = None,
    json_logs: bool = False,
) -> Logger:
    """Настраивает глобальный логгер.

    Args:
        level: Уровень логирования (по умолчанию из настроек).
        log_dir: Каталог для файла логов (по умолчанию из настроек).
        json_logs: Писать файл в формате JSON-строк.

    Returns:
        Сконфигурированный объект ``loguru.logger``.
    """
    global _configured
    resolved_level = (level or settings.log_level).upper()
    resolved_dir = Path(log_dir or settings.log_dir)
    resolved_dir.mkdir(parents=True, exist_ok=True)

    logger.remove()

    logger.add(
        sys.stderr,
        level=resolved_level,
        format=_CONSOLE_FORMAT,
        colorize=True,
        backtrace=False,
        diagnose=False,
        enqueue=False,
    )

    logger.add(
        resolved_dir / "bot_{time:YYYY-MM-DD}.log",
        level=resolved_level,
        format=_FILE_FORMAT,
        rotation="00:00",
        retention="14 days",
        compression="zip",
        encoding="utf-8",
        backtrace=True,
        diagnose=False,
        serialize=json_logs,
        enqueue=True,
    )

    logger.add(
        resolved_dir / "errors_{time:YYYY-MM-DD}.log",
        level="ERROR",
        format=_FILE_FORMAT,
        rotation="00:00",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        backtrace=True,
        diagnose=False,
        enqueue=True,
    )

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    for noisy in _NOISY_LOGGERS:
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True
    return logger


def get_logger() -> Logger:
    """Возвращает настроенный логгер, при необходимости конфигурируя его.

    Returns:
        Объект ``loguru.logger``.
    """
    if not _configured:
        setup_logger()
    return logger
