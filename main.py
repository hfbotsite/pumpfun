"""Точка входа скальпинг-бота: CLI, запуск пайплайна, режим ``--status``.

Сборка командной строки:

* ``--dry-run`` — бумажное исполнение (поток событий настоящий, ордера нет);
* ``--watchlist MINT [MINT ...]`` — пулы, отслеживаемые с момента старта;
* ``--positions N`` — лимит одновременных позиций (по умолчанию из настроек);
* ``--capital SOL`` — лимит свободного капитала (по умолчанию из настроек);
* ``--db PATH`` — путь к SQLite-БД позиций;
* ``--close-on-stop`` — принудительно продавать позиции при завершении;
* ``--log-level LEVEL`` — уровень логирования;
* ``--status`` — показать открытые позиции и сводку по закрытым сделкам
  из БД и выйти (без сети и подписок).

Запуск без аргументов поднимает live-пайплайн и обрабатывает поток до
Ctrl+C / SIGTERM: подписки снимаются, ресурсы закрываются, в лог пишется
сводка. Открытые позиции по умолчанию остаются открытыми и переживают
рестарт благодаря SQLite-хранилищу.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time
from pathlib import Path

from config.settings import settings
from core.persistence import PortfolioStore
from core.pipeline import LivePipeline
from models.trade_data import Position
from utils.logger import get_logger, setup_logger

if sys.platform != "win32":
    # uvloop ускоряет event loop на Linux VPS (продакшен-сценарий проекта).
    try:
        import uvloop

        uvloop.install()
    except ImportError:  # pragma: no cover - опциональная зависимость
        pass

logger = get_logger()


def _build_parser() -> argparse.ArgumentParser:
    """Собирает парсер аргументов командной строки.

    Returns:
        :class:`argparse.ArgumentParser` с описанием на русском.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "Solana Pump Momentum & Liquidity Scalping Bot "
            "(Pump.fun / Raydium, исполнение через Jito)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="бумажное исполнение: без отправки транзакций",
    )
    parser.add_argument(
        "--watchlist",
        nargs="+",
        default=(),
        metavar="MINT",
        help="mint-адреса пулов для отслеживания с момента старта",
    )
    parser.add_argument(
        "--positions",
        type=int,
        default=None,
        metavar="N",
        help="лимит одновременных позиций (по умолчанию MAX_CONCURRENT_POSITIONS)",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=None,
        metavar="SOL",
        help="лимит свободного капитала в SOL (по умолчанию CAPITAL_SOL)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        metavar="PATH",
        help="путь к SQLite-БД позиций (по умолчанию PORTFOLIO_DB_PATH)",
    )
    parser.add_argument(
        "--close-on-stop",
        action="store_true",
        help="принудительно продавать позиции при завершении работы",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        metavar="LEVEL",
        help="уровень логирования: DEBUG/INFO/WARNING/ERROR",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="показать открытые позиции и сводку сделок из БД и выйти",
    )
    return parser


def _resolve_db_path(args: argparse.Namespace) -> str:
    """Определяет путь к БД позиций с учётом режима dry-run.

    Args:
        args: Разобранные аргументы командной строки.

    Returns:
        Путь к файлу SQLite-БД.
    """
    explicit = args.db
    if explicit:
        return str(explicit)
    if args.dry_run:
        return str(settings.portfolio_dry_db_path)
    return str(settings.portfolio_db_path)


def _format_position_row(position: Position, now_ts: float) -> str:
    """Форматирует одну строку таблицы открытых позиций.

    Args:
        position: Открытая позиция из БД.
        now_ts: Текущее время, unix ts (для возраста позиции).

    Returns:
        Готовая строка для консоли.
    """
    base_mint = position.base_mint
    short_mint = base_mint[:8] if len(base_mint) > 8 else base_mint
    hold_seconds = max(0.0, now_ts - position.opened_at)
    return (
        f"  {short_mint:<8} {position.dex.value:<8} "
        f"in={position.entry_sol:.4f} SOL  "
        f"tokens={position.token_amount:.4f}  "
        f"entry={position.entry_price:.9f}  "
        f"peak={position.peak_price:.9f}  hold={hold_seconds:.0f}c  "
        f"TP1={'да' if position.tp1_taken else 'нет'}"
    )


def _show_status(db_path: str) -> int:
    """Печатает открытые позиции и сводку закрытых сделок из БД.

    Args:
        db_path: Путь к SQLite-БД позиций.

    Returns:
        Код выхода (0 — БД прочитана, 1 — БД отсутствует).
    """
    if not Path(db_path).exists():
        print(f"БД позиций не найдена: {db_path}")
        return 1
    store = PortfolioStore(db_path)
    try:
        positions = store.load_open_positions()
        summary = store.closed_trades_summary()
    finally:
        store.close()

    print(f"БД: {db_path}")
    print(f"Открытые позиции: {len(positions)}")
    now_ts = time.time()
    for position in positions:
        print(_format_position_row(position, now_ts))
    print(
        "Закрытые сделки: {closed_trades}  "
        "вложено={invested_sol} SOL  возвращено={returned_sol} SOL  "
        "PnL={pnl_sol} SOL".format(**summary)
    )
    return 0


async def _run_pipeline(args: argparse.Namespace) -> int:
    """Запускает live/dry-run пайплайн до сигнала завершения.

    Args:
        args: Разобранные аргументы командной строки.

    Returns:
        Код выхода (0 — штатное завершение).
    """
    pipeline = LivePipeline(
        dry_run=args.dry_run,
        watchlist=args.watchlist,
        max_concurrent_positions=args.positions,
        capital_sol=args.capital,
        close_positions_on_stop=args.close_on_stop,
        db_path=args.db,
    )

    loop = asyncio.get_running_loop()
    stop_requested = False

    def _request_stop() -> None:
        """Ставит флаг остановки (идемпотентно по сигналам)."""
        nonlocal stop_requested
        if stop_requested:
            return
        stop_requested = True
        logger.warning("Сигнал завершения: останавливаю пайплайн...")
        pipeline.request_stop()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _request_stop)

    await pipeline.run()
    return 0


def main(argv: list[str] | None = None) -> int:
    """Синхронная точка входа.

    Args:
        argv: Аргументы командной строки (``None`` — ``sys.argv``).

    Returns:
        Код выхода процесса.
    """
    args = _build_parser().parse_args(argv)
    setup_logger(level=args.log_level)

    if args.status:
        return _show_status(_resolve_db_path(args))

    logger.info(
        "Старт бота: mode={}, network={}, positions={}, db={}",
        "dry-run" if args.dry_run else "LIVE",
        settings.solana_network,
        args.positions if args.positions is not None else settings.max_concurrent_positions,
        _resolve_db_path(args),
    )
    try:
        return asyncio.run(_run_pipeline(args))
    except KeyboardInterrupt:
        # На Windows SIGINT приходит как исключение: pipeline.run() уже
        # выполнил свой finally со stop() — здесь просто выходим штатно.
        logger.warning("Завершено по Ctrl+C")
        return 0
    except Exception:
        logger.exception("Критическая ошибка пайплайна")
        return 1


if __name__ == "__main__":
    sys.exit(main())
