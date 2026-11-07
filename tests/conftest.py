"""Общие фабрики тестовых данных: пулы, события сделок, позиции.

Фабрики собирают валидные объекты с реалистичными значениями по умолчанию,
чтобы тесты меняли только параметры, значимые для проверяемого поведения.
"""

from __future__ import annotations

from typing import Any

from models.trade_data import DEXType, PoolInfo, Position, TradeEvent, TradeSide

#: «Хороший» пул: 40 SOL ликвидности, 800 млн токенов.
GOOD_POOL_SOL_LAMPORTS: int = 40_000_000_000
GOOD_POOL_TOKENS: int = 800_000_000_000_000

POOL_ADDRESS: str = "Pool1111111111111111111111111111111111111111"
BASE_MINT: str = "Mint11111111111111111111111111111111111111111"
WALLET: str = "User11111111111111111111111111111111111111111"


def make_pool(**overrides: Any) -> PoolInfo:
    """Создаёт снимок пула с реалистичными резервами по умолчанию.

    Args:
        overrides: Поля PoolInfo для переопределения.

    Returns:
        :class:`~models.trade_data.PoolInfo`.
    """
    fields: dict[str, Any] = {
        "dex": DEXType.PUMP_FUN,
        "pool_address": POOL_ADDRESS,
        "base_mint": BASE_MINT,
        "quote_mint": "So11111111111111111111111111111111111111112",
        "base_decimals": 6,
        "quote_decimals": 9,
        "base_reserve": GOOD_POOL_TOKENS,
        "quote_reserve": GOOD_POOL_SOL_LAMPORTS,
        "created_at": 1000.0,
        "updated_at": 1000.0,
    }
    fields.update(overrides)
    return PoolInfo(**fields)


def make_event(
    side: TradeSide = TradeSide.BUY,
    *,
    sol_amount: float = 0.5,
    token_amount: float = 100_000.0,
    timestamp: float = 1000.0,
    signature: str = "sig",
    **overrides: Any,
) -> TradeEvent:
    """Создаёт событие сделки.

    Args:
        side: Направление сделки.
        sol_amount: Объём в SOL.
        token_amount: Количество токенов.
        timestamp: Время события, unix ts.
        signature: Подпись транзакции (уникализуйте для разных событий).
        overrides: Прочие поля TradeEvent.

    Returns:
        :class:`~models.trade_data.TradeEvent`.
    """
    fields: dict[str, Any] = {
        "signature": signature,
        "pool_address": POOL_ADDRESS,
        "base_mint": BASE_MINT,
        "dex": DEXType.PUMP_FUN,
        "side": side,
        "sol_amount": sol_amount,
        "token_amount": token_amount,
        "wallet": WALLET,
        "timestamp": timestamp,
    }
    fields.update(overrides)
    return TradeEvent(**fields)


def make_position(**overrides: Any) -> Position:
    """Создаёт открытую позицию с базовыми значениями.

    Args:
        overrides: Поля Position для переопределения.

    Returns:
        :class:`~models.trade_data.Position`.
    """
    fields: dict[str, Any] = {
        "base_mint": BASE_MINT,
        "pool_address": POOL_ADDRESS,
        "dex": DEXType.PUMP_FUN,
        "entry_price": 5e-9,
        "entry_sol": 0.5,
        "token_amount": 1e8,
        "initial_token_amount": 1e8,
        "opened_at": 1000.0,
        "peak_price": 5e-9,
        "tp1_taken": False,
        "realized_sol": 0.0,
        "signature": "entry-sig",
    }
    fields.update(overrides)
    return Position(**fields)
