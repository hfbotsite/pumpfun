"""Тесты Borsh-декодеров: событие TradeEvent и аккаунт bonding curve."""

from __future__ import annotations

import pytest
from solders.pubkey import Pubkey

from core.decoders import (
    TRADE_EVENT_BODY_SIZE,
    decode_bonding_curve,
    decode_pump_trade_event,
    vault_amount_from_account,
)
from utils.spl import SplError, decode_token_amount


def _pub32(seed: int) -> bytes:
    """32 байта псевдо-публичного ключа с заданным наполнением.

    Args:
        seed: Заполняющий байт.

    Returns:
        32 байта.
    """
    return bytes([seed]) * 32


def _u64(value: int) -> bytes:
    """Кодирует little-endian u64.

    Args:
        value: Значение.

    Returns:
        8 байт.
    """
    return value.to_bytes(8, "little")


def _i64(value: int) -> bytes:
    """Кодирует little-endian i64.

    Args:
        value: Значение.

    Returns:
        8 байт.
    """
    return value.to_bytes(8, "little", signed=True)


def build_trade_event_payload(
    *,
    mint: bytes = _pub32(1),
    sol_amount: int = 1_000_000_000,
    token_amount: int = 200_000_000,
    is_buy: bool = True,
    user: bytes = _pub32(2),
    timestamp: int = 1_700_000_000,
    virtual_sol_reserves: int = 30_000_000_000,
    virtual_token_reserves: int = 600_000_000_000_000,
) -> bytes:
    """Собирает бинарное тело события TradeEvent Pump.fun (105 байт).

    Returns:
        Payload без дискриминатора.
    """
    return (
        mint
        + _u64(sol_amount)
        + _u64(token_amount)
        + (b"\x01" if is_buy else b"\x00")
        + user
        + _i64(timestamp)
        + _u64(virtual_sol_reserves)
        + _u64(virtual_token_reserves)
    )


class TestPumpTradeEvent:
    """Разбор Anchor-события сделки."""

    def test_decode_buy(self) -> None:
        """Покупка разбирается во все поля без смещений."""
        payload = build_trade_event_payload()
        event = decode_pump_trade_event(payload)
        assert event is not None
        assert event.mint == str(Pubkey.from_bytes(_pub32(1)))
        assert event.sol_amount == 1_000_000_000
        assert event.token_amount == 200_000_000
        assert event.is_buy is True
        assert event.timestamp == 1_700_000_000
        assert event.virtual_sol_reserves == 30_000_000_000

    def test_decode_sell_flag(self) -> None:
        """Флаг is_buy=0 трактуется как продажа."""
        payload = build_trade_event_payload(is_buy=False)
        event = decode_pump_trade_event(payload)
        assert event is not None
        assert event.is_buy is False

    def test_truncated_payload_rejected(self) -> None:
        """Payload меньше 105 байт не разбирается."""
        payload = build_trade_event_payload()[:TRADE_EVENT_BODY_SIZE - 1]
        assert decode_pump_trade_event(payload) is None

    def test_user_field_layout(self) -> None:
        """Поле user читается с корректного смещения (49..81)."""
        payload = build_trade_event_payload()
        event = decode_pump_trade_event(payload)
        # user != mint при разных seed.
        assert event.user != event.mint


class TestBondingCurve:
    """Разбор аккаунта bonding curve (дискриминатор + 73 байта полей)."""

    def test_decode_curve(self) -> None:
        """Смещения полей соответствуют фактическому layout (поля с 8-го байта)."""
        data = (
            b"\x17\xb8\x10\x11\xf7\xd9\xe3\xfb"  # Anchor-дискриминатор аккаунта
            + _u64(600_000_000_000_000)  # virtual_token_reserves
            + _u64(30_000_000_000)     # virtual_sol_reserves
            + _u64(200_000_000_000_000)  # real_token_reserves
            + _u64(9_000_000_000)      # real_sol_reserves
            + _u64(1_000_000_000_000_000)  # token_total_supply
            + b"\x00"                   # complete
            + _pub32(3)                 # creator
        )
        curve = decode_bonding_curve(data)
        assert curve is not None
        assert curve.virtual_token_reserves == 600_000_000_000_000
        assert curve.virtual_sol_reserves == 30_000_000_000
        assert curve.real_sol_reserves == 9_000_000_000
        assert curve.complete is False
        assert curve.creator  # непустая base58-строка

    def test_short_data_rejected(self) -> None:
        """Короткий аккаунт не разбирается."""
        assert decode_bonding_curve(b"\x00" * 40) is None


class TestVaultAmount:
    """Баланс vault-аккаунта из сырых данных."""

    def test_token_account_amount(self) -> None:
        """Смещение amount=64 читает баланс SPL Token-аккаунта."""
        data = bytearray(165)
        data[64:72] = _u64(123_456_789)
        assert vault_amount_from_account(bytes(data)) == 123_456_789

    def test_bad_data_returns_none(self) -> None:
        """Мусорные данные дают None (SplError проглатывается)."""
        assert vault_amount_from_account(b"\x00") is None
        with pytest.raises(SplError):
            # Прямой декодер обязан бросать исключение на коротких данных.
            decode_token_amount(b"\x01\x02")
