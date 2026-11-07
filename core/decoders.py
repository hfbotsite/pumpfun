"""Разбор (декодирование) on-chain данных Solana для поддерживаемых DEX.

Модуль содержит только чистые функции разбора бинарных данных аккаунтов и
событий из логов. Никаких сетевых вызовов — это делает декодеры полностью
тестируемыми и пригодными для бэктестера.

Поддерживаемые layouts:

* :func:`decode_bonding_curve` — Pump.fun bonding curve (``BondingCurve``);
* :func:`decode_pump_swap_pool` — PumpSwap AMM (``Pool``);
* :func:`decode_raydium_v4_pool` — Raydium AMM V4 (``AmmInfo``);
* :func:`decode_pump_trade_event` — событие ``TradeEvent`` из логов Pump.fun.

Все резервы возвращаются в «сырых» единицах: лампорты для SOL, base units
для токенов.
"""

from __future__ import annotations

from dataclasses import dataclass

from solders.pubkey import Pubkey

from utils.constants import (
    PUMP_FUN_AMM_PROGRAM_ID,
    RAYDIUM_AMM_V4_PROGRAM_ID,
)
from utils.spl import SplError, decode_token_amount

# --------------------------------------------------------------------------- #
#  Базовые утилиты разбора
# --------------------------------------------------------------------------- #


def _u64(data: bytes, offset: int) -> int:
    """Читает little-endian ``u64`` по смещению.

    Args:
        data: Байты аккаунта.
        offset: Смещение начала поля.

    Returns:
        Значение как целое.

    Raises:
        ValueError: Если данных недостаточно.
    """
    end = offset + 8
    if len(data) < end:
        raise ValueError(f"Данных недостаточно для u64 @ {offset} (len={len(data)})")
    return int.from_bytes(data[offset:end], "little", signed=False)


def _u16(data: bytes, offset: int) -> int:
    """Читает little-endian ``u16`` по смещению.

    Args:
        data: Байты аккаунта.
        offset: Смещение начала поля.

    Returns:
        Значение как целое.

    Raises:
        ValueError: Если данных недостаточно.
    """
    end = offset + 2
    if len(data) < end:
        raise ValueError(f"Данных недостаточно для u16 @ {offset} (len={len(data)})")
    return int.from_bytes(data[offset:end], "little", signed=False)


def _i64(data: bytes, offset: int) -> int:
    """Читает little-endian ``i64`` по смещению.

    Args:
        data: Байты аккаунта.
        offset: Смещение начала поля.

    Returns:
        Значение как целое со знаком.

    Raises:
        ValueError: Если данных недостаточно.
    """
    end = offset + 8
    if len(data) < end:
        raise ValueError(f"Данных недостаточно для i64 @ {offset} (len={len(data)})")
    return int.from_bytes(data[offset:end], "little", signed=True)


def _pubkey(data: bytes, offset: int) -> str:
    """Читает 32-байтовый ``Pubkey`` по смещению.

    Args:
        data: Байты аккаунта.
        offset: Смещение начала ключа.

    Returns:
        Адрес в base58.

    Raises:
        ValueError: Если данных недостаточно.
    """
    end = offset + 32
    if len(data) < end:
        raise ValueError(f"Данных недостаточно для pubkey @ {offset} (len={len(data)})")
    return str(Pubkey.from_bytes(data[offset:end]))


# --------------------------------------------------------------------------- #
#  Pump.fun bonding curve
# --------------------------------------------------------------------------- #
#: Смещение ``virtual_token_reserves`` в аккаунте BondingCurve.
_BC_VIRTUAL_TOKEN_RESERVES: int = 8

#: Смещение ``virtual_sol_reserves`` в аккаунте BondingCurve.
_BC_VIRTUAL_SOL_RESERVES: int = 16

#: Смещение ``real_token_reserves`` в аккаунте BondingCurve.
_BC_REAL_TOKEN_RESERVES: int = 24

#: Смещение ``real_sol_reserves`` в аккаунте BondingCurve.
_BC_REAL_SOL_RESERVES: int = 32

#: Смещение ``token_total_supply`` в аккаунте BondingCurve.
_BC_TOKEN_TOTAL_SUPPLY: int = 40

#: Смещение флага ``complete`` в аккаунте BondingCurve.
_BC_COMPLETE: int = 48

#: Смещение ``creator`` в аккаунте BondingCurve.
_BC_CREATOR: int = 49

#: Минимальный размер валидного аккаунта BondingCurve.
BC_MIN_SIZE: int = 81


@dataclass(frozen=True, slots=True)
class BondingCurveState:
    """Состояние bonding curve Pump.fun."""

    virtual_token_reserves: int
    virtual_sol_reserves: int
    real_token_reserves: int
    real_sol_reserves: int
    token_total_supply: int
    complete: bool
    creator: str


def decode_bonding_curve(data: bytes) -> BondingCurveState | None:
    """Разбирает аккаунт bonding curve Pump.fun.

    Args:
        data: Сырые байты аккаунта ``BondingCurve``.

    Returns:
        :class:`BondingCurveState` или ``None``, если размер не соответствует.
    """
    if len(data) < BC_MIN_SIZE:
        return None
    try:
        return BondingCurveState(
            virtual_token_reserves=_u64(data, _BC_VIRTUAL_TOKEN_RESERVES),
            virtual_sol_reserves=_u64(data, _BC_VIRTUAL_SOL_RESERVES),
            real_token_reserves=_u64(data, _BC_REAL_TOKEN_RESERVES),
            real_sol_reserves=_u64(data, _BC_REAL_SOL_RESERVES),
            token_total_supply=_u64(data, _BC_TOKEN_TOTAL_SUPPLY),
            complete=bool(data[_BC_COMPLETE]),
            creator=_pubkey(data, _BC_CREATOR),
        )
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
#  PumpSwap AMM (после миграции)
# --------------------------------------------------------------------------- #
#: Смещение ``base_mint`` в аккаунте Pool (PumpSwap).
_PS_BASE_MINT: int = 43

#: Смещение ``quote_mint`` в аккаунте Pool (PumpSwap).
_PS_QUOTE_MINT: int = 75

#: Смещение ``lp_mint`` в аккаунте Pool (PumpSwap).
_PS_LP_MINT: int = 107

#: Смещение ``pool_base_token_account`` в аккаунте Pool (PumpSwap).
_PS_BASE_VAULT: int = 139

#: Смещение ``pool_quote_token_account`` в аккаунте Pool (PumpSwap).
_PS_QUOTE_VAULT: int = 171

#: Минимальный размер валидного аккаунта Pool (PumpSwap).
PS_POOL_MIN_SIZE: int = 211


@dataclass(frozen=True, slots=True)
class PumpSwapPool:
    """Разобранный аккаунт пула PumpSwap."""

    base_mint: str
    quote_mint: str
    lp_mint: str
    base_vault: str
    quote_vault: str


def decode_pump_swap_pool(data: bytes) -> PumpSwapPool | None:
    """Разбирает аккаунт пула PumpSwap AMM.

    Args:
        data: Сырые байты аккаунта ``Pool``.

    Returns:
        :class:`PumpSwapPool` или ``None``, если размер не соответствует.
    """
    if len(data) < PS_POOL_MIN_SIZE:
        return None
    try:
        return PumpSwapPool(
            base_mint=_pubkey(data, _PS_BASE_MINT),
            quote_mint=_pubkey(data, _PS_QUOTE_MINT),
            lp_mint=_pubkey(data, _PS_LP_MINT),
            base_vault=_pubkey(data, _PS_BASE_VAULT),
            quote_vault=_pubkey(data, _PS_QUOTE_VAULT),
        )
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
#  Raydium AMM V4
# --------------------------------------------------------------------------- #
#: Смещение ``base_vault`` в аккаунте AmmInfo (Raydium V4).
_RD_BASE_VAULT: int = 336

#: Смещение ``quote_vault`` в аккаунте AmmInfo (Raydium V4).
_RD_QUOTE_VAULT: int = 368

#: Смещение ``base_mint`` в аккаунте AmmInfo (Raydium V4).
_RD_BASE_MINT: int = 400

#: Смещение ``quote_mint`` в аккаунте AmmInfo (Raydium V4).
_RD_QUOTE_MINT: int = 432

#: Смещение ``lp_mint`` в аккаунте AmmInfo (Raydium V4).
_RD_LP_MINT: int = 464

#: Смещение ``lp_reserve`` в аккаунте AmmInfo (Raydium V4).
_RD_LP_RESERVE: int = 720

#: Минимальный размер валидного аккаунта AmmInfo (Raydium V4).
RD_POOL_MIN_SIZE: int = 752


@dataclass(frozen=True, slots=True)
class RaydiumV4Pool:
    """Разобранный аккаунт пула Raydium AMM V4."""

    base_mint: str
    quote_mint: str
    lp_mint: str
    base_vault: str
    quote_vault: str
    lp_reserve: int


def decode_raydium_v4_pool(data: bytes) -> RaydiumV4Pool | None:
    """Разбирает аккаунт пула Raydium AMM V4.

    Args:
        data: Сырые байты аккаунта ``AmmInfo``.

    Returns:
        :class:`RaydiumV4Pool` или ``None``, если размер не соответствует.
    """
    if len(data) < RD_POOL_MIN_SIZE:
        return None
    try:
        return RaydiumV4Pool(
            base_mint=_pubkey(data, _RD_BASE_MINT),
            quote_mint=_pubkey(data, _RD_QUOTE_MINT),
            lp_mint=_pubkey(data, _RD_LP_MINT),
            base_vault=_pubkey(data, _RD_BASE_VAULT),
            quote_vault=_pubkey(data, _RD_QUOTE_VAULT),
            lp_reserve=_u64(data, _RD_LP_RESERVE),
        )
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
#  Пул: обобщённое представление
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class DecodedPool:
    """Универсальное (DEX-независимое) описание AMM-пула с vault-аккаунтами."""

    dex_program: str
    base_mint: str
    quote_mint: str
    base_vault: str
    quote_vault: str
    lp_mint: str | None = None


def decode_pool_by_owner(owner_program: str, data: bytes) -> DecodedPool | None:
    """Разбирает аккаунт пула по программе-владельцу.

    Args:
        owner_program: Адрес программы, владеющей аккаунтом.
        data: Сырые байты аккаунта.

    Returns:
        :class:`DecodedPool` или ``None``, если layout не распознан.
    """
    if owner_program == PUMP_FUN_AMM_PROGRAM_ID:
        pool = decode_pump_swap_pool(data)
        if pool is None:
            return None
        return DecodedPool(
            dex_program=owner_program,
            base_mint=pool.base_mint,
            quote_mint=pool.quote_mint,
            base_vault=pool.base_vault,
            quote_vault=pool.quote_vault,
            lp_mint=pool.lp_mint,
        )
    if owner_program == RAYDIUM_AMM_V4_PROGRAM_ID:
        raydium = decode_raydium_v4_pool(data)
        if raydium is None:
            return None
        return DecodedPool(
            dex_program=owner_program,
            base_mint=raydium.base_mint,
            quote_mint=raydium.quote_mint,
            base_vault=raydium.base_vault,
            quote_vault=raydium.quote_vault,
            lp_mint=raydium.lp_mint,
        )
    return None


# --------------------------------------------------------------------------- #
#  События сделок Pump.fun
# --------------------------------------------------------------------------- #
#: Смещение ``mint`` в теле события TradeEvent.
_TE_MINT: int = 0

#: Смещение ``sol_amount`` в теле события TradeEvent.
_TE_SOL_AMOUNT: int = 32

#: Смещение ``token_amount`` в теле события TradeEvent.
_TE_TOKEN_AMOUNT: int = 40

#: Смещение флага ``is_buy`` в теле события TradeEvent.
_TE_IS_BUY: int = 48

#: Смещение ``user`` в теле события TradeEvent.
_TE_USER: int = 49

#: Смещение ``timestamp`` в теле события TradeEvent.
_TE_TIMESTAMP: int = 81

#: Смещение ``virtual_sol_reserves`` в теле события TradeEvent.
_TE_VIRTUAL_SOL_RESERVES: int = 89

#: Смещение ``virtual_token_reserves`` в теле события TradeEvent.
_TE_VIRTUAL_TOKEN_RESERVES: int = 97

#: Полный размер тела события TradeEvent.
TRADE_EVENT_BODY_SIZE: int = 105


@dataclass(frozen=True, slots=True)
class PumpTradeEvent:
    """Разобранное событие сделки Pump.fun."""

    mint: str
    sol_amount: int
    token_amount: int
    is_buy: bool
    user: str
    timestamp: int
    virtual_sol_reserves: int
    virtual_token_reserves: int


def decode_pump_trade_event(payload: bytes) -> PumpTradeEvent | None:
    """Разбирает тело события ``TradeEvent`` Pump.fun.

    Args:
        payload: Тело события без 8-байтового дискриминатора.

    Returns:
        :class:`PumpTradeEvent` или ``None``, если размер недостаточен.
    """
    if len(payload) < TRADE_EVENT_BODY_SIZE:
        return None
    try:
        return PumpTradeEvent(
            mint=_pubkey(payload, _TE_MINT),
            sol_amount=_u64(payload, _TE_SOL_AMOUNT),
            token_amount=_u64(payload, _TE_TOKEN_AMOUNT),
            is_buy=bool(payload[_TE_IS_BUY]),
            user=_pubkey(payload, _TE_USER),
            timestamp=_i64(payload, _TE_TIMESTAMP),
            virtual_sol_reserves=_u64(payload, _TE_VIRTUAL_SOL_RESERVES),
            virtual_token_reserves=_u64(payload, _TE_VIRTUAL_TOKEN_RESERVES),
        )
    except ValueError:
        return None


def vault_amount_from_account(data: bytes) -> int | None:
    """Извлекает баланс из данных token-аккаунта (обёртка над SPL-декодером).

    Args:
        data: Сырые байты token-аккаунта.

    Returns:
        Баланс в base units или ``None`` при ошибке разбора.
    """
    try:
        return decode_token_amount(data)
    except SplError:
        return None
