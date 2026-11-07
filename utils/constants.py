"""Программные адреса, селекторы и константы Solana.

Модуль централизует все «магические» адреса программ, mint-адреса и
дискриминаторы инструкций, используемые построителем транзакций и листенером.
Значения приведены для Solana mainnet-beta.
"""

from __future__ import annotations

from config.settings import LAMPORTS_PER_SOL

#: Количество лампортов в SOL (ре-экспорт для удобства).
SOL_LAMPORTS: int = LAMPORTS_PER_SOL

#: Десятичные знаки SOL.
SOL_DECIMALS: int = 9

#: Десятичные знаки «мемкоинов» Pump.fun (6).
PUMP_TOKEN_DECIMALS: int = 6

# --------------------------------------------------------------------------- #
#  Системные программы
# --------------------------------------------------------------------------- #
#: Native Token Program (SPL Token).
TOKEN_PROGRAM_ID: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

#: Token-2022 Program (extensions).
TOKEN_2022_PROGRAM_ID: str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

#: Associated Token Account Program.
ASSOCIATED_TOKEN_PROGRAM_ID: str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

#: Compute Budget Program.
COMPUTE_BUDGET_PROGRAM_ID: str = "ComputeBudget111111111111111111111111111111"

#: System Program.
SYSTEM_PROGRAM_ID: str = "11111111111111111111111111111111"

#: Sysvar: Recent Blockhashes.
SYSVAR_RECENT_BLOCKHASHES: str = "SysvarRecentB1ockHashes11111111111111111111"

#: Sysvar: Clock.
SYSVAR_CLOCK: str = "SysvarC1ock11111111111111111111111111111111"

#: Wrapped SOL mint.
WSOL_MINT: str = "So11111111111111111111111111111111111111112"

# --------------------------------------------------------------------------- #
#  DEX / платформы
# --------------------------------------------------------------------------- #
#: Pump.fun bonding-curve program.
PUMP_FUN_PROGRAM_ID: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

#: Pump.fun AMM (после миграции на пул).
PUMP_FUN_AMM_PROGRAM_ID: str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

#: Raydium Liquidity Pool V4 (AMM).
RAYDIUM_AMM_V4_PROGRAM_ID: str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

#: Raydium CPMM (Constant Product) program.
RAYDIUM_CPMM_PROGRAM_ID: str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"

#: Serum DEX V3 program (для маршрутов Raydium V4).
SERUM_V3_PROGRAM_ID: str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"

#: OpenBook program (замена Serum).
OPENBOOK_PROGRAM_ID: str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX"

#: Множество распознаваемых DEX-программ.
KNOWN_DEX_PROGRAMS: frozenset[str] = frozenset(
    {
        PUMP_FUN_PROGRAM_ID,
        PUMP_FUN_AMM_PROGRAM_ID,
        RAYDIUM_AMM_V4_PROGRAM_ID,
        RAYDIUM_CPMM_PROGRAM_ID,
    }
)

# --------------------------------------------------------------------------- #
#  Дискриминаторы инструкций (Anchor, 8 байт)
# --------------------------------------------------------------------------- #
#: Pump.fun: инструкция ``buy``.
PUMP_BUY_DISCRIMINATOR: bytes = bytes([102, 6, 61, 18, 1, 218, 235, 234])

#: Pump.fun: инструкция ``sell``.
PUMP_SELL_DISCRIMINATOR: bytes = bytes([51, 230, 133, 164, 1, 127, 131, 173])

#: Pump.fun event-дискриминатор ``TradeEvent`` (для разбора логов).
PUMP_TRADE_EVENT_DISCRIMINATOR: bytes = bytes([189, 219, 127, 211, 78, 230, 97, 238])

# --------------------------------------------------------------------------- #
#  Pump.fun: фиксированные аккаунты и PDA-сиды
# --------------------------------------------------------------------------- #
#: Глобальный аккаунт программы (PDA ``["global"]``) — фиксирован.
PUMP_FUN_GLOBAL: str = "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"

#: Аккаунт-получатель комиссии протокола.
PUMP_FUN_FEE_RECIPIENT: str = "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM"

#: Event authority программы (PDA ``["__event_authority"]``) — фиксирован.
PUMP_FUN_EVENT_AUTHORITY: str = "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"

#: Сид PDA bonding curve: ``["bonding-curve", mint]``.
PUMP_BONDING_CURVE_SEED: bytes = b"bonding-curve"

#: Сид PDA creator vault: ``["creator-vault", creator]``.
PUMP_CREATOR_VAULT_SEED: bytes = b"creator-vault"

#: Сид глобального аккаунта: ``["global"]``.
PUMP_GLOBAL_SEED: bytes = b"global"

#: Сид event authority: ``["__event_authority"]``.
PUMP_EVENT_AUTHORITY_SEED: bytes = b"__event_authority"

#: Размер тела события ``TradeEvent`` Anchor (без 8-байтового дискриминатора).
PUMP_TRADE_EVENT_DISCRIMINATOR_LEN: int = 8

#: Raydium AMM V4: инструкция ``swapBaseIn``.
RAYDIUM_SWAP_BASE_IN_DISCRIMINATOR: int = 9

# --------------------------------------------------------------------------- #
#  Memo / анти-фронтран (Jito)
# --------------------------------------------------------------------------- #
#: SPL Memo program v2.
MEMO_PROGRAM_ID: str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

#: Текст memo-инструкции Jito для защиты от фронтрана.
JITO_FRONTRUN_MEMO: bytes = b"jitodontfront"

#: Базовый path Jito Block Engine API (дополняется к host из настроек).
JITO_BUNDLE_PATH: str = "/api/v1/bundles"
JITO_TIP_ACCOUNTS_PATH: str = "/api/v1/getTipAccounts"
JITO_BUNDLE_STATUS_PATH: str = "/api/v1/getBundleStatuses"

# --------------------------------------------------------------------------- #
#  LP burn / lock
# --------------------------------------------------------------------------- #
#: Адреса, считающиеся «сжигателями» LP-токенов (отправка = безвозвратная).
LP_BURN_ADDRESSES: frozenset[str] = frozenset(
    {
        "1nc1nerator11111111111111111111111111111111",
        "11111111111111111111111111111111111111111111",
    }
)

#: Raydium AMM V4: инструкция ``swapBaseOut``.
RAYDIUM_SWAP_BASE_OUT_DISCRIMINATOR: int = 11

# --------------------------------------------------------------------------- #
#  Jito
# --------------------------------------------------------------------------- #
#: Официальные tip-аккаунты Jito Block Engine (mainnet).
JITO_TIP_ACCOUNTS: tuple[str, ...] = (
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
)

#: Минимальный размер чаевых Jito (лампорты).
JITO_MIN_TIP_LAMPORTS: int = 1_000

# --------------------------------------------------------------------------- #
#  AMM / торговые константы
# --------------------------------------------------------------------------- #
#: Комиссия пула Raydium AMM V4 по умолчанию (25 bps = 0.25%).
RAYDIUM_FEE_BPS: int = 25

#: Комиссия Pump.fun bonding curve (100 bps = 1.0%).
PUMP_FUN_FEE_BPS: int = 100

#: Базисная точка (1 bps = 0.01%).
BPS_DENOMINATOR: int = 10_000

#: Публичный базовый эндпоинт Solana mainnet (fallback).
PUBLIC_MAINNET_RPC: str = "https://api.mainnet-beta.solana.com"


def lamports_to_sol(lamports: int) -> float:
    """Переводит лампорты в SOL.

    Args:
        lamports: Количество лампортов.

    Returns:
        Количество SOL.
    """
    return lamports / SOL_LAMPORTS


def sol_to_lamports(amount_sol: float) -> int:
    """Переводит SOL в лампорты (округление вниз).

    Args:
        amount_sol: Количество SOL.

    Returns:
        Целое количество лампортов.
    """
    return int(amount_sol * SOL_LAMPORTS)
