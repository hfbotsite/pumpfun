"""Программные адреса, селекторы и константы Solana.

Модуль централизует все «магические» адреса программ, mint-адреса и
дискриминаторы инструкций, используемые построителем транзакций и листенером.
Значения приведены для Solana mainnet-beta.
"""

from __future__ import annotations

#: Десятичные знаки SOL.
SOL_DECIMALS: int = 9

#: Десятичные знаки «мемкоинов» Pump.fun (6).
PUMP_TOKEN_DECIMALS: int = 6

# --------------------------------------------------------------------------- #
#  Системные программы
# --------------------------------------------------------------------------- #
#: Native Token Program (SPL Token).
TOKEN_PROGRAM_ID: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

#: Associated Token Account Program.
ASSOCIATED_TOKEN_PROGRAM_ID: str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

#: System Program.
SYSTEM_PROGRAM_ID: str = "11111111111111111111111111111111"

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

#: Размер тела события ``TradeEvent`` Anchor (без 8-байтового дискриминатора).
PUMP_TRADE_EVENT_DISCRIMINATOR_LEN: int = 8

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

# --------------------------------------------------------------------------- #
#  AMM / торговые константы
# --------------------------------------------------------------------------- #
#: Комиссия пула Raydium AMM V4 по умолчанию (25 bps = 0.25%).
RAYDIUM_FEE_BPS: int = 25

#: Комиссия Pump.fun bonding curve (100 bps = 1.0%).
PUMP_FUN_FEE_BPS: int = 100

#: Базисная точка (1 bps = 0.01%).
BPS_DENOMINATOR: int = 10_000
