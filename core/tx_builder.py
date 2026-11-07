"""Сборка инструкций Solana: Pump.fun bonding curve, Compute Budget, Jito.

Модуль — чистая, детерминированная фабрика инструкций. Он не выполняет сетевых
вызовов и не подписывает транзакции: его задача — из уже разрешённых адресов
аккаунтов собрать :class:`solders.instruction.Instruction`.

Поддерживается:

* **Pump.fun bonding curve** — инструкции ``buy`` / ``sell`` с явным
  ``max_sol_cost`` / ``min_sol_output`` (защита от проскальзывания);
* **Compute Budget** — лимит Compute Unit и приоритетная комиссия
  (micro-lamports per CU) для гарантированного попадания в ближайший слот;
* **Jito tip** — системный перевод чаевых валидатору;
* **Memo ``jitodontfront``** — анти-фронтран инструкция Jito.

Вывод адресов (PDA) инкапсулирован в функциях :func:`derive_bonding_curve`,
:func:`derive_creator_vault` и :func:`derive_bonding_curve_ata`; при
некорректных входных данных поднимается :class:`TxBuilderError`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from config.settings import settings
from utils.constants import (
    JITO_FRONTRUN_MEMO,
    MEMO_PROGRAM_ID,
    PUMP_BONDING_CURVE_SEED,
    PUMP_BUY_DISCRIMINATOR,
    PUMP_CREATOR_VAULT_SEED,
    PUMP_FUN_EVENT_AUTHORITY,
    PUMP_FUN_FEE_RECIPIENT,
    PUMP_FUN_GLOBAL,
    PUMP_FUN_PROGRAM_ID,
    PUMP_SELL_DISCRIMINATOR,
    SYSTEM_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
)
from utils.spl import SplError, derive_associated_token_address, parse_pubkey


class TxBuilderError(RuntimeError):
    """Ошибка сборки инструкции транзакции."""


@dataclass(frozen=True, slots=True)
class PumpAccounts:
    """Разрешённые аккаунты для инструкций Pump.fun bonding curve.

    Attributes:
        mint: Mint мемкоина.
        user: Кошелёк-покупатель/продавец (signer).
        bonding_curve: PDA bonding curve для mint.
        associated_bonding_curve: ATA bonding curve (хранит токены кривой).
        associated_user: ATA пользователя для mint.
        creator_vault: PDA ``creator-vault`` для создателя токена.
    """

    mint: Pubkey
    user: Pubkey
    bonding_curve: Pubkey
    associated_bonding_curve: Pubkey
    associated_user: Pubkey
    creator_vault: Pubkey

    #: Глобальный аккаунт программы (обычно фиксирован).
    global_account: Pubkey = field(
        default_factory=lambda: parse_pubkey(PUMP_FUN_GLOBAL)
    )
    #: Получатель комиссии протокола.
    fee_recipient: Pubkey = field(
        default_factory=lambda: parse_pubkey(PUMP_FUN_FEE_RECIPIENT)
    )
    #: Event authority программы.
    event_authority: Pubkey = field(
        default_factory=lambda: parse_pubkey(PUMP_FUN_EVENT_AUTHORITY)
    )
    #: Адрес программы Pump.fun.
    program: Pubkey = field(
        default_factory=lambda: parse_pubkey(PUMP_FUN_PROGRAM_ID)
    )
    #: Программа токена (SPL Token / Token-2022).
    token_program: Pubkey = field(
        default_factory=lambda: parse_pubkey(TOKEN_PROGRAM_ID)
    )
    #: Системная программа.
    system_program: Pubkey = field(
        default_factory=lambda: parse_pubkey(SYSTEM_PROGRAM_ID)
    )


# --------------------------------------------------------------------------- #
#  PDA-выводы
# --------------------------------------------------------------------------- #
def derive_bonding_curve(
    mint: Pubkey | str, program: Pubkey | str = PUMP_FUN_PROGRAM_ID
) -> Pubkey:
    """Выводит PDA bonding curve для mint.

    Args:
        mint: Mint мемкоина.
        program: Адрес программы Pump.fun.

    Returns:
        :class:`Pubkey` bonding curve.

    Raises:
        TxBuilderError: Если не удалось вывести адрес.
    """
    mint_pk = mint if isinstance(mint, Pubkey) else parse_pubkey(mint)
    program_pk = program if isinstance(program, Pubkey) else parse_pubkey(program)
    try:
        address, _bump = Pubkey.find_program_address(
            [PUMP_BONDING_CURVE_SEED, bytes(mint_pk)], program_pk
        )
    except ValueError as exc:
        raise TxBuilderError("Не удалось вывести PDA bonding curve") from exc
    return address


def derive_creator_vault(
    creator: Pubkey | str, program: Pubkey | str = PUMP_FUN_PROGRAM_ID
) -> Pubkey:
    """Выводит PDA creator vault для создателя токена.

    Args:
        creator: Кошелёк создателя токена.
        program: Адрес программы Pump.fun.

    Returns:
        :class:`Pubkey` creator vault.

    Raises:
        TxBuilderError: Если не удалось вывести адрес.
    """
    creator_pk = creator if isinstance(creator, Pubkey) else parse_pubkey(creator)
    program_pk = program if isinstance(program, Pubkey) else parse_pubkey(program)
    try:
        address, _bump = Pubkey.find_program_address(
            [PUMP_CREATOR_VAULT_SEED, bytes(creator_pk)], program_pk
        )
    except ValueError as exc:
        raise TxBuilderError("Не удалось вывести PDA creator vault") from exc
    return address


def derive_bonding_curve_ata(mint: Pubkey | str, bonding_curve: Pubkey) -> Pubkey:
    """Выводит ATA bonding curve для mint.

    Args:
        mint: Mint мемкоина.
        bonding_curve: PDA bonding curve (владелец ATA).

    Returns:
        :class:`Pubkey` associated bonding curve account.

    Raises:
        TxBuilderError: Если не удалось вывести адрес.
    """
    try:
        return derive_associated_token_address(bonding_curve, mint)
    except SplError as exc:
        raise TxBuilderError("Не удалось вывести ATA bonding curve") from exc


def derive_user_ata(user: Pubkey | str, mint: Pubkey | str) -> Pubkey:
    """Выводит ATA пользователя для mint.

    Args:
        user: Кошелёк пользователя.
        mint: Mint мемкоина.

    Returns:
        :class:`Pubkey` associated token account пользователя.

    Raises:
        TxBuilderError: Если не удалось вывести адрес.
    """
    try:
        return derive_associated_token_address(user, mint)
    except SplError as exc:
        raise TxBuilderError("Не удалось вывести ATA пользователя") from exc


def build_pump_accounts(
    mint: Pubkey | str,
    user: Pubkey | str,
    creator: Pubkey | str,
) -> PumpAccounts:
    """Собирает полный набор аккаунтов Pump.fun для mint.

    Args:
        mint: Mint мемкоина.
        user: Кошелёк пользователя.
        creator: Кошелёк создателя токена (для вывода creator vault).

    Returns:
        :class:`PumpAccounts` с разрешёнными адресами.
    """
    mint_pk = mint if isinstance(mint, Pubkey) else parse_pubkey(mint)
    user_pk = user if isinstance(user, Pubkey) else parse_pubkey(user)
    bonding_curve = derive_bonding_curve(mint_pk)
    return PumpAccounts(
        mint=mint_pk,
        user=user_pk,
        bonding_curve=bonding_curve,
        associated_bonding_curve=derive_bonding_curve_ata(mint_pk, bonding_curve),
        associated_user=derive_user_ata(user_pk, mint_pk),
        creator_vault=derive_creator_vault(creator),
    )


# --------------------------------------------------------------------------- #
#  Кодирование аргументов
# --------------------------------------------------------------------------- #
def _u64_le(value: int) -> bytes:
    """Кодирует целое в little-endian ``u64``.

    Args:
        value: Значение (0..2**64-1).

    Returns:
        Восемь байт little-endian.

    Raises:
        TxBuilderError: Если значение отрицательное или не влезает в u64.
    """
    if value < 0 or value >= (1 << 64):
        raise TxBuilderError(f"Значение {value} не укладывается в u64")
    return value.to_bytes(8, "little", signed=False)


# --------------------------------------------------------------------------- #
#  Инструкции Pump.fun
# --------------------------------------------------------------------------- #
def _pump_buy_metas(acc: PumpAccounts) -> list[AccountMeta]:
    """Собирает список account-метаданных инструкции ``buy``.

    Args:
        acc: Разрешённые аккаунты.

    Returns:
        Список :class:`AccountMeta` в порядке Anchor IDL.
    """
    return [
        AccountMeta(pubkey=acc.global_account, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.fee_recipient, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.mint, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.bonding_curve, is_signer=False, is_writable=True),
        AccountMeta(
            pubkey=acc.associated_bonding_curve, is_signer=False, is_writable=True
        ),
        AccountMeta(pubkey=acc.associated_user, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.user, is_signer=True, is_writable=True),
        AccountMeta(pubkey=acc.system_program, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.token_program, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.creator_vault, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.event_authority, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.program, is_signer=False, is_writable=False),
    ]


def _pump_sell_metas(acc: PumpAccounts) -> list[AccountMeta]:
    """Собирает список account-метаданных инструкции ``sell``.

    Args:
        acc: Разрешённые аккаунты.

    Returns:
        Список :class:`AccountMeta` в порядке Anchor IDL.
    """
    return [
        AccountMeta(pubkey=acc.global_account, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.fee_recipient, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.mint, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.bonding_curve, is_signer=False, is_writable=True),
        AccountMeta(
            pubkey=acc.associated_bonding_curve, is_signer=False, is_writable=True
        ),
        AccountMeta(pubkey=acc.associated_user, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.user, is_signer=True, is_writable=True),
        AccountMeta(pubkey=acc.system_program, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.creator_vault, is_signer=False, is_writable=True),
        AccountMeta(pubkey=acc.token_program, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.event_authority, is_signer=False, is_writable=False),
        AccountMeta(pubkey=acc.program, is_signer=False, is_writable=False),
    ]


def build_pump_buy_instruction(
    acc: PumpAccounts,
    token_amount: int,
    max_sol_cost_lamports: int,
) -> Instruction:
    """Собирает инструкцию ``buy`` Pump.fun bonding curve.

    Args:
        acc: Разрешённые аккаунты Pump.fun.
        token_amount: Ожидаемое количество токенов (base units) — сколько
            запрашиваем получить.
        max_sol_cost_lamports: Максимальная цена в лампортах (защита от
            проскальзывания по SOL).

    Returns:
        :class:`solders.instruction.Instruction` покупки.

    Raises:
        TxBuilderError: Если аргументы некорректны.
    """
    if token_amount <= 0:
        raise TxBuilderError("token_amount для buy должен быть > 0")
    data = PUMP_BUY_DISCRIMINATOR + _u64_le(token_amount) + _u64_le(
        max_sol_cost_lamports
    )
    return Instruction(
        program_id=acc.program, data=data, accounts=_pump_buy_metas(acc)
    )


def build_pump_sell_instruction(
    acc: PumpAccounts,
    token_amount: int,
    min_sol_output_lamports: int,
) -> Instruction:
    """Собирает инструкцию ``sell`` Pump.fun bonding curve.

    Args:
        acc: Разрешённые аккаунты Pump.fun.
        token_amount: Количество продаваемых токенов (base units).
        min_sol_output_lamports: Минимально приемлемый выход в лампортах
            (защита от проскальзывания).

    Returns:
        :class:`solders.instruction.Instruction` продажи.

    Raises:
        TxBuilderError: Если аргументы некорректны.
    """
    if token_amount <= 0:
        raise TxBuilderError("token_amount для sell должен быть > 0")
    data = PUMP_SELL_DISCRIMINATOR + _u64_le(token_amount) + _u64_le(
        min_sol_output_lamports
    )
    return Instruction(
        program_id=acc.program, data=data, accounts=_pump_sell_metas(acc)
    )


# --------------------------------------------------------------------------- #
#  Compute Budget
# --------------------------------------------------------------------------- #
def build_compute_budget_instructions(
    *,
    compute_unit_limit: int | None = None,
    priority_fee_micro_lamports: int | None = None,
) -> list[Instruction]:
    """Собирает инструкции Compute Budget (лимит CU и приоритетная комиссия).

    Args:
        compute_unit_limit: Лимит Compute Unit (по умолчанию из настроек).
        priority_fee_micro_lamports: Приоритетная комиссия в micro-lamports
            за Compute Unit (по умолчанию из настроек).

    Returns:
        Список из одной-двух инструкций Compute Budget.
    """
    units = (
        settings.compute_unit_limit
        if compute_unit_limit is None
        else compute_unit_limit
    )
    micro_lamports = (
        settings.priority_fee_micro_lamports
        if priority_fee_micro_lamports is None
        else priority_fee_micro_lamports
    )
    instructions: list[Instruction] = [set_compute_unit_limit(units)]
    if micro_lamports > 0:
        instructions.append(set_compute_unit_price(micro_lamports))
    return instructions


# --------------------------------------------------------------------------- #
#  Jito: memo-антифронтран и чаевые
# --------------------------------------------------------------------------- #
def build_frontrun_memo_instruction(
    user: Pubkey | str,
    memo: bytes = JITO_FRONTRUN_MEMO,
) -> Instruction:
    """Собирает memo-инструкцию ``jitodontfront`` (анти-сэндвич Jito).

    Args:
        user: Кошелёк-подписант (владелец memo-инструкции).
        memo: Текст memo (по умолчанию ``jitodontfront``).

    Returns:
        :class:`solders.instruction.Instruction` memo.
    """
    user_pk = user if isinstance(user, Pubkey) else parse_pubkey(user)
    return Instruction(
        program_id=parse_pubkey(MEMO_PROGRAM_ID),
        data=memo,
        accounts=[AccountMeta(pubkey=user_pk, is_signer=True, is_writable=False)],
    )


def build_tip_instruction(
    user: Pubkey | str,
    tip_account: Pubkey | str,
    lamports: int,
) -> Instruction:
    """Собирает системную инструкцию перевода чаевых Jito.

    Args:
        user: Кошелёк-плательщик (signer).
        tip_account: Аккаунт-получатель чаевых Jito.
        lamports: Размер чаевых в лампортах.

    Returns:
        :class:`solders.instruction.Instruction` перевода.

    Raises:
        TxBuilderError: Если размер чаевых некорректен.
    """
    if lamports <= 0:
        raise TxBuilderError("Размер чаевых должен быть > 0")
    user_pk = user if isinstance(user, Pubkey) else parse_pubkey(user)
    tip_pk = tip_account if isinstance(tip_account, Pubkey) else parse_pubkey(tip_account)
    return Instruction(
        program_id=parse_pubkey(SYSTEM_PROGRAM_ID),
        data=(2).to_bytes(4, "little") + lamports.to_bytes(8, "little"),
        accounts=[
            AccountMeta(pubkey=user_pk, is_signer=True, is_writable=True),
            AccountMeta(pubkey=tip_pk, is_signer=False, is_writable=True),
        ],
    )
