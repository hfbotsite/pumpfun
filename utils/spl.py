"""SPL-хелперы: кошелёк, associated token account, разбор данных аккаунтов.

Модуль инкапсулирует низкоуровневые операции с ``solders``:

* загрузка :class:`solders.keypair.Keypair` из base58-строки или JSON-файла;
* вывод associated token address (ATA) для пары ``owner/mint``;
* декодирование ``Mint``-аккаунта (authorities, decimals, supply);
* декодирование ``Token``-аккаунта (доступный баланс).

Все публичные адреса — строки base58, конвертация выполняется внутри.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import base58
from solders.keypair import Keypair
from solders.pubkey import Pubkey

from config.settings import settings
from utils.constants import ASSOCIATED_TOKEN_PROGRAM_ID, TOKEN_PROGRAM_ID

#: Длина сериализованного Mint-аккаунта (SPL Token).
MINT_ACCOUNT_SIZE: int = 82

#: Смещение поля ``amount`` в Token-аккаунте.
_TOKEN_AMOUNT_OFFSET: int = 64

#: Смещение поля ``decimals`` в Mint-аккаунте.
_MINT_DECIMALS_OFFSET: int = 44

#: Смещение COption<mint_authority> в Mint-аккаунте.
_MINT_AUTHORITY_OFFSET: int = 0

#: Смещение COption<freeze_authority> в Mint-аккаунте.
_MINT_FREEZE_OFFSET: int = 46

#: Размер сериализованного COption<Pubkey> (4 байта тега + 32 байта ключа).
_COPTION_ACCOUNT_SIZE: int = 36


class SplError(RuntimeError):
    """Ошибка при работе с SPL-данными."""


@dataclass(frozen=True, slots=True)
class MintInfo:
    """Разобранные данные Mint-аккаунта."""

    mint_authority: str | None
    freeze_authority: str | None
    decimals: int
    supply: int


def parse_pubkey(value: str) -> Pubkey:
    """Парсит строку base58 в :class:`Pubkey`.

    Args:
        value: Адрес в base58.

    Returns:
        Объект :class:`solders.pubkey.Pubkey`.

    Raises:
        SplError: Если адрес некорректен.
    """
    try:
        return Pubkey.from_string(value)
    except (ValueError, TypeError) as exc:  # solders raises ValueError
        raise SplError(f"Некорректный pubkey: {value!r}") from exc


def load_keypair(
    private_key: str | None = None,
    keypair_path: str | Path | None = None,
) -> Keypair:
    """Загружает ключевую пару кошелька.

    Приоритет: явный ``private_key`` → аргумент ``keypair_path`` → настройки канала.
    Приватный ключ принимается как base58-строка (64 байта) либо как JSON-массив
    из файла Solana CLI (``[12, 34, ...]``).

    Args:
        private_key: base58-строка приватного ключа.
        keypair_path: Путь к JSON-файлу keypair.

    Returns:
        :class:`solders.keypair.Keypair`.

    Raises:
        SplError: Если ключ не задан или некорректен.
    """
    resolved_key = (private_key or settings.wallet_private_key or "").strip()
    if resolved_key:
        return _keypair_from_base58(resolved_key)

    resolved_path = keypair_path or settings.wallet_keypair_path
    path = Path(resolved_path)
    if not path.exists():
        raise SplError(
            "Приватный ключ не задан: заполните WALLET_PRIVATE_KEY или "
            f"создайте файл keypair по пути {path}"
        )
    return _keypair_from_json_file(path)


def _keypair_from_base58(value: str) -> Keypair:
    """Строит keypair из base58-строки.

    Args:
        value: base58 приватного ключа (или сессионного секрета).

    Returns:
        :class:`solders.keypair.Keypair`.

    Raises:
        SplError: При ошибке декодирования.
    """
    try:
        raw = base58.b58decode(value)
    except (ValueError, TypeError) as exc:
        raise SplError("Некорректный base58 в приватном ключе") from exc
    try:
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except ValueError as exc:
        raise SplError("Не удалось создать keypair из приватного ключа") from exc
    raise SplError(
        f"Неверная длина приватного ключа: {len(raw)} байт (ожидается 32 или 64)"
    )


def _keypair_from_json_file(path: Path) -> Keypair:
    """Строит keypair из JSON-файла Solana CLI.

    Args:
        path: Путь к JSON-массиву байт.

    Returns:
        :class:`solders.keypair.Keypair`.

    Raises:
        SplError: При ошибке чтения/разбора.
    """
    try:
        raw = bytes(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError) as exc:
        raise SplError(f"Не удалось прочитать keypair из {path}") from exc
    try:
        if len(raw) == 64:
            return Keypair.from_bytes(raw)
        if len(raw) == 32:
            return Keypair.from_seed(raw)
    except ValueError as exc:
        raise SplError(f"Некорректный keypair в {path}") from exc
    raise SplError(f"Неверная длина keypair в {path}: {len(raw)} байт")


def derive_associated_token_address(
    owner: Pubkey | str,
    mint: Pubkey | str,
    token_program_id: Pubkey | str = TOKEN_PROGRAM_ID,
) -> Pubkey:
    """Выводит associated token address (ATA).

    Args:
        owner: Владелец токен-аккаунта.
        mint: Mint токена.
        token_program_id: Программа токена (SPL Token или Token-2022).

    Returns:
        :class:`Pubkey` associated token account.

    Raises:
        SplError: Если не удалось вывести адрес.
    """
    owner_pk = owner if isinstance(owner, Pubkey) else parse_pubkey(owner)
    mint_pk = mint if isinstance(mint, Pubkey) else parse_pubkey(mint)
    token_pk = (
        token_program_id
        if isinstance(token_program_id, Pubkey)
        else parse_pubkey(token_program_id)
    )
    ata_program = parse_pubkey(ASSOCIATED_TOKEN_PROGRAM_ID)
    seeds = [bytes(owner_pk), bytes(token_pk), bytes(mint_pk)]
    try:
        address, _bump = Pubkey.find_program_address(seeds, ata_program)
    except ValueError as exc:
        raise SplError("Не удалось вывести ATA") from exc
    return address


def decode_mint(data: bytes) -> MintInfo:
    """Разбирает данные Mint-аккаунта SPL Token.

    Args:
        data: Сырые байты аккаунта.

    Returns:
        :class:`MintInfo` с authorities, decimals и supply.

    Raises:
        SplError: Если размер данных меньше минимального.
    """
    if len(data) < MINT_ACCOUNT_SIZE:
        raise SplError(
            f"Данные Mint короче ожидаемых: {len(data)} < {MINT_ACCOUNT_SIZE}"
        )

    mint_authority = _decode_coption_pubkey(data, _MINT_AUTHORITY_OFFSET)
    freeze_authority = _decode_coption_pubkey(data, _MINT_FREEZE_OFFSET)
    decimals = data[_MINT_DECIMALS_OFFSET]
    supply = int.from_bytes(data[36:44], byteorder="little", signed=False)
    return MintInfo(
        mint_authority=mint_authority,
        freeze_authority=freeze_authority,
        decimals=decimals,
        supply=supply,
    )


def decode_token_amount(data: bytes) -> int:
    """Извлекает доступный баланс из данных Token-аккаунта.

    Args:
        data: Сырые байты token-аккаунта SPL Token.

    Returns:
        Доступный объём токенов (base units).

    Raises:
        SplError: Если данных недостаточно.
    """
    end = _TOKEN_AMOUNT_OFFSET + 8
    if len(data) < end:
        raise SplError(f"Данные Token короче ожидаемых: {len(data)} < {end}")
    return int.from_bytes(
        data[_TOKEN_AMOUNT_OFFSET:end], byteorder="little", signed=False
    )


def _decode_coption_pubkey(data: bytes, offset: int) -> str | None:
    """Разбирает COption<Pubkey> по смещению.

    Args:
        data: Сырые байты аккаунта.
        offset: Смещение начала COption.

    Returns:
        Адрес в base58 или ``None``, если слот пуст.
    """
    if len(data) < offset + _COPTION_ACCOUNT_SIZE:
        return None
    tag = int.from_bytes(data[offset : offset + 4], byteorder="little")
    if tag != 1:
        return None
    key_bytes = data[offset + 4 : offset + _COPTION_ACCOUNT_SIZE]
    return str(Pubkey.from_bytes(key_bytes))
