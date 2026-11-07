"""Клиент Jupiter Swap API v6 — fallback-маршрут для мигрировавших токенов.

Основной путь исполнения бота — прямые инструкции Pump.fun bonding curve
(:mod:`core.tx_builder`). Однако после миграции токена ликвидность уезжает на
PumpSwap / Raydium, где прямой AMM-роут требует учёта Serum/OpenBook-аккаунтов.
Для таких случаев модуль даёт агрегаторный fallback через Jupiter:

* :meth:`JupiterClient.get_quote` — получить котировку (``/quote``);
* :meth:`JupiterClient.get_swap_instructions` — получить **сырые** инструкции
  свапа (``/swap-instructions``), что позволяет добавить собственные Compute
  Budget, memo-антифронтран и Jito-tip и собрать единую v0-транзакцию;
* :meth:`JupiterClient.fetch_lookup_tables` — подтянуть address lookup tables,
  чтобы уложить маршрут в лимит размера транзакции.

Использование ``/swap-instructions`` (а не готового ``/swap``) принципиально:
только так мы сохраняем полный контроль над Jito-tip и анти-MEV защитой.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from typing import Any, Protocol, Self

import aiohttp
import base58
from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from config.settings import settings
from utils.latency import LatencyRegistry, now_us
from utils.logger import get_logger
from utils.spl import SplError, parse_pubkey

logger = get_logger()

#: Размер метаданных Address Lookup Table (до списка адресов).
LOOKUP_TABLE_META_SIZE: int = 56

#: Символы, отличающие base64-данные от base58.
_BASE64_MARKERS: tuple[str, ...] = ("+", "/", "=")

#: HTTP-статусы, при которых запрос имеет смысл повторить.
_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

#: Базовая задержка перед повтором запроса, c (растёт экспоненциально).
_RETRY_BASE_SECONDS: float = 0.15

#: Потолок задержки перед повтором запроса, c.
_RETRY_MAX_SECONDS: float = 1.2


class JupiterError(RuntimeError):
    """Ошибка взаимодействия с Jupiter Swap API."""


def _retry_backoff_seconds(attempt: int) -> float:
    """Возвращает экспоненциальную задержку перед повтором запроса.

    Args:
        attempt: Номер повторной попытки (1 — первый повтор).

    Returns:
        Задержка в секундах, ограниченная :data:`_RETRY_MAX_SECONDS`.
    """
    delay = _RETRY_BASE_SECONDS * 2.0 ** max(0, attempt - 1)
    return min(delay, _RETRY_MAX_SECONDS)


@dataclass(frozen=True, slots=True)
class JupiterQuote:
    """Котировка Jupiter, необходимая для сборки свапа."""

    raw: dict[str, Any]
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    price_impact_pct: float
    slippage_bps: int
    route_labels: tuple[str, ...] = ()

    @classmethod
    def from_response(
        cls, payload: dict[str, Any], *, slippage_bps: int
    ) -> JupiterQuote:
        """Строит котировку из JSON-ответа ``/quote``.

        Args:
            payload: Тело ответа Jupiter.
            slippage_bps: Запрошенное проскальзывание в базисных пунктах.

        Returns:
            :class:`JupiterQuote`.

        Raises:
            JupiterError: Если ответ не содержит обязательных полей.
        """
        try:
            plan = payload.get("routePlan", [])
            labels = tuple(
                str(step.get("swapInfo", {}).get("label", ""))
                for step in plan
                if isinstance(step, dict)
            )
            return cls(
                raw=payload,
                input_mint=str(payload["inputMint"]),
                output_mint=str(payload["outputMint"]),
                in_amount=int(payload["inAmount"]),
                out_amount=int(payload["outAmount"]),
                price_impact_pct=float(payload.get("priceImpactPct", 0.0)) * 100.0,
                slippage_bps=slippage_bps,
                route_labels=labels,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise JupiterError(f"Некорректный ответ /quote: {payload!r}") from exc


@dataclass(slots=True)
class JupiterSwapInstructions:
    """Набор сырых инструкций свапа, полученных от Jupiter."""

    setup_instructions: list[Instruction] = field(default_factory=list)
    swap_instruction: Instruction | None = None
    cleanup_instruction: Instruction | None = None
    other_instructions: list[Instruction] = field(default_factory=list)
    compute_budget_instructions: list[Instruction] = field(default_factory=list)
    address_lookup_table_addresses: list[str] = field(default_factory=list)

    def core_instructions(self) -> list[Instruction]:
        """Возвращает инструкции свапа без Compute Budget аггрегатора.

        Мы всегда используем собственные Compute Budget-инструкции, поэтому
        версии Jupiter отбрасываются, чтобы не дублировать приоритетную комиссию.

        Returns:
            Список инструкций в порядке исполнения.
        """
        ordered: list[Instruction] = []
        ordered.extend(self.setup_instructions)
        if self.swap_instruction is not None:
            ordered.append(self.swap_instruction)
        if self.cleanup_instruction is not None:
            ordered.append(self.cleanup_instruction)
        ordered.extend(self.other_instructions)
        return ordered


class JupiterClient:
    """Асинхронный клиент Jupiter Swap API v6.

    Держит собственную ``aiohttp``-сессию с keep-alive. Все запросы
    инструментируются метриками латентности и повторяются при временных
    сбоях узла агрегатора.
    """

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout_seconds: float | None = None,
        max_retries: int = 2,
        latency: LatencyRegistry | None = None,
    ) -> None:
        """Инициализирует клиент.

        Args:
            base_url: Базовый URL Jupiter API (по умолчанию из настроек).
            timeout_seconds: Таймаут запроса, c.
            max_retries: Число повторов при временных сбоях.
            latency: Реестр метрик латентности.
        """
        self._base_url = (base_url or settings.jupiter_api_url).rstrip("/")
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_seconds or settings.rpc_timeout_seconds
        )
        self._max_retries = max_retries
        self._latency = latency or LatencyRegistry()
        self._stats = self._latency.get("jupiter")
        self._session: aiohttp.ClientSession | None = None

    @property
    def base_url(self) -> str:
        """Базовый URL клиента."""
        return self._base_url

    async def start(self) -> None:
        """Создаёт HTTP-сессию. Идемпотентно."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(
                    limit=16, ttl_dns_cache=300, enable_cleanup_closed=True
                ),
            )

    async def close(self) -> None:
        """Закрывает HTTP-сессию."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> Self:
        """Вход в контекст — старт сессии."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста — закрытие сессии."""
        await self.close()

    # --------------------------------------------------------------- Внутреннее
    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Выполняет HTTP-запрос к Jupiter с ретраями.

        Args:
            method: HTTP-метод (``GET``/``POST``).
            path: Путь относительно базового URL.
            params: Query-параметры.
            body: JSON-тело запроса.

        Returns:
            Разобранный JSON-ответ.

        Raises:
            JupiterError: При сетевом сбое или некорректном ответе.
        """
        await self.start()
        assert self._session is not None
        url = f"{self._base_url}{path}"
        last_error: str = ""
        for attempt in range(self._max_retries + 1):
            if attempt > 0:
                await asyncio.sleep(_retry_backoff_seconds(attempt))
            start = now_us()
            try:
                async with self._session.request(
                    method, url, params=params, json=body
                ) as response:
                    text = await response.text()
                    self._stats.observe_us(now_us() - start)
                    if response.status in _RETRYABLE_STATUS:
                        last_error = f"HTTP {response.status}: {text[:200]}"
                        continue
                    if response.status >= 400:
                        raise JupiterError(
                            f"Jupiter {path} HTTP {response.status}: {text[:300]}"
                        )
                    parsed = json.loads(text)
                    return parsed if isinstance(parsed, dict) else {"value": parsed}
            except (aiohttp.ClientError, TimeoutError) as exc:
                self._stats.observe_us(now_us() - start)
                last_error = str(exc)
        raise JupiterError(f"Jupiter {path} недоступен: {last_error}")

    # ------------------------------------------------------------------- Quote
    async def get_quote(
        self,
        *,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int,
        restrict_intermediate_tokens: bool = True,
        only_direct_routes: bool = False,
    ) -> JupiterQuote:
        """Запрашивает котировку свапа.

        Args:
            input_mint: Mint входного токена.
            output_mint: Mint выходного токена.
            amount: Объём входа в base units.
            slippage_bps: Проскальзывание в базисных пунктах.
            restrict_intermediate_tokens: Запрещать лишние промежуточные токены.
            only_direct_routes: Использовать только прямые маршруты.

        Returns:
            :class:`JupiterQuote`.

        Raises:
            JupiterError: Если котировку получить не удалось.
        """
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
            "swapMode": "ExactIn",
            "restrictIntermediateTokens": str(
                restrict_intermediate_tokens
            ).lower(),
            "onlyDirectRoutes": str(only_direct_routes).lower(),
        }
        payload = await self._request("GET", "/quote", params=params)
        return JupiterQuote.from_response(payload, slippage_bps=slippage_bps)

    # ------------------------------------------------------- Swap Instructions
    async def get_swap_instructions(
        self,
        quote: JupiterQuote,
        *,
        user_public_key: str,
        wrap_and_unwrap_sol: bool = True,
        use_shared_accounts: bool = True,
    ) -> JupiterSwapInstructions:
        """Запрашивает сырые инструкции свапа.

        Args:
            quote: Ранее полученная котировка.
            user_public_key: Публичный ключ кошелька-исполнителя.
            wrap_and_unwrap_sol: Оборачивать/разворачивать SOL автоматически.
            use_shared_accounts: Использовать разделяемые аккаунты Jupiter.

        Returns:
            :class:`JupiterSwapInstructions`.

        Raises:
            JupiterError: Если инструкции получить не удалось.
        """
        body = {
            "quoteResponse": quote.raw,
            "userPublicKey": user_public_key,
            "wrapAndUnwrapSol": wrap_and_unwrap_sol,
            "useSharedAccounts": use_shared_accounts,
            "dynamicComputeUnitLimit": False,
        }
        payload = await self._request("POST", "/swap-instructions", body=body)
        return _parse_swap_instructions(payload)

    # ------------------------------------------------- Address Lookup Tables
    async def fetch_lookup_tables(
        self, table_addresses: list[str], rpc_account_reader: _LookupTableReader
    ) -> list[AddressLookupTableAccount]:
        """Загружает и разбирает address lookup tables.

        Args:
            table_addresses: Адреса lookup table.
            rpc_account_reader: Объект с методом ``get_multiple_accounts``.

        Returns:
            Список :class:`AddressLookupTableAccount` (только валидные).
        """
        if not table_addresses:
            return []
        states = await rpc_account_reader.get_multiple_accounts(table_addresses)
        tables: list[AddressLookupTableAccount] = []
        for address, state in zip(table_addresses, states, strict=False):
            if state is None:
                continue
            table = decode_lookup_table_account(address, state.data)
            if table is not None:
                tables.append(table)
        return tables


class _LookupTableReader(Protocol):
    """Структурный контракт читателя аккаунтов.

    Ему удовлетворяет :class:`~core.client.SolanaRpcClient`: jupiter.py не
    импортирует транспортный слой, чтобы не создавать циклическую зависимость.
    """

    async def get_multiple_accounts(
        self, pubkeys: list[str], commitment: str = "processed"
    ) -> list[Any]:
        """Читает состояния аккаунтов.

        Args:
            pubkeys: Адреса аккаунтов.
            commitment: Уровень подтверждения.

        Returns:
            Список состояний (``None`` для отсутствующих аккаунтов).
        """
        ...


# --------------------------------------------------------------------------- #
#  Разбор ответов Jupiter
# --------------------------------------------------------------------------- #
def decode_instruction(payload: dict[str, Any]) -> Instruction:
    """Собирает :class:`Instruction` из JSON-описания Jupiter.

    Args:
        payload: Словарь ``{programId, accounts, data}``.

    Returns:
        :class:`solders.instruction.Instruction`.

    Raises:
        JupiterError: Если структура некорректна.
    """
    try:
        program_id = parse_pubkey(str(payload["programId"]))
        raw_accounts = payload.get("accounts", [])
        accounts = [
            AccountMeta(
                pubkey=parse_pubkey(str(item["pubkey"])),
                is_signer=bool(item.get("isSigner", False)),
                is_writable=bool(item.get("isWritable", False)),
            )
            for item in raw_accounts
        ]
        data = _decode_instruction_data(str(payload.get("data", "")))
    except (KeyError, TypeError, SplError, ValueError) as exc:
        raise JupiterError(f"Некорректная инструкция Jupiter: {payload!r}") from exc
    return Instruction(program_id=program_id, data=data, accounts=accounts)


def _decode_instruction_data(encoded: str) -> bytes:
    """Декодирует данные инструкции (base58 или base64).

    Args:
        encoded: Строка данных из ответа Jupiter.

    Returns:
        Сырые байты инструкции.
    """
    if not encoded:
        return b""
    if any(marker in encoded for marker in _BASE64_MARKERS):
        return base64.b64decode(encoded)
    return base58.b58decode(encoded)


def _parse_instruction_list(items: Any) -> list[Instruction]:
    """Разбирает список инструкций из ответа Jupiter.

    Args:
        items: Список JSON-описаний инструкций (или ``None``).

    Returns:
        Список :class:`Instruction`.
    """
    if not isinstance(items, list):
        return []
    parsed: list[Instruction] = []
    for item in items:
        if isinstance(item, dict):
            parsed.append(decode_instruction(item))
    return parsed


def _parse_optional_instruction(item: Any) -> Instruction | None:
    """Разбирает одиночную (возможно отсутствующую) инструкцию.

    Args:
        item: JSON-описание инструкции или ``None``.

    Returns:
        :class:`Instruction` или ``None``.
    """
    if isinstance(item, dict):
        return decode_instruction(item)
    return None


def _parse_swap_instructions(payload: dict[str, Any]) -> JupiterSwapInstructions:
    """Разбирает ответ ``/swap-instructions``.

    Args:
        payload: Тело ответа Jupiter.

    Returns:
        :class:`JupiterSwapInstructions`.
    """
    table_addresses = payload.get("addressLookupTableAddresses", [])
    return JupiterSwapInstructions(
        setup_instructions=_parse_instruction_list(payload.get("setupInstructions")),
        swap_instruction=_parse_optional_instruction(payload.get("swapInstruction")),
        cleanup_instruction=_parse_optional_instruction(
            payload.get("cleanupInstruction")
        ),
        other_instructions=_parse_instruction_list(payload.get("otherInstructions")),
        compute_budget_instructions=_parse_instruction_list(
            payload.get("computeBudgetInstructions")
        ),
        address_lookup_table_addresses=[
            str(addr) for addr in table_addresses if isinstance(addr, str)
        ],
    )


def decode_lookup_table_account(
    address: str, data: bytes
) -> AddressLookupTableAccount | None:
    """Разбирает аккаунт Address Lookup Table в список адресов.

    Формат аккаунта: 56 байт метаданных, далее — последовательность
    32-байтовых адресов.

    Args:
        address: Адрес lookup table (он же ключ).
        data: Сырые байты аккаунта.

    Returns:
        :class:`AddressLookupTableAccount` или ``None``, если данных недостаточно.
    """
    if len(data) <= LOOKUP_TABLE_META_SIZE:
        return None
    try:
        key = parse_pubkey(address)
    except SplError:
        return None
    body = data[LOOKUP_TABLE_META_SIZE:]
    addresses = [
        Pubkey.from_bytes(body[offset : offset + 32])
        for offset in range(0, len(body) - 31, 32)
    ]
    return AddressLookupTableAccount(key=key, addresses=addresses)
