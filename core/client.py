"""Асинхронный транспорт Solana: HTTP JSON-RPC и WebSocket.

Модуль предоставляет два независимых клиента:

* :class:`SolanaRpcClient` — HTTP JSON-RPC поверх ``aiohttp`` с постоянной
  сессией, ретраями с экспоненциальной задержкой и измерением латентности;
* :class:`SolanaWsClient` — WebSocket-подписки поверх ``websockets`` с
  авто-реконнектом (exponential backoff) и повторной установкой подписок.

Оба клиента не знают о доменной логике: они отдают «сырые» сетевые данные,
которые парсятся в слоях :mod:`core.listener` и :mod:`core.executor`.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self

import aiohttp
from websockets.asyncio.client import connect as _ws_connect
from websockets.exceptions import ConnectionClosed

from config.settings import settings
from utils.latency import LatencyRegistry, LatencyStats, now_us
from utils.logger import get_logger

logger = get_logger()

#: Базовый путь Jito Block Engine API.
_JSONRPC_HEADERS: dict[str, str] = {"Content-Type": "application/json"}

#: HTTP-статусы, при которых запрос имеет смысл повторить.
_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504, 522, 524})

#: JSON-RPC коды, при которых запрос имеет смысл повторить.
_RETRYABLE_RPC_CODES: frozenset[int] = frozenset({-32005, -32603})


class RpcError(RuntimeError):
    """Ошибка HTTP JSON-RPC (не сетевой сбой, а отказ узла)."""

    def __init__(self, message: str, *, code: int | None = None, data: Any = None) -> None:
        """Создаёт ошибку.

        Args:
            message: Текст ошибки.
            code: JSON-RPC код (если есть).
            data: Дополнительные данные (если есть).
        """
        super().__init__(message)
        self.code = code
        self.data = data


@dataclass(frozen=True, slots=True)
class AccountState:
    """Состояние аккаунта в сети."""

    lamports: int
    owner: str
    executable: bool
    rent_epoch: int
    data: bytes


@dataclass(frozen=True, slots=True)
class BlockhashInfo:
    """Свежий blockhash и граница его валидности."""

    blockhash: str
    last_valid_block_height: int


@dataclass(frozen=True, slots=True)
class SignatureStatus:
    """Статус подтверждения транзакции."""

    signature: str
    slot: int | None
    err: Any | None
    confirmation_status: str | None

    @property
    def is_confirmed(self) -> bool:
        """Транзакция подтверждена (confirmed/finalized) без ошибки."""
        return (
            self.err is None
            and self.confirmation_status in {"confirmed", "finalized"}
        )


def _parse_account_value(value: Any) -> AccountState | None:
    """Разбирает ``value`` из ответа ``getAccountInfo``.

    Args:
        value: Секция ``result.value`` (dict или None).

    Returns:
        :class:`AccountState` или ``None``, если аккаунт не существует.
    """
    if not isinstance(value, dict):
        return None
    data_field = value.get("data")
    raw_data: bytes = b""
    if isinstance(data_field, list) and data_field:
        raw_data = base64.b64decode(str(data_field[0]))
    elif isinstance(data_field, str):
        raw_data = base64.b64decode(data_field)
    return AccountState(
        lamports=int(value.get("lamports", 0)),
        owner=str(value.get("owner", "")),
        executable=bool(value.get("executable", False)),
        rent_epoch=int(value.get("rentEpoch", 0)),
        data=raw_data,
    )


class SolanaRpcClient:
    """Асинхронный HTTP JSON-RPC клиент Solana.

    Держит одну ``aiohttp.ClientSession`` с keep-alive на всё время работы,
    что снижает накладные расходы на TLS-хендшейк. Все вызовы инструментируются
    метриками латентности и автоматически повторяются при временных сбоях.
    """

    def __init__(
        self,
        http_url: str | None = None,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
        latency: LatencyRegistry | None = None,
    ) -> None:
        """Инициализирует клиент.

        Args:
            http_url: URL HTTP RPC (по умолчанию из настроек/Helius).
            timeout_seconds: Таймаут запроса, c.
            max_retries: Число повторов при временных сбоях.
            latency: Реестр метрик латентности.
        """
        self._url = http_url or settings.http_rpc_url
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_seconds or settings.rpc_timeout_seconds
        )
        self._max_retries = (
            max_retries if max_retries is not None else settings.rpc_max_retries
        )
        self._latency = latency or LatencyRegistry()
        self._rpc_stats: LatencyStats = self._latency.get("rpc")
        self._session: aiohttp.ClientSession | None = None
        self._request_id: int = 0
        self._closed: bool = False

    @property
    def url(self) -> str:
        """HTTP RPC endpoint клиента."""
        return self._url

    async def start(self) -> None:
        """Создаёт HTTP-сессию. Идемпотентно."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers=_JSONRPC_HEADERS,
                connector=aiohttp.TCPConnector(
                    limit=64, ttl_dns_cache=300, enable_cleanup_closed=True
                ),
            )
            self._closed = False

    async def close(self) -> None:
        """Закрывает HTTP-сессию."""
        self._closed = True
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> Self:
        """Вход в асинхронный контекст — старт сессии."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста — закрытие сессии."""
        await self.close()

    def _next_id(self) -> int:
        """Возвращает следующий id JSON-RPC-запроса.

        Returns:
            Инкрементируемый целочисленный идентификатор.
        """
        self._request_id += 1
        return self._request_id

    async def _call(
        self,
        method: str,
        params: list[Any] | None = None,
        *,
        retries: int | None = None,
    ) -> Any:
        """Выполняет JSON-RPC вызов с ретраями и учётом латентности.

        Args:
            method: Имя RPC-метода.
            params: Список параметров метода.
            retries: Переопределение числа повторов.

        Returns:
            Значение ``result`` из ответа.

        Raises:
            RpcError: При отказе узла (после исчерпания повторов).
            aiohttp.ClientError: При неустранимом сетевом сбое.
        """
        if self._session is None:
            await self.start()
        assert self._session is not None

        attempts = self._max_retries if retries is None else retries
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params or [],
        }
        last_error: Exception | None = None

        for attempt in range(attempts + 1):
            start = now_us()
            try:
                async with self._session.post(self._url, json=payload) as response:
                    body = await response.text()
                    self._rpc_stats.observe_us(now_us() - start)
                    if response.status in _RETRYABLE_STATUS:
                        last_error = RpcError(
                            f"HTTP {response.status} для {method}", code=response.status
                        )
                        await self._sleep_backoff(attempt, response.status)
                        continue
                    if response.status >= 400:
                        raise RpcError(
                            f"HTTP {response.status} на {method}: {body[:256]}",
                            code=response.status,
                        )
                    parsed = json.loads(body)
            except (TimeoutError, aiohttp.ClientError) as exc:
                self._rpc_stats.observe_us(now_us() - start)
                last_error = exc
                await self._sleep_backoff(attempt)
                continue

            error = parsed.get("error")
            if error is not None:
                code = int(error.get("code", 0))
                if code in _RETRYABLE_RPC_CODES and attempt < attempts:
                    last_error = RpcError(
                        str(error.get("message", "rpc error")),
                        code=code,
                        data=error.get("data"),
                    )
                    await self._sleep_backoff(attempt)
                    continue
                raise RpcError(
                    str(error.get("message", "rpc error")),
                    code=code,
                    data=error.get("data"),
                )
            return parsed.get("result")

        if last_error is not None:
            raise last_error
        raise RpcError(f"Не удалось выполнить {method} после {attempts + 1} попыток")

    async def _sleep_backoff(self, attempt: int, status: int | None = None) -> None:
        """Выдерживает экспоненциальную паузу между повторами.

        Args:
            attempt: Номер текущей попытки (с нуля).
            status: HTTP-статус, если пауза вызвана им.
        """
        base = 0.15 if status == 429 else 0.08
        delay = min(base * (2**attempt), 2.0)
        await asyncio.sleep(delay)

    # ------------------------------------------------------------------ Методы
    async def get_health(self) -> str:
        """Проверяет доступность узла.

        Returns:
            Строка состояния узла (обычно ``"ok"``).
        """
        return str(await self._call("getHealth"))

    async def get_slot(self, commitment: str = "processed") -> int:
        """Возвращает текущий слот.

        Args:
            commitment: Уровень подтверждения.

        Returns:
            Номер слота.
        """
        return int(await self._call("getSlot", [{"commitment": commitment}]))

    async def get_block_height(self, commitment: str = "processed") -> int:
        """Возвращает текущую высоту блока.

        Args:
            commitment: Уровень подтверждения.

        Returns:
            Высота блока.
        """
        return int(await self._call("getBlockHeight", [{"commitment": commitment}]))

    async def get_balance(self, pubkey: str, commitment: str = "processed") -> int:
        """Возвращает баланс аккаунта в лампортах.

        Args:
            pubkey: Адрес аккаунта.
            commitment: Уровень подтверждения.

        Returns:
            Баланс в лампортах.
        """
        result = await self._call(
            "getBalance", [pubkey, {"commitment": commitment}]
        )
        return int(result.get("value", 0))

    async def get_account_info(
        self, pubkey: str, commitment: str = "processed"
    ) -> AccountState | None:
        """Возвращает состояние аккаунта.

        Args:
            pubkey: Адрес аккаунта.
            commitment: Уровень подтверждения.

        Returns:
            :class:`AccountState` или ``None``, если аккаунт не найден.
        """
        result = await self._call(
            "getAccountInfo",
            [pubkey, {"encoding": "base64", "commitment": commitment}],
        )
        return _parse_account_value(result.get("value") if result else None)

    async def get_multiple_accounts(
        self, pubkeys: list[str], commitment: str = "processed"
    ) -> list[AccountState | None]:
        """Возвращает состояния нескольких аккаунтов одним запросом.

        Args:
            pubkeys: Список адресов.
            commitment: Уровень подтверждения.

        Returns:
            Список :class:`AccountState` (``None`` для отсутствующих).
        """
        if not pubkeys:
            return []
        result = await self._call(
            "getMultipleAccounts",
            [pubkeys, {"encoding": "base64", "commitment": commitment}],
        )
        values = result.get("value", []) if result else []
        return [_parse_account_value(item) for item in values]

    async def get_latest_blockhash(
        self, commitment: str = "processed"
    ) -> BlockhashInfo:
        """Возвращает свежий blockhash.

        Args:
            commitment: Уровень подтверждения.

        Returns:
            :class:`BlockhashInfo` с hash и границей валидности.
        """
        result = await self._call(
            "getLatestBlockhash", [{"commitment": commitment}]
        )
        value = result.get("value", {}) if result else {}
        return BlockhashInfo(
            blockhash=str(value.get("blockhash", "")),
            last_valid_block_height=int(value.get("lastValidBlockHeight", 0)),
        )

    async def get_signature_statuses(
        self, signatures: list[str]
    ) -> list[SignatureStatus | None]:
        """Проверяет статусы подтверждения подписей.

        Args:
            signatures: Список подписей транзакций.

        Returns:
            Список :class:`SignatureStatus` (``None`` для неизвестных).
        """
        if not signatures:
            return []
        result = await self._call(
            "getSignatureStatuses",
            [signatures, {"searchTransactionHistory": False}],
        )
        values = result.get("value", []) if result else []
        statuses: list[SignatureStatus | None] = []
        for index, item in enumerate(values):
            if not isinstance(item, dict):
                statuses.append(None)
                continue
            statuses.append(
                SignatureStatus(
                    signature=signatures[index],
                    slot=item.get("slot"),
                    err=item.get("err"),
                    confirmation_status=item.get("confirmationStatus"),
                )
            )
        return statuses

    async def get_signatures_for_address(
        self,
        address: str,
        *,
        limit: int = 500,
        commitment: str = "confirmed",
    ) -> list[dict[str, Any]]:
        """Возвращает историю подписей адреса (новые впереди).

        Args:
            address: Адрес аккаунта (например, bonding curve).
            limit: Максимум записей (лимит узла — 1000).
            commitment: Уровень подтверждения.

        Returns:
            Список записей ``{signature, slot, err, blockTime, ...}``.

        Raises:
            RpcError: При ошибке запроса.
        """
        result = await self._call(
            "getSignaturesForAddress",
            [address, {"limit": limit, "commitment": commitment}],
        )
        return [item for item in (result or []) if isinstance(item, dict)]

    async def send_transaction(
        self,
        raw_transaction: bytes,
        *,
        skip_preflight: bool = True,
        max_retries: int = 0,
        commitment: str = "processed",
    ) -> str:
        """Отправляет подписанную транзакцию.

        Args:
            raw_transaction: Сериализованная подписанная транзакция.
            skip_preflight: Пропускать preflight-симуляцию (быстрее).
            max_retries: Число ретраев на стороне RPC (обычно 0 при Jito).
            commitment: Уровень подтверждения для preflight.

        Returns:
            Подпись транзакции (base58).

        Raises:
            RpcError: При отказе отправки.
        """
        encoded = base64.b64encode(raw_transaction).decode("ascii")
        config = {
            "encoding": "base64",
            "skipPreflight": skip_preflight,
            "maxRetries": max_retries,
            "preflightCommitment": commitment,
        }
        return str(await self._call("sendTransaction", [encoded, config]))

    async def simulate_transaction(
        self, raw_transaction: bytes, *, sig_verify: bool = False
    ) -> dict[str, Any]:
        """Симулирует транзакцию без отправки.

        Args:
            raw_transaction: Сериализованная транзакция.
            sig_verify: Проверять подписи при симуляции.

        Returns:
            Словарь ``result`` с полями ``value`` (logs, err, unitsConsumed).
        """
        encoded = base64.b64encode(raw_transaction).decode("ascii")
        config = {
            "encoding": "base64",
            "sigVerify": sig_verify,
            "commitment": "processed",
        }
        result = await self._call("simulateTransaction", [encoded, config])
        return result if isinstance(result, dict) else {}

    async def get_program_accounts(
        self,
        program_id: str,
        *,
        data_size: int | None = None,
        memcmp: tuple[int, str] | None = None,
        commitment: str = "processed",
    ) -> list[tuple[str, AccountState]]:
        """Возвращает аккаунты программы, отфильтрованные на стороне узла.

        Используется для поиска AMM-пула по mint токена после миграции: фильтр
        ``memcmp`` по смещению ``base_mint`` возвращает ровно один аккаунт, что
        на порядок дешевле полного перебора пулов.

        Args:
            program_id: Адрес программы-владельца аккаунтов.
            data_size: Требуемый размер данных аккаунта (``None`` — без фильтра).
            memcmp: Пара ``(смещение, base58-строка)`` для точного совпадения
                байтов аккаунта.
            commitment: Уровень подтверждения.

        Returns:
            Список пар ``(адрес аккаунта, состояние)``.

        Raises:
            RpcError: При отказе узла.
        """
        filters: list[dict[str, Any]] = []
        if data_size is not None:
            filters.append({"dataSize": data_size})
        if memcmp is not None:
            offset, encoded = memcmp
            filters.append({"memcmp": {"offset": offset, "bytes": encoded}})
        config: dict[str, Any] = {
            "encoding": "base64",
            "commitment": commitment,
        }
        if filters:
            config["filters"] = filters
        result = await self._call("getProgramAccounts", [program_id, config])
        if not isinstance(result, list):
            return []
        accounts: list[tuple[str, AccountState]] = []
        for item in result:
            if not isinstance(item, dict):
                continue
            address = str(item.get("pubkey", ""))
            state = _parse_account_value(item.get("account"))
            if address and state is not None:
                accounts.append((address, state))
        return accounts

    async def get_token_account_balance(self, token_account: str) -> int:
        """Возвращает баланс token-аккаунта в base units.

        Args:
            token_account: Адрес token-аккаунта.

        Returns:
            Баланс в base units (0 при ошибке формата).
        """
        result = await self._call(
            "getTokenAccountBalance", [token_account, {"commitment": "processed"}]
        )
        if not isinstance(result, dict):
            return 0
        value = result.get("value", {})
        return int(value.get("amount", 0))

    async def get_token_supply(self, mint: str) -> int:
        """Возвращает общий объём (supply) mint в base units.

        Args:
            mint: Адрес mint.

        Returns:
            Общий объём в base units (0 при ошибке формата).
        """
        result = await self._call(
            "getTokenSupply", [mint, {"commitment": "processed"}]
        )
        if not isinstance(result, dict):
            return 0
        value = result.get("value", {})
        return int(value.get("amount", 0))

    async def get_token_largest_accounts(
        self, mint: str, *, commitment: str = "confirmed"
    ) -> list[str]:
        """Возвращает адреса крупнейших token-аккаунтов mint.

        Args:
            mint: Адрес mint.
            commitment: Уровень подтверждения.

        Returns:
            Список адресов token-аккаунтов (пустой при ошибке формата).
        """
        result = await self._call(
            "getTokenLargestAccounts", [mint, {"commitment": commitment}]
        )
        if not isinstance(result, dict):
            return []
        values = result.get("value", [])
        if not isinstance(values, list):
            return []
        return [
            str(item["address"])
            for item in values
            if isinstance(item, dict) and item.get("address")
        ]

    async def get_transaction(
        self,
        signature: str,
        *,
        commitment: str = "confirmed",
        max_supported_version: int = 0,
    ) -> dict[str, Any] | None:
        """Возвращает разобранную транзакцию по подписи.

        Args:
            signature: Подпись транзакции.
            commitment: Уровень подтверждения.
            max_supported_version: Максимальная поддерживаемая версия
                транзакции (0 = только legacy).

        Returns:
            Словарь транзакции (``jsonParsed``) или ``None``, если не найдена.

        Raises:
            RpcError: При отказе узла.
        """
        result = await self._call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": commitment,
                    "maxSupportedTransactionVersion": max_supported_version,
                },
            ],
        )
        return result if isinstance(result, dict) else None
# --------------------------------------------------------------------------- #
#  WebSocket
# --------------------------------------------------------------------------- #
class WsSubscriptionKind(StrEnum):
    """Типы поддерживаемых WS-подписок."""

    ACCOUNT = "accountSubscribe"
    LOGS = "logsSubscribe"
    SLOT = "slotSubscribe"
    SIGNATURE = "signatureSubscribe"
    ROOT = "rootSubscribe"


#: Тип колбэка для WS-уведомлений.
WsCallback = Callable[[dict[str, Any]], Awaitable[None] | None]

#: Методы-уведомления, которые доставляются как ``<kind>Notification``.
_NOTIFICATION_KINDS: frozenset[str] = frozenset(
    {
        "accountNotification",
        "logsNotification",
        "slotNotification",
        "signatureNotification",
        "rootNotification",
    }
)


@dataclass(slots=True)
class _Subscription:
    """Запись активной WS-подписки."""

    kind: WsSubscriptionKind
    params: list[Any]
    callback: WsCallback
    server_id: int | None = None
    active: bool = False
    latency_name: str = "ws"


class SolanaWsClient:
    """Асинхронный WebSocket-клиент Solana с авто-реконнектом.

    Поддерживает произвольное число подписок (логи программ, изменения
    аккаунтов, слоты). При разрыве связи переподключается с экспоненциальной
    задержкой и автоматически восстанавливает все активные подписки.

    Колбэки могут быть синхронными или асинхронными; исключения в них
    логируются и не роняют reader-loop.
    """

    def __init__(
        self,
        ws_url: str | None = None,
        *,
        ping_interval: float = 20.0,
        ping_timeout: float = 20.0,
        close_timeout: float = 5.0,
        max_size: int | None = None,
        latency: LatencyRegistry | None = None,
        request_timeout: float = 10.0,
    ) -> None:
        """Инициализирует WS-клиент.

        Args:
            ws_url: URL WebSocket RPC (по умолчанию из настроек/Helius).
            ping_interval: Интервал keepalive-пингов, c.
            ping_timeout: Таймаут ожидания pong, c.
            close_timeout: Таймаут закрытия соединения, c.
            max_size: Максимальный размер входящего сообщения (None = без лимита).
            latency: Реестр метрик латентности.
            request_timeout: Таймаут ожидания ack на запрос, c.
        """
        self._url = ws_url or settings.ws_rpc_url
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._close_timeout = close_timeout
        self._max_size = max_size
        self._request_timeout = request_timeout
        self._latency = latency or LatencyRegistry()

        self._ws: Any | None = None
        self._task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()
        # Диагностика качества соединения: уведомления с момента подключения.
        self._conn_notifications = 0
        self._closed = False

        self._request_id = 0
        self._token = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._records: dict[int, _Subscription] = {}
        self._by_server_id: dict[int, _Subscription] = {}

    @property
    def url(self) -> str:
        """WS endpoint клиента."""
        return self._url

    @property
    def notifications_since_connect(self) -> int:
        """Уведомлений получено с момента текущего подключения."""
        return self._conn_notifications

    @property
    def is_connected(self) -> bool:
        """Открыто ли сейчас соединение."""
        return self._connected.is_set()

    async def start(self) -> None:
        """Запускает фоновый loop подключения."""
        if self._task is None or self._task.done():
            self._closed = False
            self._task = asyncio.create_task(self._run_forever(), name="ws-client")

    async def close(self) -> None:
        """Останавливает клиент и закрывает соединение."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self._close_socket()
        self._fail_pending(RpcError("WS-клиент остановлен"))

    async def __aenter__(self) -> Self:
        """Вход в контекст — запуск клиента."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста — остановка клиента."""
        await self.close()

    async def wait_connected(self, timeout: float = 15.0) -> bool:
        """Ожидает установления соединения.

        Args:
            timeout: Максимальное ожидание, c.

        Returns:
            ``True``, если соединение установлено.
        """
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True

    # ------------------------------------------------------------ Подписки
    async def subscribe(
        self,
        kind: WsSubscriptionKind,
        params: list[Any],
        callback: WsCallback,
        *,
        latency_name: str = "ws",
    ) -> int:
        """Регистрирует подписку.

        Args:
            kind: Тип подписки.
            params: Параметры метода подписки.
            callback: Колбэк, вызываемый на каждое уведомление.
            latency_name: Имя метрики латентности доставки.

        Returns:
            Локальный токен подписки (для отписки).
        """
        self._token += 1
        token = self._token
        record = _Subscription(
            kind=kind, params=params, callback=callback, latency_name=latency_name
        )
        self._records[token] = record
        if self.is_connected:
            await self._issue_subscription(record)
        return token

    async def subscribe_logs(
        self,
        program_id: str,
        callback: WsCallback,
        *,
        commitment: str = "processed",
    ) -> int:
        """Подписывается на логи программы.

        Args:
            program_id: Адрес программы (например, Pump.fun/Raydium).
            callback: Колбэк уведомлений.
            commitment: Уровень подтверждения.

        Returns:
            Локальный токен подписки.
        """
        params: list[Any] = [
            {"mentions": [program_id]},
            {"commitment": commitment},
        ]
        return await self.subscribe(
            WsSubscriptionKind.LOGS, params, callback, latency_name="ws_logs"
        )

    async def subscribe_account(
        self,
        pubkey: str,
        callback: WsCallback,
        *,
        commitment: str = "processed",
    ) -> int:
        """Подписывается на изменения аккаунта.

        Args:
            pubkey: Адрес аккаунта (например, пула).
            callback: Колбэк уведомлений.
            commitment: Уровень подтверждения.

        Returns:
            Локальный токен подписки.
        """
        params: list[Any] = [
            pubkey,
            {"encoding": "base64", "commitment": commitment},
        ]
        return await self.subscribe(
            WsSubscriptionKind.ACCOUNT, params, callback, latency_name="ws_account"
        )

    async def subscribe_slot(self, callback: WsCallback) -> int:
        """Подписывается на уведомления о новых слотах.

        Args:
            callback: Колбэк уведомлений.

        Returns:
            Локальный токен подписки.
        """
        return await self.subscribe(
            WsSubscriptionKind.SLOT, [], callback, latency_name="ws_slot"
        )

    async def unsubscribe(self, token: int) -> None:
        """Отменяет подписку по локальному токену.

        Args:
            token: Токен, возвращённый :meth:`subscribe`.
        """
        record = self._records.pop(token, None)
        if record is None:
            return
        if record.server_id is not None:
            self._by_server_id.pop(record.server_id, None)
            if self.is_connected:
                method = record.kind.value.replace("Subscribe", "Unsubscribe")
                await self._send_raw(
                    {"jsonrpc": "2.0", "id": self._next_request_id(), "method": method,
                     "params": [record.server_id]}
                )

    async def request(self, method: str, params: list[Any] | None = None) -> Any:
        """Выполняет одиночный JSON-RPC вызов через WS-соединение.

        Args:
            method: Имя метода.
            params: Параметры метода.

        Returns:
            Значение ``result``.

        Raises:
            RpcError: Если соединение недоступно или узел вернул ошибку.
        """
        if not self.is_connected:
            raise RpcError("WS-соединение не установлено")
        request_id = self._next_request_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future
        await self._send_raw(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or []}
        )
        try:
            return await asyncio.wait_for(future, timeout=self._request_timeout)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            raise RpcError(f"Таймаут WS-запроса {method}") from exc

    # --------------------------------------------------------------- Внутреннее
    def _next_request_id(self) -> int:
        """Возвращает следующий id WS-запроса.

        Returns:
            Идентификатор запроса.
        """
        self._request_id += 1
        return self._request_id

    async def _run_forever(self) -> None:
        """Основной цикл переподключений с экспоненциальным backoff."""
        backoff = settings.ws_initial_reconnect_delay
        max_backoff = settings.ws_max_reconnect_delay
        while not self._closed:
            try:
                async with _ws_connect(
                    self._url,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                    close_timeout=self._close_timeout,
                    max_size=self._max_size,
                    max_queue=4096,
                ) as socket:
                    self._ws = socket
                    backoff = settings.ws_initial_reconnect_delay
                    self._conn_notifications = 0
                    logger.info("WS подключён: {}", self._url)
                    # КРИТИЧНО: читатель стартует ДО переподписки. Иначе
                    # ответы сервера на subscribe никто не читает — каждый
                    # запрос падает по таймауту, подписки не восстанавливаются
                    # (поток событий умирает после первого реконнекта), а
                    # keepalive-ping также остаётся без ответа (разрыв).
                    reader_task = asyncio.create_task(self._reader(socket))
                    try:
                        await self._resubscribe_all()
                        self._connected.set()
                        await reader_task
                    finally:
                        if not reader_task.done():
                            reader_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await reader_task
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError) as exc:
                logger.warning("WS разрыв: {}", exc)
            except Exception as exc:
                logger.opt(exception=exc).error("WS ошибка: {}", exc)
            finally:
                was_connected = self._connected.is_set()
                if was_connected and self._conn_notifications == 0:
                    logger.warning(
                        "WS: соединение разорвалось до первого уведомления "
                        "({} подписок восстановлено) — сервер не присылал "
                        "события; возможен лимит/прокси",
                        len(self._records),
                    )
                self._connected.clear()
                self._ws = None
                self._fail_pending(RpcError("WS-соединение потеряно"))

            if self._closed:
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def _reader(self, socket: Any) -> None:
        """Читает и диспетчеризует входящие сообщения.

        Args:
            socket: Активное WS-соединение.
        """
        async for raw in socket:
            try:
                message = json.loads(raw)
            except (TypeError, ValueError):
                logger.debug("WS: пропущено неразборчивое сообщение")
                continue
            await self._dispatch(message)

    async def _dispatch(self, message: dict[str, Any]) -> None:
        """Маршрутизирует одно входящее сообщение.

        Args:
            message: Разобранный JSON-объект.
        """
        message_id = message.get("id")
        if message_id is not None and message_id in self._pending:
            future = self._pending.pop(message_id)
            if future.done():
                return
            error = message.get("error")
            if error is not None:
                future.set_exception(
                    RpcError(str(error.get("message", "rpc error")),
                             code=int(error.get("code", 0)))
                )
            else:
                future.set_result(message.get("result"))
            return

        method = message.get("method")
        if method not in _NOTIFICATION_KINDS:
            return
        self._conn_notifications += 1
        params = message.get("params") or {}
        server_id = params.get("subscription")
        if server_id is None:
            return
        record = self._by_server_id.get(int(server_id))
        if record is None:
            return
        await self._invoke(record, params.get("result"))

    async def _invoke(self, record: _Subscription, result: Any) -> None:
        """Вызывает колбэк подписки, изолируя исключения.

        Args:
            record: Запись подписки.
            result: Полезная нагрузка уведомления.
        """
        start = now_us()
        payload = result if isinstance(result, dict) else {"value": result}
        try:
            outcome = record.callback(payload)
            if asyncio.iscoroutine(outcome):
                await outcome
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.opt(exception=exc).error(
                "WS callback ошибка ({}): {}", record.kind.value, exc
            )
        finally:
            self._latency.get(record.latency_name).observe_us(now_us() - start)

    async def _resubscribe_all(self) -> None:
        """Восстанавливает все ранее активные подписки после реконнекта."""
        for record in self._records.values():
            # После реконнекта сервер не помнит НИ ОДНОЙ подписки —
            # переподписываем все записи (в т.ч. неудавшиеся ранее).
            record.server_id = None
            record.active = False
            await self._issue_subscription(record)
            await asyncio.sleep(0.05)  # без шторма подписок при реконнекте

    async def _issue_subscription(self, record: _Subscription) -> None:
        """Отправляет запрос подписки и регистрирует server id.

        Args:
            record: Запись подписки.
        """
        request_id = self._next_request_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        # Таймаут-обработчик ниже НЕ удаляет future из _pending: при разрыве
        # _fail_pending выставит ему RpcError, и asyncio заспамит
        # «Future exception was never retrieved». Подавляем заранее.
        future.add_done_callback(lambda f: f.cancelled() or f.exception())
        self._pending[request_id] = future
        await self._send_raw(
            {"jsonrpc": "2.0", "id": request_id, "method": record.kind.value,
             "params": record.params}
        )
        try:
            server_id = await asyncio.wait_for(future, timeout=self._request_timeout)
        except RpcError as exc:
            self._pending.pop(request_id, None)
            logger.warning(
                "WS: подписка {} отклонена сервером: {}",
                record.kind.value,
                exc,
            )
            return
        except TimeoutError:
            # Ответа нет = соединение наполовину мертво (pong/push не идут).
            # Рвём сокет: реконнект-цикл переподключится и переподпишется.
            self._pending.pop(request_id, None)
            logger.warning(
                "WS: подписка {} не подтверждена за {} c — переподключение",
                record.kind.value,
                self._request_timeout,
            )
            await self._close_socket()
            raise RpcError(
                f"WS: подписка {record.kind.value} не подтверждена (таймаут)"
            ) from None
        record.server_id = int(server_id)
        record.active = True
        self._by_server_id[record.server_id] = record

    async def _send_raw(self, payload: dict[str, Any]) -> None:
        """Сериализует и отправляет JSON-объект в сокет.

        Args:
            payload: JSON-RPC объект.

        Raises:
            RpcError: Если соединение отсутствует.
        """
        socket = self._ws
        if socket is None:
            raise RpcError("WS-соединение недоступно")
        try:
            await socket.send(json.dumps(payload))
        except ConnectionClosed as exc:
            # Конвертируем в RpcError: все вызывающие (подписки, запросы)
            # уже умеют обрабатывать RpcError, а ConnectionClosed — нет.
            raise RpcError(f"WS-соединение потеряно: {exc}") from exc

    def _fail_pending(self, error: Exception) -> None:
        """Помечает все ожидающие запросы ошибкой.

        Args:
            error: Исключение, которым завершаются futures.
        """
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def _close_socket(self) -> None:
        """Аккуратно закрывает сокет, подавляя ошибки."""
        socket = self._ws
        self._ws = None
        if socket is not None:
            with contextlib.suppress(Exception):
                await socket.close()
