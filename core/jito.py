"""Клиент Jito Block Engine API: отправка бандлов и чаевые.

Все транзакции бота отправляются как **Jito Bundles** — это даёт защиту от
MEV/сэндвич-атак и гарантирует «атомарность» (бандл либо попадает в блок
целиком, либо не попадает вовсе). Модуль инкапсулирует JSON-RPC-взаимодействие
с Block Engine:

* :meth:`JitoClient.get_tip_accounts` — список аккаунтов для чаевых (кэш);
* :meth:`JitoClient.choose_tip_account` — равномерный выбор tip-аккаунта;
* :meth:`JitoClient.send_bundle` — отправка набора подписанных транзакций;
* :meth:`JitoClient.get_bundle_statuses` — опрос статуса бандла.

Транспорт — ``aiohttp`` с ретраями и экспоненциальной задержкой; сетевые
ошибки никогда не «проглатываются» молча, а поднимаются как
:class:`~core.jito.JitoError`.
"""

from __future__ import annotations

import asyncio
import base64
import json
import random
import time
from typing import Any, Self

import aiohttp

from config.settings import settings
from models.trade_data import JitoBundleStatus
from utils.constants import (
    JITO_BUNDLE_PATH,
    JITO_BUNDLE_STATUS_PATH,
    JITO_TIP_ACCOUNTS,
    JITO_TIP_ACCOUNTS_PATH,
)
from utils.latency import LatencyRegistry, now_us
from utils.logger import get_logger

logger = get_logger()

#: HTTP-статусы, при которых запрос имеет смысл повторить.
_RETRYABLE_STATUS: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

#: Кэш tip-аккаунтов живёт час, чтобы не дёргать Block Engine каждый ордер.
_TIP_CACHE_SECONDS: float = 3600.0

#: Базовая задержка перед повтором запроса, c (растёт экспоненциально).
_RETRY_BASE_SECONDS: float = 0.15

#: Потолок задержки перед повтором запроса, c.
_RETRY_MAX_SECONDS: float = 1.2


class JitoError(RuntimeError):
    """Ошибка взаимодействия с Jito Block Engine."""


def _retry_backoff_seconds(attempt: int) -> float:
    """Возвращает экспоненциальную задержку перед повтором запроса.

    Args:
        attempt: Номер повторной попытки (1 — первый повтор).

    Returns:
        Задержка в секундах, ограниченная :data:`_RETRY_MAX_SECONDS`.
    """
    delay = _RETRY_BASE_SECONDS * 2.0 ** max(0, attempt - 1)
    return min(delay, _RETRY_MAX_SECONDS)


class JitoClient:
    """Асинхронный JSON-RPC клиент Jito Block Engine.

    Держит ``aiohttp``-сессию с keep-alive и кэширует список tip-аккаунтов.
    Если Block Engine недоступен при получении tip-аккаунтов, используется
    статический список из :data:`~utils.constants.JITO_TIP_ACCOUNTS`.
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
            base_url: URL Jito Block Engine (по умолчанию из настроек).
            timeout_seconds: Таймаут запроса, c.
            max_retries: Число повторов при временных сбоях.
            latency: Реестр метрик латентности.
        """
        self._base_url = (base_url or settings.jito_block_engine_url).rstrip("/")
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_seconds or settings.rpc_timeout_seconds
        )
        self._max_retries = max_retries
        self._latency = latency or LatencyRegistry()
        self._stats = self._latency.get("jito")
        self._session: aiohttp.ClientSession | None = None
        self._tip_accounts: list[str] = list(JITO_TIP_ACCOUNTS)
        self._tip_fetched_at: float = 0.0
        self._request_id: int = 0

    @property
    def base_url(self) -> str:
        """Базовый URL Block Engine."""
        return self._base_url

    async def start(self) -> None:
        """Создаёт HTTP-сессию. Идемпотентно."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"Content-Type": "application/json"},
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
    def _next_id(self) -> int:
        """Возвращает следующий id JSON-RPC-запроса.

        Returns:
            Идентификатор запроса.
        """
        self._request_id += 1
        return self._request_id

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Выполняет JSON-RPC POST к Block Engine с ретраями.

        Args:
            path: Путь относительно базового URL.
            payload: JSON-RPC тело запроса.

        Returns:
            Разобранный JSON-ответ.

        Raises:
            JitoError: При сетевом сбое или исчерпании повторов.
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
                async with self._session.post(url, data=json.dumps(payload)) as response:
                    text = await response.text()
                    self._stats.observe_us(now_us() - start)
                    if response.status in _RETRYABLE_STATUS:
                        last_error = f"HTTP {response.status}: {text[:200]}"
                        continue
                    if response.status >= 400:
                        raise JitoError(
                            f"Jito {path} HTTP {response.status}: {text[:300]}"
                        )
                    parsed = json.loads(text)
            except (aiohttp.ClientError, TimeoutError) as exc:
                self._stats.observe_us(now_us() - start)
                last_error = str(exc)
                continue

            if not isinstance(parsed, dict):
                last_error = "некорректный JSON-ответ"
                continue
            error = parsed.get("error")
            if error is not None:
                message = (
                    error.get("message") if isinstance(error, dict) else str(error)
                )
                raise JitoError(f"Jito {path}: {message}")
            return parsed
        raise JitoError(f"Jito {path} недоступен: {last_error}")

    # ------------------------------------------------------------- Tip-аккаунты
    async def get_tip_accounts(self, *, force_refresh: bool = False) -> list[str]:
        """Возвращает список аккаунтов для чаевых Jito.

        Args:
            force_refresh: Обновить кэш принудительно.

        Returns:
            Непустой список адресов (статический fallback при сбое).
        """
        if (
            not force_refresh
            and self._tip_fetched_at > 0
            and time.time() - self._tip_fetched_at < _TIP_CACHE_SECONDS
        ):
            return self._tip_accounts
        try:
            response = await self._post(
                JITO_TIP_ACCOUNTS_PATH,
                {
                    "jsonrpc": "2.0",
                    "id": self._next_id(),
                    "method": "getTipAccounts",
                    "params": [],
                },
            )
        except JitoError as exc:
            logger.warning("Не удалось получить tip-аккаунты Jito: {}", exc)
            return self._tip_accounts
        result = response.get("result")
        if isinstance(result, list) and result:
            self._tip_accounts = [str(addr) for addr in result]
            self._tip_fetched_at = time.time()
        return self._tip_accounts

    async def choose_tip_account(self) -> str:
        """Выбирает случайный аккаунт для чаевых.

        Returns:
            Адрес tip-аккаунта (base58).
        """
        accounts = await self.get_tip_accounts()
        return random.choice(accounts)

    # ----------------------------------------------------------------- Бандлы
    async def send_bundle(self, transactions: list[bytes]) -> str:
        """Отправляет бандл подписанных транзакций.

        Args:
            transactions: Список сериализованных подписанных транзакций.

        Returns:
            ID бандла (base58).

        Raises:
            JitoError: Если бандл пуст или отклонён Block Engine.
        """
        if not transactions:
            raise JitoError("Нельзя отправить пустой бандл")
        encoded = [base64.b64encode(raw).decode("ascii") for raw in transactions]
        response = await self._post(
            JITO_BUNDLE_PATH,
            {
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "sendBundle",
                "params": [encoded, {"encoding": "base64"}],
            },
        )
        bundle_id = response.get("result")
        if not isinstance(bundle_id, str) or not bundle_id:
            raise JitoError(f"Jito не вернул ID бандла: {response}")
        return bundle_id

    async def get_bundle_statuses(
        self, bundle_ids: list[str]
    ) -> dict[str, JitoBundleStatus]:
        """Возвращает статусы бандлов по их идентификаторам.

        Args:
            bundle_ids: Список ID бандлов.

        Returns:
            Словарь ``bundle_id -> JitoBundleStatus``.
        """
        if not bundle_ids:
            return {}
        response = await self._post(
            JITO_BUNDLE_STATUS_PATH,
            {
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": "getBundleStatuses",
                "params": [[bundle_ids], {"encoding": "base64"}],
            },
        )
        result = response.get("result")
        value = result.get("value", []) if isinstance(result, dict) else []
        statuses: dict[str, JitoBundleStatus] = {}
        for item in value:
            if not isinstance(item, dict):
                continue
            bundle_id = str(item.get("bundle_id", ""))
            if not bundle_id:
                continue
            statuses[bundle_id] = JitoBundleStatus(
                bundle_id=bundle_id,
                status=str(item.get("confirmation_status", "Pending")),
                landed_slot=item.get("slot"),
                error=_first_error(item.get("err")),
            )
        return statuses


def _first_error(err: Any) -> str | None:
    """Извлекает первый элемент ошибки бандла (``err`` бывает вложенным).

    Args:
        err: Значение поля ``err`` из ответа Jito.

    Returns:
        Строковое описание ошибки или ``None``.
    """
    if err is None:
        return None
    if isinstance(err, dict):
        return json.dumps(err, ensure_ascii=False)[:300]
    if isinstance(err, list):
        for item in err:
            if item:
                return str(item)[:300]
        return None
    return str(err)[:300]
