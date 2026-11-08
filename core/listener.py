"""Потоковый слушатель on-chain событий (Geyser/Helius WebSocket).

Модуль превращает «сырые» WS-уведомления в доменные события бота:

* **``logsSubscribe`` на программу Pump.fun** — извлекает Anchor-события
  ``TradeEvent`` (каждая покупка/продажа на bonding curve содержит готовые
  виртуальные резервы, поэтому дополнительный RPC-запрос не нужен);
* **``accountSubscribe`` на аккаунт кривой или vault-аккаунты AMM** —
  поддерживает резервы пула в актуальном состоянии, а для пулов без
  событийного лога (Raydium V4, PumpSwap) синтезирует сделки по дельтам
  резервов;
* **обнаружение новых пулов** — первый ``TradeEvent`` по неизвестному mint
  автоматически регистрирует пул в :class:`~core.pool_registry.PoolRegistry`
  и подписывает его аккаунты.

Слушатель не принимает торговых решений: он кладёт :class:`TradeEvent` в
:class:`asyncio.Queue`, откуда их забирает :class:`~core.trader.MomentumTrader`.
Очередь выбрана осознанно — колбэк WS не должен блокироваться исполнением
ордера, иначе reader-loop перестанет читать поток и задержки вырастут.

Все резервы хранятся в «сырых» единицах (лампорты / base units).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Self

from solders.pubkey import Pubkey

from config.settings import settings
from core.client import RpcError, SolanaRpcClient, SolanaWsClient
from core.decoders import (
    DecodedPool,
    PumpTradeEvent,
    decode_bonding_curve,
    decode_pool_by_owner,
    decode_pump_trade_event,
    vault_amount_from_account,
)
from core.pool_registry import PoolRegistry, TrackedPool
from core.tx_builder import TxBuilderError, derive_bonding_curve
from models.trade_data import DEXType, TradeEvent, TradeSide
from utils.constants import (
    PUMP_FUN_AMM_PROGRAM_ID,
    PUMP_FUN_PROGRAM_ID,
    PUMP_TOKEN_DECIMALS,
    PUMP_TRADE_EVENT_DISCRIMINATOR,
    PUMP_TRADE_EVENT_DISCRIMINATOR_LEN,
    RAYDIUM_AMM_V4_PROGRAM_ID,
    SOL_DECIMALS,
    WSOL_MINT,
)
from utils.latency import LatencyRegistry, now_us
from utils.logger import get_logger

logger = get_logger()

#: Префикс строки лога с Anchor-событием.
_PROGRAM_DATA_PREFIX: str = "Program data: "

#: Ёмкость очереди событий (защита от неограниченного роста памяти).
DEFAULT_QUEUE_SIZE: int = 8192

#: Минимальный дельта-объём для синтеза сделки по vault-аккаунту, SOL.
_MIN_SYNTHETIC_SOL: float = 1e-9


@dataclass(slots=True)
class ListenerStats:
    """Счётчики работы слушателя (для логов и health-check)."""

    log_notifications: int = 0
    account_notifications: int = 0
    trade_events: int = 0
    synthetic_trades: int = 0
    discovered_pools: int = 0
    dropped_events: int = 0
    parse_errors: int = 0
    started_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, float | int]:
        """Сводка счётчиков.

        Returns:
            Словарь со всеми счётчиками и временем работы.
        """
        return {
            "log_notifications": self.log_notifications,
            "account_notifications": self.account_notifications,
            "trade_events": self.trade_events,
            "synthetic_trades": self.synthetic_trades,
            "discovered_pools": self.discovered_pools,
            "dropped_events": self.dropped_events,
            "parse_errors": self.parse_errors,
            "uptime_seconds": round(time.time() - self.started_at, 1),
        }


def extract_pump_trade_events(logs: Sequence[str]) -> list[PumpTradeEvent]:
    """Извлекает события ``TradeEvent`` из логов транзакции Pump.fun.

    Args:
        logs: Строки лога из WS-уведомления ``logsNotification``.

    Returns:
        Список разобранных событий (пустой, если лог не содержит событий).
    """
    events: list[PumpTradeEvent] = []
    for line in logs:
        if not line.startswith(_PROGRAM_DATA_PREFIX):
            continue
        encoded = line[len(_PROGRAM_DATA_PREFIX) :].strip()
        if not encoded:
            continue
        try:
            raw = base64.b64decode(encoded, validate=False)
        except (ValueError, binascii.Error):
            continue
        if len(raw) <= PUMP_TRADE_EVENT_DISCRIMINATOR_LEN:
            continue
        if raw[:PUMP_TRADE_EVENT_DISCRIMINATOR_LEN] != PUMP_TRADE_EVENT_DISCRIMINATOR:
            continue
        event = decode_pump_trade_event(raw[PUMP_TRADE_EVENT_DISCRIMINATOR_LEN:])
        if event is not None:
            events.append(event)
    return events


class PoolStreamListener:
    """Слушатель потока сделок и состояния пулов.

    Подписывается на логи программы Pump.fun и на аккаунты отслеживаемых
    пулов, поддерживает :class:`~core.pool_registry.PoolRegistry` в актуальном
    состоянии и публикует :class:`~models.trade_data.TradeEvent` в
    асинхронную очередь :attr:`trades`.

    Attributes:
        trades: Очередь событий сделок для потребителя (трейдера).
        stats: Счётчики работы слушателя.
    """

    def __init__(
        self,
        rpc: SolanaRpcClient,
        ws: SolanaWsClient,
        registry: PoolRegistry | None = None,
        *,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        commitment: str = "processed",
        latency: LatencyRegistry | None = None,
        on_pool_discovered: Callable[[TrackedPool], None] | None = None,
    ) -> None:
        """Инициализирует слушатель.

        Args:
            rpc: HTTP RPC-клиент (для первичного чтения аккаунтов пула).
            ws: WS-клиент с авто-реконнектом.
            registry: Реестр пулов (создаётся при ``None``).
            queue_size: Ёмкость очереди событий.
            commitment: Уровень подтверждения для подписок.
            latency: Реестр метрик латентности.
            on_pool_discovered: Необязательный синхронный колбэк на новый пул.
        """
        self._rpc = rpc
        self._ws = ws
        self._registry = registry if registry is not None else PoolRegistry()
        self._commitment = commitment
        self._latency = latency or LatencyRegistry()
        self._parse_stats = self._latency.get("parse")
        self._on_pool_discovered = on_pool_discovered

        self.trades: asyncio.Queue[TradeEvent] = asyncio.Queue(maxsize=queue_size)
        self.stats = ListenerStats()
        self._log_subscriptions: list[int] = []
        self._subscribed_programs: set[str] = set()
        self._retry_task: asyncio.Task[None] | None = None
        self._subscription_tokens: dict[str, list[int]] = {}

    # ------------------------------------------------------------------ Свойства
    @property
    def registry(self) -> PoolRegistry:
        """Реестр отслеживаемых пулов."""
        return self._registry

    # ------------------------------------------------------------- Жизненный цикл
    async def start(self) -> None:
        """Запускает подписки на логи программ.

        Если Helius WS недоступен при старте — бот **не падает**: слушатель
        уходит в фоновый ретрай и подпишется, как только сеть появится
        (дашборд всё это время показывает состояние).
        """
        await self._ws.start()
        if await self._ws.wait_connected(timeout=20.0):
            await self._subscribe_program_logs()
            return
        logger.critical(
            "WS Helius недоступен при старте — продолжаю работу с фоновым "
            "переподключением; события пойдут после восстановления сети "
            "(дашборд показывает состояние)"
        )
        self._retry_task = asyncio.create_task(self._retry_subscribe())

    async def _subscribe_program_logs(self) -> None:
        """Подписывает лог-программы; требует живого WS-соединения.

        Raises:
            RpcError: Если подписка не удалась (ретрай снаружи).
        """
        for program_id in (PUMP_FUN_PROGRAM_ID, PUMP_FUN_AMM_PROGRAM_ID):
            if program_id in self._subscribed_programs:
                continue
            token = await self._ws.subscribe_logs(
                program_id, self._handle_logs_notification, commitment=self._commitment
            )
            self._log_subscriptions.append(token)
            self._subscribed_programs.add(program_id)
        logger.info(
            "Слушатель запущен: программы {}, {} (включённые DEX: {})",
            PUMP_FUN_PROGRAM_ID,
            PUMP_FUN_AMM_PROGRAM_ID,
            settings.enabled_dex,
        )

    async def _retry_subscribe(self) -> None:
        """Фоновый цикл: подпишется, как только WS-соединение вернётся."""
        while True:
            try:
                if self._ws.is_connected:
                    await self._subscribe_program_logs()
                    return
            except RpcError as exc:
                logger.debug("Ретрай подписки слушателя не удался: {}", exc)
            await asyncio.sleep(5.0)

    async def unsubscribe_pool(self, pool_address: str) -> None:
        """Отписывает аккаунты пула (после удаления из реестра).

        Без этого число account-подписок растёт неограниченно, и каждый
        реконнект переподписывает десятки «мёртвых» аккаунтов.

        Args:
            pool_address: Адрес пула.
        """
        tokens = self._subscription_tokens.pop(pool_address, [])
        for token in tokens:
            try:
                await self._ws.unsubscribe(token)
            except RpcError as exc:
                logger.debug("Отписка {} не удалась: {}", token, exc)

    async def stop(self) -> None:
        """Останавливает слушатель и снимает все подписки."""
        if self._retry_task is not None and not self._retry_task.done():
            self._retry_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._retry_task
        self._retry_task = None
        for token in self._log_subscriptions:
            await self._ws.unsubscribe(token)
        self._log_subscriptions.clear()
        for tokens in self._subscription_tokens.values():
            for token in tokens:
                await self._ws.unsubscribe(token)
        self._subscription_tokens.clear()
        logger.info("Слушатель остановлен: {}", self.stats.as_dict())

    async def __aenter__(self) -> Self:
        """Вход в контекст — запуск слушателя."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста — остановка слушателя."""
        await self.stop()

    # -------------------------------------------------------- Обработка логов
    async def _handle_logs_notification(self, payload: dict[str, Any]) -> None:
        """Обрабатывает уведомление ``logsNotification``.

        Args:
            payload: Полезная нагрузка WS-уведомления.
        """
        self.stats.log_notifications += 1
        value = payload.get("value")
        if not isinstance(value, dict):
            return
        if value.get("err") is not None:
            return
        logs = value.get("logs")
        if not isinstance(logs, list):
            return

        started = now_us()
        try:
            events = extract_pump_trade_events(logs)
        except Exception as exc:
            self.stats.parse_errors += 1
            logger.debug("Не удалось разобрать логи: {}", exc)
            return
        finally:
            self._parse_stats.observe_us(now_us() - started)

        signature = str(value.get("signature", ""))
        for event in events:
            await self._process_pump_trade(event, signature)

    async def _process_pump_trade(
        self, event: PumpTradeEvent, signature: str
    ) -> None:
        """Регистрирует пул (если новый) и публикует событие сделки.

        Args:
            event: Разобранное событие Pump.fun.
            signature: Подпись транзакции.
        """
        pool = self._registry.get_by_mint(event.mint)
        if pool is None:
            pool = await self._discover_bonding_curve(event.mint, signature)
            if pool is None:
                return

        self._apply_bonding_curve_reserves(
            pool, event.virtual_sol_reserves, event.virtual_token_reserves
        )

        side = TradeSide.BUY if event.is_buy else TradeSide.SELL
        trade = TradeEvent(
            signature=signature,
            pool_address=pool.pool_address,
            base_mint=pool.base_mint,
            dex=pool.dex,
            side=side,
            sol_amount=event.sol_amount / float(10**SOL_DECIMALS),
            token_amount=event.token_amount / float(10**pool.base_decimals),
            wallet=event.user,
            timestamp=event.timestamp if event.timestamp > 0 else time.time(),
        )
        self.stats.trade_events += 1
        await self._publish(trade)

    async def _discover_bonding_curve(
        self, mint: str, signature: str
    ) -> TrackedPool | None:
        """Создаёт и регистрирует пул bonding curve по mint.

        Адрес кривой выводится детерминированно (PDA), поэтому дополнительный
        RPC-запрос не требуется; аккаунт кривой подписывается сразу, чтобы
        резервы не «отставали» между событиями сделок.

        Args:
            mint: Mint мемкоина.
            signature: Подпись транзакции, открывшей пул.

        Returns:
            Зарегистрированный :class:`TrackedPool` или ``None`` при ошибке.
        """
        try:
            curve = derive_bonding_curve(mint)
        except (TxBuilderError, ValueError) as exc:
            logger.debug("Не удалось вывести bonding curve для {}: {}", mint, exc)
            return None

        pool = TrackedPool(
            pool_address=str(curve),
            dex=DEXType.PUMP_FUN,
            dex_program=PUMP_FUN_PROGRAM_ID,
            base_mint=mint,
            quote_mint=WSOL_MINT,
            base_decimals=PUMP_TOKEN_DECIMALS,
            quote_decimals=SOL_DECIMALS,
            curve_address=str(curve),
            discovery_signature=signature,
        )
        registered = self._registry.register(pool)
        self.stats.discovered_pools += 1
        logger.info(
            "Обнаружен новый пул Pump.fun: {} (mint {})", registered.pool_address, mint
        )

        await self._subscribe_pool_accounts(registered)
        if self._on_pool_discovered is not None:
            try:
                self._on_pool_discovered(registered)
            except Exception as exc:
                logger.warning("Ошибка колбэка обнаружения пула: {}", exc)
        return registered

    async def _subscribe_pool_accounts(self, pool: TrackedPool) -> None:
        """Подписывается на аккаунты пула (кривая либо vault-аккаунты).

        Args:
            pool: Отслеживаемый пул.
        """
        addresses = pool.account_addresses
        if not addresses:
            return
        tokens = self._subscription_tokens.setdefault(pool.pool_address, [])
        for address in addresses:
            callback = self._make_account_callback(pool.pool_address, address)
            try:
                token = await self._ws.subscribe_account(
                    address, callback, commitment=self._commitment
                )
            except RpcError as exc:
                logger.warning("Не удалось подписаться на {}: {}", address, exc)
                continue
            tokens.append(token)
            pool.subscription_tokens.append(token)

    def _make_account_callback(
        self, pool_address: str, account_address: str
    ) -> Callable[[dict[str, Any]], Any]:
        """Создаёт колбэк для уведомлений по конкретному аккаунту.

        Args:
            pool_address: Адрес пула, которому принадлежит аккаунт.
            account_address: Адрес кривой или vault-аккаунта.

        Returns:
            Асинхронный колбэк для WS-клиента.
        """

        async def callback(payload: dict[str, Any]) -> None:
            await self._handle_account_notification(
                pool_address, account_address, payload
            )

        return callback

    async def _handle_account_notification(
        self, pool_address: str, account_address: str, payload: dict[str, Any]
    ) -> None:
        """Обрабатывает изменение аккаунта пула.

        Для bonding curve обновляет виртуальные резервы; для vault-аккаунтов
        AMM — читает баланс токена и синтезирует сделку по дельте резерва.

        Args:
            pool_address: Адрес пула.
            account_address: Адрес изменившегося аккаунта.
            payload: Полезная нагрузка WS-уведомления.
        """
        self.stats.account_notifications += 1
        pool = self._registry.get(pool_address)
        if pool is None:
            return
        value = payload.get("value")
        if not isinstance(value, dict):
            return
        data_field = value.get("data")
        if not isinstance(data_field, list) or not data_field:
            return
        try:
            raw = base64.b64decode(data_field[0], validate=False)
        except (ValueError, binascii.Error):
            self.stats.parse_errors += 1
            return

        if pool.curve_address == account_address:
            state = decode_bonding_curve(raw)
            if state is None:
                return
            self._apply_bonding_curve_reserves(
                pool, state.virtual_sol_reserves, state.virtual_token_reserves
            )
            return

        amount = vault_amount_from_account(raw)
        if amount is None:
            return
        await self._apply_vault_delta(pool, account_address, amount)

    # ---------------------------------------------------- Применение состояния
    @staticmethod
    def _apply_bonding_curve_reserves(
        pool: TrackedPool, virtual_sol: int, virtual_token: int
    ) -> None:
        """Обновляет резервы bonding curve из события/аккаунта.

        Pump.fun использует AMM на виртуальных резервах, поэтому в пул
        записываются именно ``virtual_sol_reserves`` /
        ``virtual_token_reserves`` — только так цена и price impact совпадают
        с реальным поведением контракта.

        Args:
            pool: Отслеживаемый пул.
            virtual_sol: Виртуальный резерв SOL (лампорты).
            virtual_token: Виртуальный резерв токенов (base units).
        """
        pool.quote_reserve = max(0, virtual_sol)
        pool.base_reserve = max(0, virtual_token)
        pool.last_update = time.time()
        if pool.base_reserve > 0 and pool.quote_reserve > 0:
            pool.last_price = (
                pool.quote_reserve
                / float(10**pool.quote_decimals)
                / (pool.base_reserve / float(10**pool.base_decimals))
            )

    async def _apply_vault_delta(
        self, pool: TrackedPool, vault: str, new_amount: int
    ) -> None:
        """Обновляет резерв vault и синтезирует сделку по дельте.

        Для AMM-пулов (PumpSwap, Raydium V4) событийного лога о сделке нет,
        но изменение баланса vault однозначно определяет сторону и объём
        сделки: уменьшение резерва базового токена — покупка, уменьшение
        резерва quote — продажа.

        Args:
            pool: Отслеживаемый пул.
            vault: Адрес изменившегося vault-аккаунта.
            new_amount: Новый баланс vault в base units.
        """
        is_base = vault == pool.base_vault
        old_amount = pool.base_reserve if is_base else pool.quote_reserve
        if new_amount == old_amount:
            return

        if is_base:
            pool.base_reserve = new_amount
        else:
            pool.quote_reserve = new_amount
        pool.last_update = time.time()

        delta_base = abs(new_amount - old_amount) if is_base else 0
        delta_quote = abs(new_amount - old_amount) if not is_base else 0
        if delta_base == 0 and delta_quote == 0:
            return

        side = (
            TradeSide.BUY
            if (is_base and new_amount < old_amount)
            else TradeSide.SELL
        )
        sol_amount = delta_quote / float(10**SOL_DECIMALS)
        token_amount = delta_base / float(10**pool.base_decimals)

        if pool.base_reserve > 0 and pool.quote_reserve > 0:
            price = (
                pool.quote_reserve
                / float(10**pool.quote_decimals)
                / (pool.base_reserve / float(10**pool.base_decimals))
            )
            pool.last_price = price
            if sol_amount <= _MIN_SYNTHETIC_SOL and token_amount > 0:
                sol_amount = token_amount * price
        if sol_amount <= 0:
            return

        trade = TradeEvent(
            signature=f"synthetic:{vault}:{int(pool.last_update * 1000)}",
            pool_address=pool.pool_address,
            base_mint=pool.base_mint,
            dex=pool.dex,
            side=side,
            sol_amount=sol_amount,
            token_amount=token_amount,
            wallet=None,
            timestamp=pool.last_update,
        )
        self.stats.synthetic_trades += 1
        await self._publish(trade)

    async def _publish(self, trade: TradeEvent) -> None:
        """Кладёт событие в очередь без блокировки reader-loop.

        Args:
            trade: Событие сделки.
        """
        try:
            self.trades.put_nowait(trade)
        except asyncio.QueueFull:
            self.stats.dropped_events += 1
            logger.warning(
                "Очередь событий переполнена ({}), событие по {} отброшено",
                self.trades.qsize(),
                trade.pool_address,
            )

    # -------------------------------------------------------- Ручная загрузка
    async def register_pool_from_mint(self, mint: str) -> TrackedPool | None:
        """Регистрирует пул по mint, определяя DEX через RPC.

        Используется при старте (watchlist) и когда токен мигрировал на AMM:
        ищется аккаунт PumpSwap/Raydium, содержащий mint в поле ``base_mint``.

        Args:
            mint: Mint мемкоина.

        Returns:
            Зарегистрированный :class:`TrackedPool` или ``None``, если ни
            bonding curve, ни AMM-пул не найдены.
        """
        existing = self._registry.get_by_mint(mint)
        if existing is not None:
            return existing

        bonding_pool = await self._discover_bonding_curve(mint, "")
        if bonding_pool is not None:
            await self._refresh_pool_from_chain(bonding_pool)
            if bonding_pool.is_ready:
                return bonding_pool
            self._registry.remove(bonding_pool.pool_address)

        for program_id, dex in (
            (PUMP_FUN_AMM_PROGRAM_ID, DEXType.PUMP_FUN),
            (RAYDIUM_AMM_V4_PROGRAM_ID, DEXType.RAYDIUM),
        ):
            if not settings.dex_enabled(dex):
                continue
            pool = await self._find_amm_pool(program_id, dex, mint)
            if pool is not None:
                return pool
        return None

    async def _find_amm_pool(
        self, program_id: str, dex: DEXType, mint: str
    ) -> TrackedPool | None:
        """Ищет AMM-пул по mint через ``getProgramAccounts``.

        Args:
            program_id: Программа DEX.
            dex: Тип DEX для регистрации.
            mint: Mint базового токена.

        Returns:
            Зарегистрированный :class:`TrackedPool` или ``None``.
        """
        try:
            encoded_mint = str(Pubkey.from_string(mint))
        except (ValueError, TypeError):
            return None
        try:
            accounts = await self._rpc.get_program_accounts(
                program_id, memcmp=(43, encoded_mint), commitment=self._commitment
            )
        except RpcError as exc:
            logger.debug("getProgramAccounts для {} не удался: {}", program_id, exc)
            return None

        for address, state in accounts:
            decoded: DecodedPool | None = decode_pool_by_owner(program_id, state.data)
            if decoded is None:
                continue
            pool = TrackedPool(
                pool_address=address,
                dex=dex,
                dex_program=program_id,
                base_mint=decoded.base_mint,
                quote_mint=decoded.quote_mint,
                base_decimals=PUMP_TOKEN_DECIMALS,
                quote_decimals=SOL_DECIMALS,
                base_vault=decoded.base_vault,
                quote_vault=decoded.quote_vault,
                lp_mint=decoded.lp_mint,
            )
            registered = self._registry.register(pool)
            self.stats.discovered_pools += 1
            logger.info("Найден AMM-пул {} для mint {}", address, mint)
            await self._refresh_pool_from_chain(registered)
            await self._subscribe_pool_accounts(registered)
            return registered
        return None

    async def _refresh_pool_from_chain(self, pool: TrackedPool) -> None:
        """Читает резервы пула из сети.

        Нужно для AMM-пулов, где нет событийного лога: первичное состояние
        берётся из vault-аккаунтов (или из аккаунта кривой для Pump.fun).

        Args:
            pool: Отслеживаемый пул.
        """
        addresses = [addr for addr in (pool.base_vault, pool.quote_vault) if addr]
        if pool.curve_address is not None:
            addresses = [pool.curve_address]
        if not addresses:
            return
        try:
            states = await self._rpc.get_multiple_accounts(
                addresses, commitment=self._commitment
            )
        except RpcError as exc:
            logger.debug(
                "Не удалось прочитать резервы пула {}: {}", pool.pool_address, exc
            )
            return

        if pool.curve_address is not None and states:
            state = states[0]
            if state is not None:
                curve = decode_bonding_curve(state.data)
                if curve is not None:
                    self._apply_bonding_curve_reserves(
                        pool, curve.virtual_sol_reserves, curve.virtual_token_reserves
                    )
            return

        for address, state in zip(addresses, states, strict=False):
            if state is None:
                continue
            amount = vault_amount_from_account(state.data)
            if amount is None:
                continue
            if address == pool.base_vault:
                pool.base_reserve = amount
            else:
                pool.quote_reserve = amount
        pool.last_update = time.time()
