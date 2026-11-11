"""Исполнитель ордеров: сборка Jito-бандлов покупки/продажи.

Модуль связывает всю низкоуровневую механику в единый «горячий» путь
исполнения сделки:

1. выбор маршрута — прямые инструкции Pump.fun bonding curve
   (:mod:`core.tx_builder`) либо агрегаторный fallback Jupiter
   (:mod:`core.jupiter`), если токен уже мигрировал в AMM;
2. сборка v0-транзакции с обязательными Compute Budget, memo-антифронтран
   ``jitodontfront`` и инструкцией чаевых Jito;
3. подпись единственным ключом кошелька и отправка **Jito-бандлом** через
   :class:`~core.jito.JitoClient` (защита от MEV и гарантия атомарности).

Проскальзывание задаётся явно: ``max_sol_cost`` для покупки на bonding curve
и ``min_sol_output`` для продажи (значения приходят из
:class:`~utils.amm.AmmQuote` с уже применённым дисконтом
``BUY_SLIPPAGE_PCT``/``SELL_SLIPPAGE_PCT``). Для маршрута Jupiter
проскальзывание транслируется в ``slippageBps``.

Исполнитель не хранит состояние позиции и не принимает торговых решений —
это чистый «sender», вызываемый пайплайном.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Self

from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.hash import Hash
from solders.instruction import Instruction
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from config.settings import LAMPORTS_PER_SOL, settings
from core.client import BlockhashInfo, RpcError, SolanaRpcClient
from core.jito import JitoClient, JitoError
from core.jupiter import JupiterClient, JupiterError
from core.tx_builder import (
    PumpAccounts,
    TxBuilderError,
    build_compute_budget_instructions,
    build_frontrun_memo_instruction,
    build_pump_accounts,
    build_pump_buy_instruction,
    build_pump_sell_instruction,
    build_tip_instruction,
)
from models.trade_data import DEXType, OrderResult, PoolInfo, TradeSide
from utils.amm import AmmQuote, max_in_with_slippage
from utils.constants import WSOL_MINT
from utils.latency import LatencyRegistry, now_us
from utils.logger import get_logger

logger = get_logger()


class ExecutorError(RuntimeError):
    """Ошибка построения или отправки ордера."""


# --------------------------------------------------------------------------- #
#  Заявки на исполнение
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class BuyOrder:
    """Заявка на покупку базового токена за SOL.

    Attributes:
        pool: Снимок пула на момент сигнала.
        sol_amount_lamports: Входной объём в лампортах.
        expected_tokens: Ожидаемое количество токенов (base units).
        min_tokens_out: Минимально приемлемое количество токенов с дисконтом
            ``BUY_SLIPPAGE_PCT`` (для маршрута AMM/Jupiter).
        price_impact_pct: Оценённый Price Impact ордера, %.
        creator: Кошелёк создателя токена (для Pump.fun creator vault).
    """

    pool: PoolInfo
    sol_amount_lamports: int
    expected_tokens: int
    min_tokens_out: int
    price_impact_pct: float = 0.0
    creator: str | None = None


@dataclass(frozen=True, slots=True)
class SellOrder:
    """Заявка на продажу базового токена за SOL.

    Attributes:
        pool: Снимок пула на момент сигнала.
        token_amount: Продаваемое количество токенов (base units).
        quote: AMM-котировка продажи (её ``min_amount_out`` — минимальный
            приемлемый выход в лампортах с учётом ``SELL_SLIPPAGE_PCT``).
        price_impact_pct: Оценённый Price Impact ордера, %.
        creator: Кошелёк создателя токена (для Pump.fun creator vault).
    """

    pool: PoolInfo
    token_amount: int
    quote: AmmQuote
    price_impact_pct: float = 0.0
    creator: str | None = None


@dataclass(slots=True)
class _SwapPlan:
    """Собранный план свапа (инструкции + lookup tables) до подписи.

    Attributes:
        instructions: Инструкции в порядке исполнения.
        lookup_tables: Address lookup tables (только для маршрута Jupiter).
        route: Маршрут свапа (``pump``/``jupiter``).
    """

    instructions: list[Instruction] = field(default_factory=list)
    lookup_tables: list[AddressLookupTableAccount] = field(default_factory=list)
    route: str = "pump"


# --------------------------------------------------------------------------- #
#  Исполнитель
# --------------------------------------------------------------------------- #
class OrderExecutor:
    """Отправляет ордера через RPC-транзакции либо Jito Bundles.

    Канал отправки задаётся настройкой ``EXECUTION_VENUE``:

    * ``rpc`` (по умолчанию) — обычная подписанная транзакция через RPC
      (Helius), без memo и Jito-чаевых; подтверждение через
      ``getSignatureStatuses``;
    * ``jito`` — бандл с memo ``jitodontfront``, Compute Budget и чаевыми
      валидатору; подтверждение через ``getBundleStatuses``.

    Маршрут свапа (прямые инструкции bonding curve или Jupiter) не зависит
    от канала отправки. Не владеет жизненным циклом RPC — его останавливает
    вызывающий код; клиенты Jito/Jupiter закрываются вместе с исполнителем,
    если были созданы внутри.
    """

    def __init__(
        self,
        rpc: SolanaRpcClient,
        keypair: Keypair,
        *,
        venue: str | None = None,
        jito: JitoClient | None = None,
        jupiter: JupiterClient | None = None,
        latency: LatencyRegistry | None = None,
        buy_slippage_pct: float | None = None,
        sell_slippage_pct: float | None = None,
        tip_lamports: int | None = None,
    ) -> None:
        """Инициализирует исполнитель.

        Args:
            rpc: HTTP RPC-клиент (для получения blockhash).
            keypair: Ключ кошелька-исполнителя (плательщик и подписант).
            jito: Клиент Jito Block Engine (создаётся при ``None``).
            jupiter: Клиент Jupiter (создаётся при ``None`` и включённом фолбэке).
            latency: Реестр метрик латентности.
            buy_slippage_pct: Проскальзывание покупки, % (по умолчанию из настроек).
            sell_slippage_pct: Проскальзывание продажи, % (по умолчанию из настроек).
            tip_lamports: Размер чаевых Jito, лампорты (по умолчанию из настроек).
        """
        self._rpc = rpc
        self._keypair = keypair
        self._pubkey: Pubkey = keypair.pubkey()
        self._latency = latency or LatencyRegistry()
        self._submit_stats = self._latency.get("submit")

        self._venue: str = (
            (venue or settings.execution_venue).strip().lower()
        )
        if self._venue not in ("rpc", "jito"):
            raise ExecutorError(f"неизвестный канал отправки: {self._venue}")

        self._jito: JitoClient | None = jito
        self._owns_jito = False
        if self._jito is None and self._venue == "jito":
            self._jito = JitoClient(latency=self._latency)
            self._owns_jito = True
        self._jupiter: JupiterClient | None = jupiter
        self._owns_jupiter = False
        if self._jupiter is None and settings.use_jupiter_fallback:
            self._jupiter = JupiterClient(latency=self._latency)
            self._owns_jupiter = True

        self._buy_slippage_pct = (
            settings.buy_slippage_pct
            if buy_slippage_pct is None
            else buy_slippage_pct
        )
        self._sell_slippage_pct = (
            settings.sell_slippage_pct
            if sell_slippage_pct is None
            else sell_slippage_pct
        )
        self._tip_lamports = (
            settings.jito_tip_lamports if tip_lamports is None else tip_lamports
        )

    # -------------------------------------------------------------- Свойства
    @property
    def wallet(self) -> str:
        """Адрес кошелька-исполнителя (base58)."""
        return str(self._pubkey)

    @property
    def venue(self) -> str:
        """Канал отправки ордеров (``rpc``/``jito``)."""
        return self._venue

    @property
    def buy_slippage_pct(self) -> float:
        """Проскальзывание покупки, %."""
        return self._buy_slippage_pct

    @property
    def sell_slippage_pct(self) -> float:
        """Проскальзывание продажи, %."""
        return self._sell_slippage_pct

    @property
    def tip_lamports(self) -> int:
        """Размер чаевых Jito, лампорты (venue=jito)."""
        return self._tip_lamports

    # ------------------------------------------------------------- Жизненный цикл
    async def start(self) -> None:
        """Открывает сетевые сессии под-клиентов. Идемпотентно."""
        if self._jito is not None:
            await self._jito.start()
        if self._jupiter is not None:
            await self._jupiter.start()

    async def close(self) -> None:
        """Закрывает под-клиенты, созданные исполнителем."""
        if self._owns_jito and self._jito is not None:
            await self._jito.close()
        if self._owns_jupiter and self._jupiter is not None:
            await self._jupiter.close()

    async def __aenter__(self) -> Self:
        """Вход в контекст — старт под-клиентов."""
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выход из контекста — закрытие под-клиентов."""
        await self.close()

    # --------------------------------------------------------------- Публичное
    async def execute_buy(self, order: BuyOrder) -> OrderResult:
        """Исполняет заявку на покупку.

        Args:
            order: Заявка на покупку.

        Returns:
            :class:`~models.trade_data.OrderResult` с подписью и ID бандла.
        """
        if order.sol_amount_lamports <= 0:
            return OrderResult.failed(
                side=TradeSide.BUY,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error="нулевой размер входного ордера",
            )
        try:
            plan = await self._plan_buy(order)
        except (ExecutorError, TxBuilderError, JupiterError, JitoError) as exc:
            logger.warning("Не удалось собрать покупку {}: {}", order.pool.base_mint, exc)
            return OrderResult.failed(
                side=TradeSide.BUY,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error=str(exc),
                venue=self._venue,
            )
        return await self._assemble_and_send(
            plan,
            side=TradeSide.BUY,
            base_mint=order.pool.base_mint,
            pool_address=order.pool.pool_address,
            sol_amount=order.sol_amount_lamports / LAMPORTS_PER_SOL,
            token_amount=order.expected_tokens / float(
                10**order.pool.base_decimals
            ),
            price_impact_pct=order.price_impact_pct,
        )

    async def execute_sell(self, order: SellOrder) -> OrderResult:
        """Исполняет заявку на продажу.

        Args:
            order: Заявка на продажу.

        Returns:
            :class:`~models.trade_data.OrderResult` с подписью и ID бандла.
        """
        if order.token_amount <= 0:
            return OrderResult.failed(
                side=TradeSide.SELL,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error="нулевой размер продаваемой позиции",
            )
        try:
            plan = await self._plan_sell(order)
        except (ExecutorError, TxBuilderError, JupiterError, JitoError) as exc:
            logger.warning("Не удалось собрать продажу {}: {}", order.pool.base_mint, exc)
            return OrderResult.failed(
                side=TradeSide.SELL,
                base_mint=order.pool.base_mint,
                pool_address=order.pool.pool_address,
                error=str(exc),
                venue=self._venue,
            )
        return await self._assemble_and_send(
            plan,
            side=TradeSide.SELL,
            base_mint=order.pool.base_mint,
            pool_address=order.pool.pool_address,
            sol_amount=order.quote.amount_out / LAMPORTS_PER_SOL,
            token_amount=order.token_amount / float(10**order.pool.base_decimals),
            price_impact_pct=order.price_impact_pct,
        )

    # ---------------------------------------------------- Планирование маршрута
    async def _plan_buy(self, order: BuyOrder) -> _SwapPlan:
        """Строит план покупки (bonding curve либо Jupiter).

        Args:
            order: Заявка на покупку.

        Returns:
            :class:`_SwapPlan` с инструкциями и lookup tables.

        Raises:
            ExecutorError: Если ни один маршрут недоступен.
        """
        if self._use_pump_route(order.pool) and order.creator is not None:
            return self._plan_pump_buy(order)
        return await self._plan_jupiter_buy(order)

    async def _plan_sell(self, order: SellOrder) -> _SwapPlan:
        """Строит план продажи (bonding curve либо Jupiter).

        Args:
            order: Заявка на продажу.

        Returns:
            :class:`_SwapPlan` с инструкциями и lookup tables.

        Raises:
            ExecutorError: Если ни один маршрут недоступен.
        """
        if self._use_pump_route(order.pool) and order.creator is not None:
            return self._plan_pump_sell(order)
        return await self._plan_jupiter_sell(order)

    @staticmethod
    def _use_pump_route(pool: PoolInfo) -> bool:
        """Проверяет, доступен ли прямой маршрут Pump.fun bonding curve.

        Args:
            pool: Снимок пула.

        Returns:
            ``True`` для токена, ещё не мигрировавшего с bonding curve.
        """
        return pool.dex == DEXType.PUMP_FUN and pool.lp_mint is None

    # ------------------------------------------------------ Маршрут Pump.fun
    def _plan_pump_buy(self, order: BuyOrder) -> _SwapPlan:
        """Собирает план покупки напрямую на bonding curve Pump.fun.

        Проскальзывание входа реализуется через ``max_sol_cost`` (потолок
        расхода SOL), а ожидаемое количество токенов — через ``amount``.

        Args:
            order: Заявка на покупку.

        Returns:
            :class:`_SwapPlan` с инструкцией ``buy``.

        Raises:
            ExecutorError: Если не удалось построить аккаунты/инструкцию.
        """
        assert order.creator is not None
        try:
            accounts: PumpAccounts = build_pump_accounts(
                order.pool.base_mint, self._pubkey, order.creator
            )
            max_sol_cost = max_in_with_slippage(
                order.sol_amount_lamports, self._buy_slippage_pct
            )
            instruction = build_pump_buy_instruction(
                accounts, order.expected_tokens, max_sol_cost
            )
        except TxBuilderError as exc:
            raise ExecutorError(f"Pump.fun buy: {exc}") from exc
        return _SwapPlan(
            instructions=[instruction],
            route="pump",
        )

    def _plan_pump_sell(self, order: SellOrder) -> _SwapPlan:
        """Собирает план продажи напрямую на bonding curve Pump.fun.

        Проскальзывание выхода реализуется через ``min_sol_output`` —
        минимально приемлемый выход SOL с учётом ``SELL_SLIPPAGE_PCT``.

        Args:
            order: Заявка на продажу.

        Returns:
            :class:`_SwapPlan` с инструкцией ``sell``.

        Raises:
            ExecutorError: Если не удалось построить аккаунты/инструкцию.
        """
        assert order.creator is not None
        try:
            accounts: PumpAccounts = build_pump_accounts(
                order.pool.base_mint, self._pubkey, order.creator
            )
            instruction = build_pump_sell_instruction(
                accounts, order.token_amount, order.quote.min_amount_out
            )
        except TxBuilderError as exc:
            raise ExecutorError(f"Pump.fun sell: {exc}") from exc
        return _SwapPlan(
            instructions=[instruction],
            route="pump",
        )

    # -------------------------------------------------------- Маршрут Jupiter
    def _require_jupiter(self) -> JupiterClient:
        """Возвращает клиент Jupiter или поднимает ошибку.

        Returns:
            :class:`~core.jupiter.JupiterClient`.

        Raises:
            ExecutorError: Если fallback отключён.
        """
        if self._jupiter is None:
            raise ExecutorError(
                "Jupiter fallback отключён, а прямой маршрут недоступен"
            )
        return self._jupiter

    async def _plan_jupiter_buy(self, order: BuyOrder) -> _SwapPlan:
        """Собирает план покупки через агрегатор Jupiter.

        Args:
            order: Заявка на покупку.

        Returns:
            :class:`_SwapPlan` с инструкциями свапа и lookup tables.

        Raises:
            ExecutorError: Если fallback отключён.
            JupiterError: При сбое агрегатора.
        """
        jupiter = self._require_jupiter()
        slippage_bps = max(1, round(self._buy_slippage_pct * 100))
        quote = await jupiter.get_quote(
            input_mint=WSOL_MINT,
            output_mint=order.pool.base_mint,
            amount=order.sol_amount_lamports,
            slippage_bps=slippage_bps,
        )
        swap = await jupiter.get_swap_instructions(
            quote, user_public_key=self.wallet
        )
        tables = await jupiter.fetch_lookup_tables(
            swap.address_lookup_table_addresses, self._rpc
        )
        return _SwapPlan(
            instructions=swap.core_instructions(),
            lookup_tables=tables,
            route="jupiter",
        )

    async def _plan_jupiter_sell(self, order: SellOrder) -> _SwapPlan:
        """Собирает план продажи через агрегатор Jupiter.

        Args:
            order: Заявка на продажу.

        Returns:
            :class:`_SwapPlan` с инструкциями свапа и lookup tables.

        Raises:
            ExecutorError: Если fallback отключён.
            JupiterError: При сбое агрегатора.
        """
        jupiter = self._require_jupiter()
        slippage_bps = max(1, round(self._sell_slippage_pct * 100))
        quote = await jupiter.get_quote(
            input_mint=order.pool.base_mint,
            output_mint=WSOL_MINT,
            amount=order.token_amount,
            slippage_bps=slippage_bps,
        )
        swap = await jupiter.get_swap_instructions(
            quote, user_public_key=self.wallet
        )
        tables = await jupiter.fetch_lookup_tables(
            swap.address_lookup_table_addresses, self._rpc
        )
        return _SwapPlan(
            instructions=swap.core_instructions(),
            lookup_tables=tables,
            route="jupiter",
        )

    # --------------------------------------------------- Сборка и отправка
    async def _assemble_and_send(
        self,
        plan: _SwapPlan,
        *,
        side: TradeSide,
        base_mint: str,
        pool_address: str,
        sol_amount: float,
        token_amount: float,
        price_impact_pct: float,
    ) -> OrderResult:
        """Собирает транзакцию, подписывает и отправляет Jito-бандлом.

        Args:
            plan: План свапа.
            side: Направление ордера.
            base_mint: Mint мемкоина.
            pool_address: Адрес пула.
            sol_amount: Объём ордера в SOL (для отчёта).
            token_amount: Объём ордера в токенах (для отчёта).
            price_impact_pct: Оценённый Price Impact, %.

        Returns:
            :class:`~models.trade_data.OrderResult`.
        """
        if not plan.instructions:
            return OrderResult.failed(
                side=side,
                base_mint=base_mint,
                pool_address=pool_address,
                error="пустой план свапа",
                venue=self._venue,
            )

        started = now_us()
        landed_slot: int | None = None
        bundle_id: str | None = None
        confirmed = True
        try:
            blockhash = await self._rpc.get_latest_blockhash("processed")
            if self._venue == "rpc":
                instructions = self._compose_instructions(plan.instructions, None)
                transaction = self._compile_and_sign(
                    instructions, plan.lookup_tables, blockhash
                )
                signature = await self._rpc.send_transaction(
                    bytes(transaction), skip_preflight=True, max_retries=0
                )
            else:
                assert self._jito is not None
                tip_account = await self._jito.choose_tip_account()
                instructions = self._compose_instructions(
                    plan.instructions, tip_account
                )
                transaction = self._compile_and_sign(
                    instructions, plan.lookup_tables, blockhash
                )
                bundle_id = await self._jito.send_bundle([bytes(transaction)])
                signature = str(transaction.signatures[0])
        except (RpcError, JitoError, ExecutorError, TxBuilderError) as exc:
            logger.warning("Ошибка отправки ордера {}: {}", base_mint, exc)
            return OrderResult.failed(
                side=side,
                base_mint=base_mint,
                pool_address=pool_address,
                error=str(exc),
                venue=self._venue,
            )

        elapsed_us = now_us() - started
        self._submit_stats.observe_us(elapsed_us)

        if self._venue == "rpc":
            if settings.jito_require_confirmation:
                confirmed, landed_slot, error = await self._await_rpc_confirmation(
                    signature
                )
                if not confirmed:
                    # Транзакция могла приземлиться после таймаута —
                    # статус подписи нужно проверить вручную.
                    logger.critical(
                        "Транзакция {} НЕ подтверждена ({}): sig={} — "
                        "проверьте статус вручную, средства могли быть "
                        "списаны",
                        base_mint,
                        error or "таймаут",
                        signature,
                    )
                    return OrderResult.failed(
                        side=side,
                        base_mint=base_mint,
                        pool_address=pool_address,
                        error=f"транзакция не подтверждена: {error or 'таймаут'}",
                        venue=self._venue,
                    )
            logger.info(
                "Ордер {} отправлен (rpc): sig={} slot={} ({:.1f} мс)",
                side.value,
                signature,
                landed_slot,
                elapsed_us / 1000.0,
            )
            return OrderResult(
                success=True,
                side=side,
                base_mint=base_mint,
                pool_address=pool_address,
                signature=signature,
                bundle_id=None,
                sol_amount=sol_amount,
                token_amount=token_amount,
                price_impact_pct=price_impact_pct,
                venue="rpc",
                confirmed=confirmed,
                landed_slot=landed_slot,
                submitted_at=time.time(),
                latency_ms=elapsed_us / 1000.0,
            )

        # --- Канал Jito ------------------------------------------------------
        assert bundle_id is not None
        logger.info(
            "Ордер {} отправлен (jito, {}): sig={} bundle={} ({:.1f} мс)",
            side.value,
            plan.route,
            signature,
            bundle_id,
            elapsed_us / 1000.0,
        )
        if settings.jito_require_confirmation:
            jito = self._jito
            assert jito is not None
            status = await jito.await_bundle_confirmation(bundle_id)
            normalized = status.status.strip().lower()
            if normalized == "landed":
                landed_slot = status.landed_slot
                logger.info("Бандл {} подтверждён в слоте {}", bundle_id, landed_slot)
            else:
                confirmed = False
                logger.critical(
                    "Бандл {} НЕ подтверждён ({}): sig={} — проверьте "
                    "статус подписи вручную, средства могли быть списаны",
                    bundle_id,
                    status.error or status.status,
                    signature,
                )
                return OrderResult.failed(
                    side=side,
                    base_mint=base_mint,
                    pool_address=pool_address,
                    error=(
                        f"бандл не подтверждён: {status.status}"
                        + (f" ({status.error})" if status.error else "")
                    ),
                    venue="jito",
                )

        return OrderResult(
            success=True,
            side=side,
            base_mint=base_mint,
            pool_address=pool_address,
            signature=signature,
            bundle_id=bundle_id,
            sol_amount=sol_amount,
            token_amount=token_amount,
            price_impact_pct=price_impact_pct,
            venue="jito",
            confirmed=confirmed,
            landed_slot=landed_slot,
            submitted_at=time.time(),
            latency_ms=elapsed_us / 1000.0,
        )

    def _compose_instructions(
        self, swap_instructions: list[Instruction], tip_account: str | None
    ) -> list[Instruction]:
        """Собирает итоговый список инструкций транзакции.

        Для канала Jito порядок: memo-антифронтран → Compute Budget → свап →
        чаевые. Для канала ``rpc`` memo и чаевые опускаются — только
        Compute Budget и свап.

        Args:
            swap_instructions: Ядро свапа (без служебных инструкций).
            tip_account: Аккаунт-получатель чаевых Jito (``None`` для rpc).

        Returns:
            Полный список инструкций для компиляции.
        """
        instructions: list[Instruction] = []
        if tip_account is not None:
            instructions.append(build_frontrun_memo_instruction(self._pubkey))
        instructions.extend(build_compute_budget_instructions())
        instructions.extend(swap_instructions)
        if tip_account is not None:
            instructions.append(
                build_tip_instruction(self._pubkey, tip_account, self._tip_lamports)
            )
        return instructions

    async def _await_rpc_confirmation(
        self,
        signature: str,
        *,
        timeout_seconds: float | None = None,
        poll_seconds: float | None = None,
    ) -> tuple[bool, int | None, str | None]:
        """Опрашивает статус подписи до подтверждения, отказа или таймаута.

        Подтверждением считается попадание транзакции в блок (любой уровень
        ``processed``/``confirmed``/``finalized``); ``err`` в статусе —
        немедленный отказ.

        Args:
            signature: Подпись отправленной транзакции.
            timeout_seconds: Общий таймаут, c (по умолчанию из настроек).
            poll_seconds: Интервал опроса, c (по умолчанию из настроек).

        Returns:
            Кортеж ``(подтверждена, слот, текст ошибки)``.
        """
        timeout = (
            settings.jito_confirm_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        poll = (
            settings.jito_confirm_poll_seconds
            if poll_seconds is None
            else poll_seconds
        )
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            try:
                statuses = await self._rpc.get_signature_statuses([signature])
                status = statuses[0] if statuses else None
            except RpcError as exc:
                status = None
                logger.debug("Опрос статуса {} не удался: {}", signature, exc)
            if status is not None:
                if status.err is not None:
                    return False, status.slot, str(status.err)
                if status.confirmation_status:
                    return True, status.slot, None
            if time.monotonic() >= deadline:
                return False, None, None
            # Экспоненциальный бэктрейс: меньше вызовов getSignatureStatuses
            # (экономия кредитов) при той же чувствительности.
            poll = min(poll * 1.6, 2.0)
            await asyncio.sleep(max(0.05, poll))

    def _compile_and_sign(
        self,
        instructions: list[Instruction],
        lookup_tables: list[AddressLookupTableAccount],
        blockhash: BlockhashInfo,
    ) -> VersionedTransaction:
        """Компилирует v0-сообщение и подписывает его ключом кошелька.

        Args:
            instructions: Полный список инструкций бандла.
            lookup_tables: Address lookup tables (для маршрута Jupiter).
            blockhash: Свежий blockhash.

        Returns:
            Подписанная :class:`solders.transaction.VersionedTransaction`.

        Raises:
            ExecutorError: Если blockhash или компиляция некорректны.
        """
        try:
            recent_blockhash = Hash.from_string(blockhash.blockhash)
        except (ValueError, TypeError) as exc:
            raise ExecutorError(
                f"Некорректный blockhash: {blockhash.blockhash!r}"
            ) from exc
        try:
            message = MessageV0.try_compile(
                payer=self._pubkey,
                instructions=instructions,
                address_lookup_table_accounts=lookup_tables,
                recent_blockhash=recent_blockhash,
            )
        except ValueError as exc:
            raise ExecutorError(f"Не удалось скомпилировать транзакцию: {exc}") from exc
        return VersionedTransaction(message, [self._keypair])
