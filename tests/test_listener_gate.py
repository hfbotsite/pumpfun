"""Тесты гейта логов слушателя: только bonding curve Pump.fun.

PumpSwap (pAMM) называет своё Anchor-событие тоже ``TradeEvent`` —
дискриминатор совпадает, раскладка полей нет. Чужие логи не должны
декодироваться bonding-curve декодером, а мусорные поля (время/резервы)
не должны проходить санити-гейт даже на родной программе.
"""

from __future__ import annotations

import asyncio
import base64
import time

from core.listener import PoolStreamListener
from core.pool_registry import TrackedPool
from models.trade_data import DEXType
from tests.test_decoders import build_trade_event_payload
from utils.constants import (
    PUMP_FUN_AMM_PROGRAM_ID,
    PUMP_FUN_PROGRAM_ID,
    PUMP_TRADE_EVENT_DISCRIMINATOR,
)


class FakeWs:
    """WS-заглушка: подписки аккаунтов успешны и без сети."""

    def __init__(self) -> None:
        """Пустой список адресов подписок аккаунтов."""
        self.account_calls: list[str] = []

    async def subscribe_account(
        self, address: str, *_args: object, **_kwargs: object
    ) -> int:
        """Запоминает адрес и возвращает фиктивный id подписки.

        Args:
            address: Адрес аккаунта.

        Returns:
            Порядковый номер подписки.
        """
        self.account_calls.append(address)
        return len(self.account_calls)


def make_listener() -> PoolStreamListener:
    """Слушатель без сети (rpc не используется в обработке логов).

    Returns:
        :class:`~core.listener.PoolStreamListener`.
    """
    return PoolStreamListener(rpc=None, ws=FakeWs())  # type: ignore[arg-type]


def logs_payload(event_body: bytes, program_id: str) -> dict[str, object]:
    """Собирает WS-уведомление logsNotification с одним событием.

    Args:
        event_body: Тело события TradeEvent (без дискриминатора).
        program_id: Программа, от чьего имени публикуется лог.

    Returns:
        Словарь-полезная нагрузка.
    """
    encoded = base64.b64encode(
        PUMP_TRADE_EVENT_DISCRIMINATOR + event_body
    ).decode()
    return {
        "value": {
            "err": None,
            "signature": "sig-test",
            "logs": [
                f"Program {program_id} invoke [1]",
                f"Program data: {encoded}",
                f"Program {program_id} success",
            ],
        }
    }


async def drain(queue: asyncio.Queue[object]) -> int:
    """Сливает очередь и возвращает число накопленных событий.

    Args:
        queue: Очередь событий сделок.

    Returns:
        Количество событий в очереди.
    """
    count = 0
    while not queue.empty():
        queue.get_nowait()
        count += 1
    return count


class TestForeignProgramGate:
    """Логи PumpSwap не декодируются bonding-curve декодером."""

    async def test_pamm_logs_dropped(self) -> None:
        """Валидное тело события из pAMM-программы игнорируется."""
        listener = make_listener()
        payload = logs_payload(build_trade_event_payload(), PUMP_FUN_AMM_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_AMM_PROGRAM_ID)
        assert listener.trades.empty()
        assert listener.stats.dropped_events == 1
        assert len(listener._registry) == 0

    async def test_bonding_curve_logs_processed(self) -> None:
        """То же тело от программы bonding curve публикует сделку."""
        listener = make_listener()
        body = build_trade_event_payload(timestamp=int(time.time()))
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.qsize() == 1
        assert listener.stats.dropped_events == 0


class TestSanityGate:
    """Санити-гейт полей события на родной программе."""

    async def test_nanosecond_timestamp_dropped(self) -> None:
        """Время в наносекундах (мусор раскладки) — событие отброшено."""
        listener = make_listener()
        body = build_trade_event_payload(timestamp=int(1.5e18))
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.empty()
        assert listener.stats.dropped_events == 1

    async def test_inflated_order_dropped(self) -> None:
        """Ордер больше 10k SOL — не реальное событие кривой."""
        listener = make_listener()
        body = build_trade_event_payload(sol_amount=20_000 * 10**9)
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.empty()
        assert listener.stats.dropped_events == 1

    async def test_sub_virtual_reserves_dropped(self) -> None:
        """Виртуальный резерв SOL < 30 (константа кривой) — отброшено."""
        listener = make_listener()
        body = build_trade_event_payload(virtual_sol_reserves=10**9)
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.empty()

    async def test_live_timestamp_accepted(self) -> None:
        """Свежее время и нормальные резервы — событие публикуется."""
        listener = make_listener()
        body = build_trade_event_payload(timestamp=int(time.time()))
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.qsize() == 1


class TestAccountSubscriptions:
    """Подписки аккаунтов: кривая не подписывается, vault-пулы подписываются."""

    async def test_bonding_curve_not_subscribed(self) -> None:
        """Обнаружение кривой не создаёт accountSubscribe (лимит Helius)."""
        listener = make_listener()
        body = build_trade_event_payload(timestamp=int(time.time()))
        payload = logs_payload(body, PUMP_FUN_PROGRAM_ID)
        await listener._handle_logs_notification(payload, PUMP_FUN_PROGRAM_ID)
        assert listener.trades.qsize() == 1
        assert len(listener._registry) == 1
        ws = listener._ws
        assert isinstance(ws, FakeWs)
        assert ws.account_calls == []

    async def test_amm_pool_subscribes_vaults(self) -> None:
        """AMM-пул без кривой подписывает свои vault-аккаунты."""
        listener = make_listener()
        pool = TrackedPool(
            pool_address="Pool1111111111111111111111111111111111111111",
            dex=DEXType.PUMP_FUN,
            dex_program=PUMP_FUN_AMM_PROGRAM_ID,
            base_mint="Mint11111111111111111111111111111111111111111",
            quote_mint="So11111111111111111111111111111111111111112",
            base_vault="BaseVault111111111111111111111111111111111111",
            quote_vault="QuoteVault11111111111111111111111111111111111",
        )
        await listener._subscribe_pool_accounts(pool)
        ws = listener._ws
        assert isinstance(ws, FakeWs)
        assert ws.account_calls == [
            "BaseVault111111111111111111111111111111111111",
            "QuoteVault11111111111111111111111111111111111",
        ]
