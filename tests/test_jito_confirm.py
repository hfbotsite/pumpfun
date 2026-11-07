"""Тесты ожидания подтверждения Jito-бандла (без сети)."""

from __future__ import annotations

from core.jito import JitoBundleStatus, JitoClient
from models.trade_data import OrderResult, TradeSide


def failed_order() -> OrderResult:
    """Результат неуспешного ордера для негативных проверок."""
    return OrderResult.failed(
        side=TradeSide.BUY,
        base_mint="Mint11111111111111111111111111111111111111111",
        pool_address="Pool1111111111111111111111111111111111111111",
        error="test",
    )


class StubJito(JitoClient):
    """JitoClient с подменённым опросом статусов (без HTTP)."""

    def __init__(self, statuses: list[JitoBundleStatus]) -> None:
        """Принимает очередь статусов, отдаваемых по одному за опрос.

        Args:
            statuses: Последовательность статусов.
        """
        super().__init__("https://stub.test")
        self._statuses = list(statuses)
        self.polls = 0

    async def get_bundle_statuses(self, bundle_ids: list) -> dict:
        """Отдаёт следующий статус из заготовленной очереди.

        Args:
            bundle_ids: ID бандлов (игнорируется).

        Returns:
            Словарь с одним статусом.
        """
        self.polls += 1
        if self._statuses:
            status = self._statuses.pop(0)
        else:
            status = JitoBundleStatus(bundle_id="b", status="Pending")
        return {status.bundle_id: status}


class TestAwaitConfirmation:
    """await_bundle_confirmation: landed / failed / timeout."""

    async def test_landed_immediately(self) -> None:
        """Статус Landed подтверждает бандл с первого опроса."""
        stub = StubJito(
            [JitoBundleStatus(bundle_id="b", status="Landed", landed_slot=42)]
        )
        status = await stub.await_bundle_confirmation(
            "b", timeout_seconds=2.0, poll_seconds=0.05
        )
        assert status.status == "Landed"
        assert status.landed_slot == 42
        assert stub.polls == 1

    async def test_failed_stops_polling(self) -> None:
        """Терминальный статус Failed завершает ожидание сразу."""
        stub = StubJito(
            [JitoBundleStatus(bundle_id="b", status="Failed", error="boom")]
        )
        status = await stub.await_bundle_confirmation(
            "b", timeout_seconds=5.0, poll_seconds=0.05
        )
        assert status.status == "Failed"
        assert status.error == "boom"
        assert stub.polls == 1

    async def test_pending_until_timeout(self) -> None:
        """Вечный Pending истекает по таймауту с ошибкой последнего опроса."""
        stub = StubJito([])
        status = await stub.await_bundle_confirmation(
            "b", timeout_seconds=0.2, poll_seconds=0.05
        )
        assert status.status == "Pending"
        assert stub.polls > 1

    async def test_pending_then_landed(self) -> None:
        """Переход Pending → Landed подтверждает бандл."""
        stub = StubJito(
            [
                JitoBundleStatus(bundle_id="b", status="Pending"),
                JitoBundleStatus(bundle_id="b", status="Landed", landed_slot=7),
            ]
        )
        status = await stub.await_bundle_confirmation(
            "b", timeout_seconds=2.0, poll_seconds=0.05
        )
        assert status.status == "Landed"
        assert status.landed_slot == 7

    async def test_invalid_is_terminal(self) -> None:
        """Статус Invalid терминален, как и Failed."""
        stub = StubJito([JitoBundleStatus(bundle_id="b", status="Invalid")])
        status = await stub.await_bundle_confirmation(
            "b", timeout_seconds=5.0, poll_seconds=0.05
        )
        assert status.status == "Invalid"
        assert stub.polls == 1


class TestOrderResultConfirmed:
    """Поле confirmed в OrderResult."""

    def test_failed_result_not_confirmed(self) -> None:
        """Неуспешный ордер всегда not confirmed."""
        result = failed_order()
        assert result.success is False
        assert result.confirmed is False
        assert result.landed_slot is None

    def test_success_defaults_not_confirmed(self) -> None:
        """Успешный ордер без явного подтверждения — not confirmed."""
        result = OrderResult(
            success=True,
            side=TradeSide.BUY,
            base_mint="m",
            pool_address="p",
        )
        assert result.confirmed is False
