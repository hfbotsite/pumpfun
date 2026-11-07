"""Тесты веб-дашборда: HTTP/SSE-сервер и сборщик снимка пайплайна."""

from __future__ import annotations

import asyncio
import json

import aiohttp
import pytest

from core.dashboard import DashboardServer
from core.pipeline import LivePipeline, PipelineStats
from core.screening import TokenScreeningService
from models.trade_data import TokenSecurity
from tests.conftest import make_pool
from tests.test_screening import FakeResolver, clean_security, wait_until
from utils.logger import get_logger


class TestDashboardServer:
    """HTTP-эндпоинты сервера на свободном порту (port=0)."""

    async def _start(self, provider) -> tuple[DashboardServer, str]:
        """Запускает сервер и возвращает его с базовым URL.

        Args:
            provider: Функция сборки снимка.

        Returns:
            Кортеж (сервер, base_url).
        """
        server = DashboardServer(provider, host="127.0.0.1", port=0, interval_seconds=0.2)
        await server.start()
        sockets = server._runner.addresses
        port = sockets[0][1]
        return server, f"http://127.0.0.1:{port}"

    async def test_page_and_snapshot(self) -> None:
        """Страница отдаёт HTML, снимок — JSON провайдера."""
        payload = {"session": {"mode": "live"}, "positions": []}
        server, base = await self._start(lambda: payload)
        try:
            async with aiohttp.ClientSession() as http:
                async with http.get(f"{base}/") as page:
                    assert page.status == 200
                    assert "PUMP SCALPER" in await page.text()
                async with http.get(f"{base}/api/snapshot") as snap:
                    assert snap.status == 200
                    assert (await snap.json())["session"]["mode"] == "live"
        finally:
            await server.close()

    async def test_sse_pushes_snapshots(self) -> None:
        """SSE-поток присылает JSON-снимок без ожидания полного таймаута."""
        payload = {"session": {"mode": "dry-run"}, "stats": {"pnl_sol": 0.5}}
        server, base = await self._start(lambda: payload)
        try:
            async with aiohttp.ClientSession() as http, http.get(
                f"{base}/api/stream"
            ) as stream:
                assert stream.status == 200
                assert stream.content_type == "text/event-stream"
                first = await asyncio.wait_for(stream.content.readline(), 3.0)
                assert first.startswith(b"data: ")
                decoded = json.loads(first[len(b"data: "):].decode("utf-8"))
                assert decoded["session"]["mode"] == "dry-run"
        finally:
            await server.close()

    async def test_log_tail_captures(self) -> None:
        """attach_logger дублирует записи лога в кольцевой буфер."""
        server, _ = await self._start(lambda: {})
        server.attach_logger()
        try:
            get_logger().warning("тестовое сообщение дашборда")
            await asyncio.sleep(0.05)
            tail = server.log_tail()
            assert any("тестовое сообщение дашборда" in line for line in tail)
        finally:
            await server.close()


class TestPipelineSnapshot:
    """Сборщик снимка пайплайна (защитный, без запуска компонентов)."""

    def test_snapshot_without_start(self) -> None:
        """До старта снимок валиден: пустые секции, dry-run режим."""
        pipeline = LivePipeline(dry_run=True)
        snapshot = pipeline.dashboard_snapshot()
        assert snapshot["session"]["mode"] == "dry-run"
        assert snapshot["positions"] == []
        assert snapshot["stats"]["closed"] == 0
        assert snapshot["stream"]["events"] == 0
        assert snapshot["pnl_curve"] == []

    def test_stats_counters_present(self) -> None:
        """Счётчики покупок/продаж в PipelineStats и as_dict."""
        stats = PipelineStats(buys=7, sells=3)
        assert stats.as_dict()["buys"] == 7
        assert stats.as_dict()["sells"] == 3


class TestScreeningCounts:
    """Счётчики отбора для дашборда."""

    async def test_counts_and_rejections(self, tmp_path) -> None:
        """counts отражает вердикты, recent_rejections отдаёт причины."""
        service = TokenScreeningService(FakeResolver(clean_security()))
        await service.start()
        try:
            service.submit(make_pool())
            await wait_until(lambda: service.counts()["approved"] == 1)
        finally:
            await service.stop()
        counts = service.counts()
        assert counts["approved"] == 1
        assert counts["rejected"] == 0

        dirty = TokenSecurity(
            mint_authority="Auth", freeze_authority=None, lp_burned_pct=100.0
        )
        service2 = TokenScreeningService(FakeResolver(dirty))
        await service2.start()
        try:
            service2.submit(make_pool())
            await wait_until(lambda: service2.counts()["rejected"] == 1)
            rejections = service2.recent_rejections()
            assert len(rejections) == 1
            assert "mint authority" in rejections[0].reason
        finally:
            await service2.stop()


@pytest.mark.asyncio
async def test_server_close_is_idempotent() -> None:
    """Повторный close не падает."""
    server = DashboardServer(lambda: {}, host="127.0.0.1", port=0)
    await server.start()
    await server.close()
    await server.close()
