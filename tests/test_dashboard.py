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


class TestAuthAndActions:
    """Аутентификация и ручные действия (кнопки)."""

    async def _start_auth(self) -> tuple[DashboardServer, str, list[dict]]:
        """Сервер с включённой аутентификацией и записью вызовов действий.

        Returns:
            Кортеж (сервер, base_url, журнал вызовов /api/action).
        """
        calls: list[dict] = []

        async def handler(payload: dict) -> dict:
            calls.append(payload)
            return {"ok": True, "echo": payload}

        server = DashboardServer(
            lambda: {"session": {"mode": "live"}},
            host="127.0.0.1",
            port=0,
            actions=handler,
            auth_user="home",
            auth_password="123",
        )
        await server.start()
        sockets = server._runner.addresses
        base = f"http://127.0.0.1:{sockets[0][1]}"
        return server, base, calls

    async def test_protected_endpoints_require_token(self) -> None:
        """Без токена snapshot/stream/action отвечают 401."""
        server, base, calls = await self._start_auth()
        try:
            async with aiohttp.ClientSession() as http:
                for path in ("/api/snapshot", "/api/stream"):
                    async with http.get(f"{base}{path}") as resp:
                        assert resp.status == 401
                async with http.post(
                    f"{base}/api/action", json={"action": "sell_all"}
                ) as resp:
                    assert resp.status == 401
            assert calls == []
        finally:
            await server.close()

    async def test_login_and_authorized_access(self) -> None:
        """Верный логин/пароль дают токен; с ним всё доступно."""
        server, base, calls = await self._start_auth()
        try:
            async with aiohttp.ClientSession() as http:
                bad = await http.post(
                    f"{base}/api/login",
                    json={"user": "home", "password": "wrong"},
                )
                assert bad.status == 401
                good = await http.post(
                    f"{base}/api/login",
                    json={"user": "home", "password": "123"},
                )
                assert good.status == 200
                token = (await good.json())["token"]
                headers = {"Authorization": f"Bearer {token}"}
                async with http.get(
                    f"{base}/api/snapshot", headers=headers
                ) as resp:
                    assert resp.status == 200
                async with http.post(
                    f"{base}/api/action",
                    headers=headers,
                    json={"action": "sell", "mint": "MintAbc"},
                ) as resp:
                    assert resp.status == 200
                    assert (await resp.json())["ok"] is True
            assert calls == [{"action": "sell", "mint": "MintAbc"}]
        finally:
            await server.close()

    async def test_token_in_query_for_sse(self) -> None:
        """EventSource не умеет заголовки — токен работает и через query."""
        server, base, _calls = await self._start_auth()
        try:
            async with aiohttp.ClientSession() as http:
                login = await http.post(
                    f"{base}/api/login",
                    json={"user": "home", "password": "123"},
                )
                token = (await login.json())["token"]
                async with http.get(
                    f"{base}/api/stream?token={token}"
                ) as resp:
                    assert resp.status == 200
                async with http.get(f"{base}/api/stream?token=bad") as resp:
                    assert resp.status == 401
        finally:
            await server.close()

    async def test_action_disabled_returns_503(self) -> None:
        """Без обработчика действий — 503 (например, --no-dashboard-режимы)."""
        server = DashboardServer(
            lambda: {},
            host="127.0.0.1",
            port=0,
        )
        await server.start()
        try:
            base = f"http://127.0.0.1:{server._runner.addresses[0][1]}"
            async with aiohttp.ClientSession() as http, http.post(
                f"{base}/api/action", json={"action": "sell_all"}
            ) as resp:
                assert resp.status == 503
        finally:
            await server.close()


class TestDashboardActionHandler:
    """Обработчик действий пайплайна (без запуска компонентов)."""

    def test_unknown_action(self) -> None:
        pipeline = LivePipeline(dry_run=True)

        async def run() -> dict:
            return await pipeline._dashboard_action({"action": "nuke"})

        result = asyncio.run(run())
        assert result["ok"] is False
        assert "неизвестное действие" in str(result["error"])

    def test_pipeline_not_started(self) -> None:
        pipeline = LivePipeline(dry_run=True)

        async def run() -> dict:
            return await pipeline._dashboard_action({"action": "sell_all"})

        result = asyncio.run(run())
        assert result["ok"] is False
        assert "не запущен" in str(result["error"])


class TestShutdown:
    """Полная остановка бота с дашборда."""

    async def test_shutdown_calls_handler(self) -> None:
        """POST /api/shutdown вызывает обработчик остановки."""
        called: list[bool] = []

        async def on_shutdown() -> None:
            called.append(True)

        server = DashboardServer(
            lambda: {}, host="127.0.0.1", port=0, on_shutdown=on_shutdown
        )
        await server.start()
        try:
            base = f"http://127.0.0.1:{server._runner.addresses[0][1]}"
            async with aiohttp.ClientSession() as http, http.post(
                f"{base}/api/shutdown"
            ) as resp:
                assert resp.status == 200
                assert (await resp.json())["ok"] is True
            await asyncio.sleep(0.7)  # delayed-задача (0.5 c)
            assert called == [True]
        finally:
            await server.close()

    async def test_shutdown_disabled_returns_503(self) -> None:
        """Без обработчика — 503 (кнопка не должна умирать молча)."""
        server = DashboardServer(lambda: {}, host="127.0.0.1", port=0)
        await server.start()
        try:
            base = f"http://127.0.0.1:{server._runner.addresses[0][1]}"
            async with aiohttp.ClientSession() as http, http.post(
                f"{base}/api/shutdown"
            ) as resp:
                assert resp.status == 503
        finally:
            await server.close()

    def test_pipeline_shutdown_is_soft_stop(self) -> None:
        """Остановка с дашборда — мягкая: торговля выключается, процесс живёт."""
        pipeline = LivePipeline(dry_run=True)
        asyncio.run(pipeline._dashboard_shutdown())
        assert pipeline._trading_active is False
        # Процесс не помечен как полностью остановленный — дашборд живёт.
        assert not pipeline._fully_stopped.is_set()
