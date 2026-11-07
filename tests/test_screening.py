"""Тесты контура отбора токенов: правила, очередь, провайдер, ре-скрин."""

from __future__ import annotations

import asyncio
import time

from core.persistence import PortfolioStore
from core.screening import TokenScreeningService
from models.trade_data import DEXType, PoolInfo, ScreeningVerdict, TokenSecurity
from tests.conftest import BASE_MINT, make_pool


def clean_security() -> TokenSecurity:
    """Безопасный контракт: authorities сняты, LP сожжён на 100%."""
    return TokenSecurity(mint_authority=None, freeze_authority=None, lp_burned_pct=100.0)


class FakeResolver:
    """Резолвер-заглушка: отдаёт заготовленный результат и считает вызовы."""

    def __init__(self, result: TokenSecurity | None) -> None:
        """Сохраняет результат для всех вызовов.

        Args:
            result: Что возвращать (или ``None`` для имитации сбоя RPC).
        """
        self.result = result
        self.calls = 0

    async def resolve(
        self,
        *,
        base_mint: str,
        dex: DEXType,
        lp_mint: str | None = None,
        lp_supply: int | None = None,
        lp_burned: int | None = None,
    ) -> TokenSecurity | None:
        """Возвращает заготовленный результат, инкрементируя счётчик.

        Args:
            base_mint: Mint мемкоина.
            dex: DEX/платформа пула.
            lp_mint: Mint LP (игнорируется).
            lp_supply: Supply LP (игнорируется).
            lp_burned: Сожжённый LP (игнорируется).

        Returns:
            Заготовленный результат.
        """
        self.calls += 1
        await asyncio.sleep(0)  # имитация IO
        return self.result


async def wait_until(condition, timeout: float = 2.0) -> None:
    """Ждёт выполнения условия с таймаутом (для фонового воркера).

    Args:
        condition: Функция-предикат.
        timeout: Таймаут, c.

    Raises:
        AssertionError: Если условие не выполнилось вовремя.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("условие не выполнено за таймаут")


class TestScreenRules:
    """Правила применения security-контура в screen()."""

    async def test_clean_token_approved(self) -> None:
        """Чистый контракт получает одобрение с данными проверки."""
        service = TokenScreeningService(FakeResolver(clean_security()))
        verdict = await service.screen(make_pool())
        assert verdict.approved
        assert verdict.base_mint == BASE_MINT
        assert verdict.reason == "контракт чист"
        assert verdict.lp_burned_pct == 100.0
        assert verdict.checked_at > 0

    async def test_mint_authority_rejected(self) -> None:
        """Активный mint authority — отказ с адресом в причине."""
        dirty = clean_security().model_copy(update={"mint_authority": "Auth"})
        service = TokenScreeningService(FakeResolver(dirty))
        verdict = await service.screen(make_pool())
        assert not verdict.approved
        assert "mint authority" in verdict.reason
        assert verdict.mint_authority == "Auth"

    async def test_freeze_authority_rejected(self) -> None:
        """Активный freeze authority — отказ."""
        dirty = clean_security().model_copy(update={"freeze_authority": "Freeze"})
        service = TokenScreeningService(FakeResolver(dirty))
        verdict = await service.screen(make_pool())
        assert not verdict.approved
        assert "freeze authority" in verdict.reason

    async def test_low_lp_burn_rejected(self) -> None:
        """LP сожжён ниже порога — отказ."""
        dirty = clean_security().model_copy(update={"lp_burned_pct": 40.0})
        service = TokenScreeningService(FakeResolver(dirty))
        verdict = await service.screen(make_pool())
        assert not verdict.approved
        assert "LP" in verdict.reason

    async def test_missing_security_rejected(self) -> None:
        """Сбой RPC (нет данных) — отказ (fail-closed)."""
        service = TokenScreeningService(FakeResolver(None))
        verdict = await service.screen(make_pool())
        assert not verdict.approved
        assert "нет данных" in verdict.reason
        assert verdict.mint_authority is None


class TestWorkerAndProvider:
    """Очередь, воркер и горячий путь провайдера."""

    async def test_submit_then_worker_approves(self) -> None:
        """submit → воркер → свежий вердикт → провайдер отдаёт данные."""
        resolver = FakeResolver(clean_security())
        service = TokenScreeningService(resolver)
        await service.start()
        try:
            assert service.submit(make_pool()) is True
            await wait_until(lambda: service.verdict(BASE_MINT) is not None)
            verdict = service.verdict(BASE_MINT)
            assert verdict is not None and verdict.approved
            assert service.status(BASE_MINT) == "approved"
            security = await service.provider(make_pool())
            assert security is not None
            assert security.lp_burned_pct == 100.0
            # Повторный провайдер не дёргает резолвер (кэш вердикта).
            assert resolver.calls == 1
            assert service.stats.approved == 1
        finally:
            await service.stop()

    async def test_provider_without_verdict_blocks_and_enqueues(self) -> None:
        """Нет вердикта → None + токен в очереди (fail-closed, прогрев окон)."""
        service = TokenScreeningService(FakeResolver(clean_security()))
        security = await service.provider(make_pool())
        assert security is None
        assert service.status(BASE_MINT) == "pending"

    async def test_provider_rejected_returns_none(self) -> None:
        """Отклонённый токен не пропускается провайдером."""
        dirty = clean_security().model_copy(update={"mint_authority": "Auth"})
        service = TokenScreeningService(FakeResolver(dirty))
        await service.start()
        try:
            service.submit(make_pool())
            await wait_until(lambda: service.status(BASE_MINT) == "rejected")
            assert await service.provider(make_pool()) is None
            assert service.status(BASE_MINT) == "rejected"
            assert service.stats.rejected == 1
        finally:
            await service.stop()

    async def test_dedup_submit(self) -> None:
        """Повторный submit до завершения проверки игнорируется."""
        service = TokenScreeningService(FakeResolver(clean_security()))
        assert service.submit(make_pool()) is True
        assert service.submit(make_pool()) is False
        assert service.status(BASE_MINT) == "pending"

    async def test_stale_verdict_triggers_rescreen(self) -> None:
        """Устаревший вердикт → провайдер блокирует и перезапускает проверку."""
        resolver = FakeResolver(clean_security())
        service = TokenScreeningService(resolver, recheck_seconds=0.05)
        await service.start()
        try:
            service.submit(make_pool())
            await wait_until(lambda: service.verdict(BASE_MINT) is not None)
            assert resolver.calls == 1
            await asyncio.sleep(0.08)  # вердикт устарел
            assert service.verdict(BASE_MINT) is None
            assert service.status(BASE_MINT) == "stale"
            assert await service.provider(make_pool()) is None
            assert service.stats.resubmitted == 1
            await wait_until(lambda: resolver.calls >= 2)
        finally:
            await service.stop()

    async def test_on_rejected_callback_fires(self) -> None:
        """Колбэк on_rejected вызывается с отклоняющим вердиктом."""
        rejected: list[ScreeningVerdict] = []

        async def callback(verdict: ScreeningVerdict) -> None:
            rejected.append(verdict)

        dirty = clean_security().model_copy(update={"freeze_authority": "F"})
        service = TokenScreeningService(
            FakeResolver(dirty), on_rejected=callback
        )
        await service.start()
        try:
            service.submit(make_pool())
            await wait_until(lambda: len(rejected) == 1)
            assert rejected[0].approved is False
        finally:
            await service.stop()

    async def test_load_persisted_without_store_is_zero(self) -> None:
        """Без store загрузка вердиктов — пустая операция."""
        service = TokenScreeningService(FakeResolver(clean_security()))
        assert service.load_persisted() == 0


class TestPersistenceRoundtrip:
    """Сохранение вердиктов в SQLite и загрузка после «рестарта»."""

    async def test_verdicts_survive_restart(self, tmp_path) -> None:
        """Вердикты сохраняются в БД и восстанавливаются новой службой."""
        db_path = tmp_path / "bot.db"
        store = PortfolioStore(db_path)
        dirty = clean_security().model_copy(update={"mint_authority": "Auth"})
        service = TokenScreeningService(
            FakeResolver(dirty), store=store, recheck_seconds=0
        )
        await service.start()
        try:
            service.submit(make_pool())
            await wait_until(lambda: service.stats.screened == 1)
        finally:
            await service.stop()
        store.close()

        store2 = PortfolioStore(db_path)
        service2 = TokenScreeningService(FakeResolver(clean_security()), store=store2)
        assert service2.load_persisted() == 1
        verdict = service2.verdict(BASE_MINT)
        assert verdict is not None
        assert verdict.approved is False
        assert "mint authority" in verdict.reason
        # Провайдер не пропустит восстановленный «отказ».
        assert await service2.provider(make_pool()) is None
        store2.close()

    def test_screening_table_roundtrip(self, tmp_path) -> None:
        """Прямой раунд-трип save/load вердикта в хранилище."""
        store = PortfolioStore(tmp_path / "rt.db")
        verdict = ScreeningVerdict(
            base_mint="MintA",
            approved=True,
            reason="контракт чист",
            mint_authority=None,
            freeze_authority=None,
            lp_burned_pct=100.0,
            checked_at=1700.0,
        )
        store.save_screening(verdict)
        loaded = store.load_screenings()
        assert len(loaded) == 1
        assert loaded[0].base_mint == "MintA"
        assert loaded[0].approved is True
        assert loaded[0].checked_at == 1700.0
        # Обновление той же записи (UPSERT).
        store.save_screening(
            verdict.model_copy(update={"approved": False, "reason": "freeze"})
        )
        reloaded = store.load_screenings()
        assert len(reloaded) == 1
        assert reloaded[0].approved is False
        assert reloaded[0].reason == "freeze"
        store.close()


def test_pool_info_snapshot_for_screening() -> None:
    """Фабричный пул несёт данные, нужные службе отбора."""
    pool: PoolInfo = make_pool()
    assert pool.base_mint == BASE_MINT
    assert pool.dex == DEXType.PUMP_FUN
