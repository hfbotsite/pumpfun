"""Контур отбора токенов до торговли (security screening).

Модуль выделяет проверку контракта в отдельный контур, отделённый от
горячего пути торговли: каждый **новый** mint при первом обнаружении
попадает в очередь службы, проверяется по правилу security-контура
(``MIN_LP_BURNED_PCT``) и получает вердикт — задолго до любого торгового
сигнала. Проверки mint/freeze authority убраны (решение от 2026-11-09):
«нет данных безопасности» не отклоняет токен — его оценивают фильтры входа.

Ключевые свойства:

* **ноль RPC по безопасности в горячем пути** — трейдер через
  :meth:`TokenScreeningService.provider` читает готовый вердикт из кэша;
* **fail-closed**: нет вердикта или вердикт устарел → провайдер возвращает
  ``None`` (SecurityGuard отклонит вход), событие не блокируется — окно
  импульса прогревается, пока проверка идёт (~100–300 мс);
* **аудит**: вердикты пишутся в SQLite-таблицу ``token_screening``
  (опциональный store) и переживают рестарт (:meth:`load_persisted`);
* **ре-скрин**: вердикты (и одобренные, и отклонённые) устаревают через
  ``SCREEN_RECHECK_SECONDS`` — authorities могут быть сняты позже, токен
  может «вылечиться»; при устаревании mint заново ставится в очередь;
* **реакция на отказ**: колбэк ``on_rejected`` позволяет пайплайну вычистить
  пул и индикаторы отклонённого токена.

Служба не принимает торговых решений и не отправляет ордеров — только
вердикт допуска и его хранение.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from config.settings import settings
from core.persistence import PortfolioStore
from models.trade_data import DEXType, PoolInfo, ScreeningVerdict, TokenSecurity
from utils.logger import get_logger

logger = get_logger()

#: Ёмкость очереди на проверку (защита от всплесков новых токенов).
DEFAULT_QUEUE_SIZE: int = 1024


class ScreeningResolver(Protocol):
    """Поставщик данных безопасности (duck-typing для тестируемости)."""

    async def resolve(
        self,
        *,
        base_mint: str,
        dex: DEXType,
        lp_mint: str | None = None,
        lp_supply: int | None = None,
        lp_burned: int | None = None,
    ) -> TokenSecurity | None:
        """Возвращает данные безопасности токена или ``None``."""
        ...


#: Колбэк реакции на отклонение токена (очистка пула/индикаторов).
RejectedCallback = Callable[[ScreeningVerdict], Awaitable[None]]


@dataclass(slots=True)
class ScreeningStats:
    """Счётчики работы службы отбора.

    Attributes:
        screened: Проверено токенов.
        approved: Одобрено к торговле.
        rejected: Отклонено (с причиной).
        dropped: Потеряно при переполнении очереди.
        resubmitted: Повторно поставлено в очередь (ре-скрин).
    """

    screened: int = 0
    approved: int = 0
    rejected: int = 0
    dropped: int = 0
    resubmitted: int = 0

    def as_dict(self) -> dict[str, int]:
        """Сводка счётчиков для логов.

        Returns:
            Словарь счётчиков.
        """
        return {
            "screened": self.screened,
            "approved": self.approved,
            "rejected": self.rejected,
            "dropped": self.dropped,
            "resubmitted": self.resubmitted,
        }


class TokenScreeningService:
    """Фоновая служба отбора токенов по правилам безопасности.

    Держит очередь на проверку, кэш вердиктов и (опционально) SQLite-store.
    Единая точка применения правил security-контура.
    """

    def __init__(
        self,
        resolver: ScreeningResolver,
        *,
        store: PortfolioStore | None = None,
        on_rejected: RejectedCallback | None = None,
        recheck_seconds: float | None = None,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        """Инициализирует службу (без сетевых вызовов).

        Args:
            resolver: Поставщик данных безопасности (RPC-резолвер).
            store: SQLite-хранилище для аудита вердиктов (``None`` — память).
            on_rejected: Асинхронный колбэк при отклонении токена.
            recheck_seconds: TTL вердикта до повторной проверки (``None`` —
                из настроек).
            queue_size: Ёмкость очереди проверки.
        """
        self._resolver = resolver
        self._store = store
        self._on_rejected = on_rejected
        self._recheck_seconds = (
            settings.screen_recheck_seconds
            if recheck_seconds is None
            else recheck_seconds
        )
        self._queue: asyncio.Queue[PoolInfo] = asyncio.Queue(maxsize=queue_size)
        self._queued: set[str] = set()
        self._verdicts: dict[str, ScreeningVerdict] = {}
        self._task: asyncio.Task[None] | None = None
        self.stats = ScreeningStats()

    # --------------------------------------------------------------- Свойства
    @property
    def recheck_seconds(self) -> float:
        """TTL вердикта до повторной проверки, c."""
        return self._recheck_seconds

    # ---------------------------------------------------------- Жизненный цикл
    async def start(self) -> None:
        """Запускает фоновый воркер проверки. Идемпотентно."""
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(
            self._worker(), name="token-screening-worker"
        )
        logger.info("Служба отбора токенов запущена")

    async def stop(self) -> None:
        """Останавливает фоновый воркер. Идемпотентно."""
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        logger.info("Служба отбора токенов остановлена: {}", self.stats.as_dict())

    def load_persisted(self) -> int:
        """Загружает вердикты из SQLite (после рестарта).

        Returns:
            Число загруженных вердиктов.
        """
        if self._store is None:
            return 0
        for verdict in self._store.load_screenings():
            self._verdicts[verdict.base_mint] = verdict
        logger.info("Вердиктов отбора загружено из БД: {}", len(self._verdicts))
        return len(self._verdicts)

    # ---------------------------------------------------------------- Публичное
    def submit(self, pool: PoolInfo) -> bool:
        """Ставит токен в очередь проверки (идемпотентно по mint).

        Args:
            pool: Снимок пула (нужен для проверки LP).

        Returns:
            ``True``, если токен был добавлен в очередь.
        """
        mint = pool.base_mint
        if mint in self._queued:
            return False
        if self.verdict(mint) is not None:
            return False  # свежий вердикт уже есть
        try:
            self._queue.put_nowait(pool)
        except asyncio.QueueFull:
            self.stats.dropped += 1
            logger.warning("Очередь отбора переполнена, {} потерян", mint)
            return False
        self._queued.add(mint)
        return True

    def verdict(self, base_mint: str) -> ScreeningVerdict | None:
        """Возвращает **свежий** вердикт токена.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            :class:`~models.trade_data.ScreeningVerdict` или ``None``, если
            вердикта нет или он устарел.
        """
        verdict = self._verdicts.get(base_mint)
        if verdict is None or self._is_stale(verdict):
            return None
        return verdict

    def status(self, base_mint: str) -> str:
        """Человекочитаемый статус токена (для логов и диагностики).

        Args:
            base_mint: Mint мемкоина.

        Returns:
            ``approved`` / ``rejected`` / ``stale`` / ``pending`` / ``unknown``.
        """
        verdict = self._verdicts.get(base_mint)
        if verdict is None:
            return "pending" if base_mint in self._queued else "unknown"
        if self._is_stale(verdict):
            return "stale"
        return "approved" if verdict.approved else "rejected"

    def counts(self) -> dict[str, int]:
        """Счётчики вердиктов для дашборда/логов (только свежие).

        Returns:
            Словарь ``pending/approved/rejected``.
        """
        approved = sum(
            1
            for v in self._verdicts.values()
            if v.approved and not self._is_stale(v)
        )
        rejected = sum(
            1
            for v in self._verdicts.values()
            if not v.approved and not self._is_stale(v)
        )
        return {
            "pending": len(self._queued),
            "approved": approved,
            "rejected": rejected,
        }

    def recent_rejections(self, limit: int = 5) -> list[ScreeningVerdict]:
        """Последние отклонения (для дашборда: кто и почему не торгуется).

        Args:
            limit: Сколько вердиктов вернуть.

        Returns:
            Список отклоняющих вердиктов, новые впереди.
        """
        rejected = [v for v in self._verdicts.values() if not v.approved]
        rejected.sort(key=lambda v: v.checked_at, reverse=True)
        return rejected[:limit]

    async def provider(self, pool: PoolInfo) -> TokenSecurity | None:
        """Провайдер данных безопасности для трейдера (горячий путь).

        Возвращает данные только из готового свежего вердикта — сетевых
        вызовов не делает. Нет вердикта → mint ставится в очередь и
        возвращается ``None`` (вход отклонится, но события продолжат
        прогревать индикаторы). Устаревший вердикт → ре-скрин и ``None``.

        Args:
            pool: Снимок пула.

        Returns:
            :class:`~models.trade_data.TokenSecurity` при одобрении, иначе
            ``None``.
        """
        mint = pool.base_mint
        verdict = self._verdicts.get(mint)
        if verdict is None:
            if self.submit(pool):
                logger.debug("Отбор: {} поставлен в очередь при первом входе", mint)
            return None
        if self._is_stale(verdict):
            if self.submit(pool):
                self.stats.resubmitted += 1
            return None
        if not verdict.approved:
            return None
        return verdict.to_security()

    # -------------------------------------------------------------- Проверка
    async def screen(self, pool: PoolInfo) -> ScreeningVerdict:
        """Проверяет контракт токена по правилам security-контура.

        Args:
            pool: Снимок пула.

        Returns:
            :class:`~models.trade_data.ScreeningVerdict` (approved/rejected).
        """
        security = await self._resolver.resolve(
            base_mint=pool.base_mint,
            dex=pool.dex,
            lp_mint=pool.lp_mint,
            lp_supply=pool.lp_supply,
            lp_burned=pool.lp_burned,
        )
        if security is None:
            # Данных безопасности нет (RPC-сбой/новизна токена): токен НЕ
            # отклоняем — его оценивают остальные фильтры входа (ликвидность,
            # импульс). lp_burned_pct=100 = «нет применимых LP-данных».
            return ScreeningVerdict(
                base_mint=pool.base_mint,
                approved=True,
                reason=(
                    "данные безопасности недоступны — допущен по фильтрам "
                    "входа (ликвидность, импульс)"
                ),
                lp_burned_pct=100.0,
                checked_at=time.time(),
            )
        if security.lp_burned_pct < settings.min_lp_burned_pct:
            return self._rejected(
                pool.base_mint,
                security,
                f"LP сожжён лишь на {security.lp_burned_pct:.2f}% "
                f"(< {settings.min_lp_burned_pct:.2f}%)",
            )
        return ScreeningVerdict(
            base_mint=pool.base_mint,
            approved=True,
            reason="контракт чист",
            mint_authority=security.mint_authority,
            freeze_authority=security.freeze_authority,
            lp_burned_pct=security.lp_burned_pct,
            checked_at=time.time(),
        )

    # -------------------------------------------------------------- Внутреннее
    def _rejected(
        self, mint: str, security: TokenSecurity | None, reason: str
    ) -> ScreeningVerdict:
        """Собирает отклоняющий вердикт с данными проверки.

        Args:
            mint: Mint мемкоина.
            security: Данные безопасности (могут быть ``None``).
            reason: Причина отказа.

        Returns:
            :class:`~models.trade_data.ScreeningVerdict` с ``approved=False``.
        """
        return ScreeningVerdict(
            base_mint=mint,
            approved=False,
            reason=reason,
            mint_authority=security.mint_authority if security else None,
            freeze_authority=security.freeze_authority if security else None,
            lp_burned_pct=security.lp_burned_pct if security else 0.0,
            checked_at=time.time(),
        )

    def _is_stale(self, verdict: ScreeningVerdict) -> bool:
        """Проверяет устаревание вердикта.

        Args:
            verdict: Проверяемый вердикт.

        Returns:
            ``True``, если вердикту больше ``recheck_seconds``.
        """
        if self._recheck_seconds <= 0:
            return False
        return verdict.age_seconds() >= self._recheck_seconds

    async def _worker(self) -> None:
        """Фоновый цикл: берёт пулы из очереди и проверяет их."""
        while True:
            pool = await self._queue.get()
            self._queued.discard(pool.base_mint)
            try:
                verdict = await self.screen(pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ошибка проверки токена {}", pool.base_mint)
                continue
            self.stats.screened += 1
            if verdict.approved:
                self.stats.approved += 1
            else:
                self.stats.rejected += 1
                logger.warning(
                    "Отбор: {} отклонён — {}", verdict.base_mint, verdict.reason
                )
            self._verdicts[verdict.base_mint] = verdict
            if self._store is not None:
                try:
                    self._store.save_screening(verdict)
                except Exception:
                    logger.exception(
                        "Не удалось сохранить вердикт {} в БД", verdict.base_mint
                    )
            if not verdict.approved and self._on_rejected is not None:
                try:
                    await self._on_rejected(verdict)
                except Exception:
                    logger.exception(
                        "Колбэк on_rejected упал на {}", verdict.base_mint
                    )
