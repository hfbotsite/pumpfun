"""Реестр отслеживаемых пулов.

Модуль хранит множество «живых» пулов (bonding curve Pump.fun, PumpSwap,
Raydium V4) и предоставляет быстрые индексы, необходимые слушателю:

* ``pool_address -> TrackedPool`` — при обновлении аккаунта пула;
* ``base_mint -> TrackedPool`` — при поиске пула по токену.

Все операции выполняются в одном event-loop, поэтому структуры не требуют
явных блокировок.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from models.trade_data import DEXType, PoolInfo
from utils.constants import PUMP_TOKEN_DECIMALS, SOL_DECIMALS


@dataclass(slots=True)
class TrackedPool:
    """Отслеживаемый пул с текущими резервами и метаданными подписок."""

    pool_address: str
    dex: DEXType
    dex_program: str
    base_mint: str
    quote_mint: str
    base_decimals: int = PUMP_TOKEN_DECIMALS
    quote_decimals: int = SOL_DECIMALS
    base_vault: str | None = None
    quote_vault: str | None = None
    lp_mint: str | None = None
    curve_address: str | None = None
    #: Кошелёк создателя токена (нужен для вывода creator vault в Pump.fun).
    creator: str | None = None
    base_reserve: int = 0
    quote_reserve: int = 0
    lp_supply: int | None = None
    lp_burned: int | None = None
    discovery_signature: str | None = None
    created_at: float = field(default_factory=time.time)
    last_update: float = 0.0
    subscription_tokens: list[int] = field(default_factory=list)
    #: Последняя известная цена (SOL за токен) — обновляется трекером.
    last_price: float = 0.0
    #: Накопленный объём торгов (SOL) с момента отслеживания пула.
    volume_sol: float = 0.0

    @property
    def is_ready(self) -> bool:
        """Есть ли уже ненулевые резервы по обеим сторонам."""
        return self.base_reserve > 0 and self.quote_reserve > 0

    @property
    def account_addresses(self) -> tuple[str, ...]:
        """Адреса аккаунтов, на которые нужно подписаться для потока сделок."""
        if self.curve_address is not None:
            return (self.curve_address,)
        addresses = [addr for addr in (self.base_vault, self.quote_vault) if addr]
        return tuple(addresses)

    def to_pool_info(self) -> PoolInfo:
        """Собирает снимок :class:`PoolInfo` из текущего состояния.

        Returns:
            Модель :class:`~models.trade_data.PoolInfo` с текущими резервами.
        """
        return PoolInfo(
            dex=self.dex,
            pool_address=self.pool_address,
            base_mint=self.base_mint,
            quote_mint=self.quote_mint,
            base_decimals=self.base_decimals,
            quote_decimals=self.quote_decimals,
            base_reserve=self.base_reserve,
            quote_reserve=self.quote_reserve,
            lp_mint=self.lp_mint,
            lp_supply=self.lp_supply,
            lp_burned=self.lp_burned,
            created_at=self.created_at,
            updated_at=self.last_update,
            volume_sol=self.volume_sol,
        )


class PoolRegistry:
    """Хранилище отслеживаемых пулов и обратных индексов."""

    __slots__ = ("_by_mint", "_pools")

    def __init__(self) -> None:
        """Инициализирует пустой реестр."""
        self._pools: dict[str, TrackedPool] = {}
        self._by_mint: dict[str, str] = {}

    def __len__(self) -> int:
        """Число отслеживаемых пулов."""
        return len(self._pools)

    def __contains__(self, pool_address: object) -> bool:
        """Проверяет наличие пула в реестре."""
        return pool_address in self._pools

    def register(self, pool: TrackedPool) -> TrackedPool:
        """Добавляет или обновляет пул в реестре.

        Args:
            pool: Описание пула.

        Returns:
            Зарегистрированный объект (существующий при повторной регистрации).
        """
        existing = self._pools.get(pool.pool_address)
        if existing is not None:
            existing.base_mint = pool.base_mint or existing.base_mint
            existing.quote_mint = pool.quote_mint or existing.quote_mint
            existing.base_vault = pool.base_vault or existing.base_vault
            existing.quote_vault = pool.quote_vault or existing.quote_vault
            existing.lp_mint = pool.lp_mint or existing.lp_mint
            existing.curve_address = pool.curve_address or existing.curve_address
            existing.creator = pool.creator or existing.creator
            existing.base_decimals = pool.base_decimals
            existing.quote_decimals = pool.quote_decimals
            self._index(existing)
            return existing

        self._pools[pool.pool_address] = pool
        self._index(pool)
        return pool

    def _index(self, pool: TrackedPool) -> None:
        """Обновляет обратный индекс mint → пул.

        Args:
            pool: Пул для индексации.
        """
        if pool.base_mint:
            self._by_mint[pool.base_mint] = pool.pool_address

    def get(self, pool_address: str) -> TrackedPool | None:
        """Возвращает пул по адресу.

        Args:
            pool_address: Адрес пула.

        Returns:
            :class:`TrackedPool` или ``None``.
        """
        return self._pools.get(pool_address)

    def get_by_mint(self, base_mint: str) -> TrackedPool | None:
        """Возвращает пул по mint базового токена.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            :class:`TrackedPool` или ``None``.
        """
        pool_address = self._by_mint.get(base_mint)
        if pool_address is None:
            return None
        return self._pools.get(pool_address)

    def remove(self, pool_address: str) -> TrackedPool | None:
        """Удаляет пул и его индексы.

        Args:
            pool_address: Адрес удаляемого пула.

        Returns:
            Удалённый объект или ``None``.
        """
        pool = self._pools.pop(pool_address, None)
        if pool is None:
            return None
        if pool.base_mint and self._by_mint.get(pool.base_mint) == pool_address:
            self._by_mint.pop(pool.base_mint, None)
        return pool

    def prune_stale(self, max_age_seconds: float) -> list[str]:
        """Удаляет пулы, не обновлявшиеся дольше указанного времени.

        Args:
            max_age_seconds: Максимальный возраст «молчания» пула, c.

        Returns:
            Список адресов удалённых пулов.
        """
        if max_age_seconds <= 0:
            return []
        now = time.time()
        stale = [
            pool.pool_address
            for pool in self._pools.values()
            if pool.last_update > 0 and now - pool.last_update > max_age_seconds
        ]
        for pool_address in stale:
            self.remove(pool_address)
        return stale
