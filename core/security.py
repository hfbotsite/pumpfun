"""Резолвер безопасности токен-контракта (mint authorities и LP burn).

Модуль читает on-chain данные через :class:`~core.client.SolanaRpcClient` и
собирает :class:`~models.trade_data.TokenSecurity`, необходимый фильтру
:class:`~filters.security_guard.SecurityGuard`.

Логика:

* **Mint / Freeze authority** — читается mint-аккаунт и разбирается
  :func:`~utils.spl.decode_mint`; отсутствие authority трактуется как снятие
  прав эмитента.
* **LP burn / lock** — для токенов на bonding curve Pump.fun ликвидность
  заблокирована в программе кривой, поэтому доля считается ``100%``. Для
  AMM-пулов (Raydium/PumpSwap) читаются крупнейшие держатели LP-mint, и доля
  сожжённого LP вычисляется как часть supply, удерживаемая «сжигающими»
  адресами (:data:`~utils.constants.LP_BURN_ADDRESSES`).

Результаты кэшируются с TTL, чтобы не дублировать RPC-запросы при высокой
частоте сигналов.
"""

from __future__ import annotations

import time

from solders.pubkey import Pubkey

from core.client import AccountState, RpcError, SolanaRpcClient
from models.trade_data import DEXType, PoolInfo, TokenSecurity
from utils.constants import LP_BURN_ADDRESSES
from utils.logger import get_logger
from utils.spl import SplError, decode_mint

logger = get_logger()

#: Время жизни записи кэша безопасности, c.
DEFAULT_SECURITY_TTL_SECONDS: float = 30.0

#: Смещение поля ``amount`` в SPL Token-аккаунте.
_TOKEN_AMOUNT_OFFSET: int = 64

#: Смещение поля ``owner`` в SPL Token-аккаунте.
_TOKEN_OWNER_OFFSET: int = 32

#: Минимальный размер SPL Token-аккаунта для разбора owner+amount.
_TOKEN_MIN_SIZE: int = 72


class TokenSecurityResolver:
    """Асинхронный резолвер данных безопасности токена.

    Кэширует результаты по mint-адресу, чтобы фильтр безопасности не бил по
    RPC на каждый сигнал. Для токенов на bonding curve Pump.fun LP считается
    полностью заблокированным (ликвидность заперта в программе кривой).
    """

    def __init__(
        self,
        rpc: SolanaRpcClient,
        *,
        ttl_seconds: float = DEFAULT_SECURITY_TTL_SECONDS,
    ) -> None:
        """Инициализирует резолвер.

        Args:
            rpc: HTTP RPC-клиент.
            ttl_seconds: Время жизни записи кэша, c.
        """
        self._rpc = rpc
        self._ttl = ttl_seconds
        self._cache: dict[str, tuple[TokenSecurity, float]] = {}

    # --------------------------------------------------------------- Публичное
    async def resolve_for_pool(self, pool: PoolInfo) -> TokenSecurity | None:
        """Собирает данные безопасности для пула.

        Args:
            pool: Снимок состояния пула.

        Returns:
            :class:`~models.trade_data.TokenSecurity` либо ``None``, если
            mint-аккаунт недоступен.
        """
        return await self.resolve(
            base_mint=pool.base_mint,
            dex=pool.dex,
            lp_mint=pool.lp_mint,
            lp_supply=pool.lp_supply,
            lp_burned=pool.lp_burned,
        )

    async def resolve(
        self,
        *,
        base_mint: str,
        dex: DEXType,
        lp_mint: str | None = None,
        lp_supply: int | None = None,
        lp_burned: int | None = None,
    ) -> TokenSecurity | None:
        """Собирает данные безопасности токена.

        Args:
            base_mint: Mint мемкоина.
            dex: DEX/платформа пула.
            lp_mint: Mint LP-токенов (если применимо).
            lp_supply: Общий объём LP (base units), если известен.
            lp_burned: Уже известная доля сожжённого LP (base units).

        Returns:
            :class:`~models.trade_data.TokenSecurity` либо ``None`` при ошибке
            получения данных mint.
        """
        cached = self._get_cached(base_mint)
        if cached is not None:
            return cached

        mint_info = await self._read_mint(base_mint)
        if mint_info is None:
            return None

        lp_burned_pct = await self._resolve_lp_burned_pct(
            dex=dex,
            lp_mint=lp_mint,
            lp_supply=lp_supply,
            lp_burned=lp_burned,
        )

        security = TokenSecurity(
            mint_authority=mint_info.mint_authority,
            freeze_authority=mint_info.freeze_authority,
            lp_burned_pct=lp_burned_pct,
        )
        self._cache[base_mint] = (security, time.time())
        return security

    # --------------------------------------------------------------- Внутреннее
    def _get_cached(self, base_mint: str) -> TokenSecurity | None:
        """Возвращает актуальную запись кэша, если она не устарела.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            Кэшированный :class:`TokenSecurity` или ``None``.
        """
        entry = self._cache.get(base_mint)
        if entry is None:
            return None
        security, stored_at = entry
        if time.time() - stored_at > self._ttl:
            return None
        return security

    async def _read_mint(self, base_mint: str) -> _MintAuthorityInfo | None:
        """Читает и разбирает mint-аккаунт.

        Args:
            base_mint: Mint мемкоина.

        Returns:
            Данные authorities или ``None`` при ошибке/отсутствии аккаунта.
        """
        try:
            account = await self._rpc.get_account_info(base_mint)
        except RpcError as exc:
            logger.warning("Не удалось прочитать mint {}: {}", base_mint, exc)
            return None
        if account is None:
            return None
        try:
            mint = decode_mint(account.data)
        except SplError as exc:
            logger.debug("Не удалось разобрать mint {}: {}", base_mint, exc)
            return None
        return _MintAuthorityInfo(
            mint_authority=mint.mint_authority,
            freeze_authority=mint.freeze_authority,
        )

    async def _resolve_lp_burned_pct(
        self,
        *,
        dex: DEXType,
        lp_mint: str | None,
        lp_supply: int | None,
        lp_burned: int | None,
    ) -> float:
        """Вычисляет долю сожжённого/заблокированного LP в процентах.

        Args:
            dex: DEX/платформа пула.
            lp_mint: Mint LP-токенов.
            lp_supply: Общий объём LP (base units).
            lp_burned: Известная доля сожжённого LP (base units).

        Returns:
            Доля от 0 до 100.
        """
        if lp_burned is not None and lp_supply:
            return min(100.0, 100.0 * lp_burned / lp_supply)

        if lp_mint is None:
            # Bonding curve Pump.fun: ликвидность заперта в программе кривой.
            if dex == DEXType.PUMP_FUN:
                return 100.0
            return 0.0

        supply = lp_supply if lp_supply is not None else await self._read_lp_supply(
            lp_mint
        )
        if not supply:
            return 0.0
        burned_amount = await self._read_burned_lp_amount(lp_mint)
        return min(100.0, 100.0 * burned_amount / supply)

    async def _read_lp_supply(self, lp_mint: str) -> int:
        """Читает общий объём LP-mint.

        Args:
            lp_mint: Mint LP-токенов.

        Returns:
            Общий объём в base units (0 при ошибке).
        """
        try:
            return await self._rpc.get_token_supply(lp_mint)
        except RpcError as exc:
            logger.debug("Не удалось прочитать supply LP {}: {}", lp_mint, exc)
            return 0

    async def _read_burned_lp_amount(self, lp_mint: str) -> int:
        """Суммирует LP-токены, удерживаемые «сжигающими» адресами.

        Args:
            lp_mint: Mint LP-токенов.

        Returns:
            Суммарный объём сожжённого LP в base units.
        """
        try:
            token_accounts = await self._rpc.get_token_largest_accounts(lp_mint)
        except RpcError as exc:
            logger.debug("Не удалось прочитать крупнейших держателей LP: {}", exc)
            return 0

        if not token_accounts:
            return 0

        try:
            states = await self._rpc.get_multiple_accounts(token_accounts)
        except RpcError as exc:
            logger.debug("Не удалось прочитать LP-аккаунты: {}", exc)
            return 0

        burned_total = 0
        for state in states:
            parsed = _parse_token_owner_amount(state)
            if parsed is None:
                continue
            owner, amount = parsed
            if owner in LP_BURN_ADDRESSES:
                burned_total += amount
        return burned_total


class _MintAuthorityInfo:
    """Внутренняя обёртка над authorities mint-аккаунта."""

    __slots__ = ("freeze_authority", "mint_authority")

    def __init__(self, mint_authority: str | None, freeze_authority: str | None) -> None:
        """Сохраняет authorities.

        Args:
            mint_authority: Mint authority (или ``None``).
            freeze_authority: Freeze authority (или ``None``).
        """
        self.mint_authority = mint_authority
        self.freeze_authority = freeze_authority


def _parse_token_owner_amount(state: AccountState | None) -> tuple[str, int] | None:
    """Извлекает owner и amount из SPL Token-аккаунта.

    Args:
        state: Состояние аккаунта из RPC.

    Returns:
        Кортеж ``(owner_base58, amount)`` или ``None`` при недостатке данных.
    """
    if state is None or len(state.data) < _TOKEN_MIN_SIZE:
        return None
    owner_bytes = state.data[_TOKEN_OWNER_OFFSET : _TOKEN_OWNER_OFFSET + 32]
    owner = str(Pubkey.from_bytes(owner_bytes))
    amount = int.from_bytes(
        state.data[_TOKEN_AMOUNT_OFFSET:_TOKEN_AMOUNT_OFFSET + 8],
        "little",
        signed=False,
    )
    return owner, amount
