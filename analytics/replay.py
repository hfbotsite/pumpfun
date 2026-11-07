"""Реплей исторических событий: CSV ↔ события и реконструкция пула.

Модуль даёт бэктестеру три вещи:

* чтение/запись сделок в «плоском» CSV-формате
  (:func:`load_events_csv`, :func:`write_events_csv`);
* описание сценария прогона — пул, начальные резервы, события и данные
  безопасности токена (:class:`ReplayScenario`);
* восстановление состояния пула после каждой сделки
  (:class:`ReplayPoolState`), чтобы котировки входа и выхода считались против
  реальных резервов на момент события, а не против статичного снимка.

Реплей сознательно не знает ни о сети, ни о стратегии: это чистый слой данных.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from config.settings import LAMPORTS_PER_SOL
from models.trade_data import DEXType, PoolInfo, TokenSecurity, TradeEvent, TradeSide
from utils.constants import PUMP_TOKEN_DECIMALS, SOL_DECIMALS, WSOL_MINT

#: Обязательные колонки CSV (порядок важен для канонической записи).
REQUIRED_COLUMNS: tuple[str, ...] = (
    "signature",
    "pool_address",
    "base_mint",
    "dex",
    "side",
    "sol_amount",
    "token_amount",
    "wallet",
    "timestamp",
)

#: Необязательная колонка с начальным резервом SOL пула.
RESERVE_SOL_COLUMN: str = "pool_sol_reserve"

#: Необязательная колонка с начальным резервом токенов пула.
RESERVE_TOKEN_COLUMN: str = "pool_token_reserve"

#: Минимальный резерв, который поддерживается при реплее (защита от деления на 0).
MIN_REPLAY_RESERVE: int = 1

#: Алиасы платформ в колонке ``dex``.
_DEX_ALIASES: dict[str, DEXType] = {
    "pump_fun": DEXType.PUMP_FUN,
    "pumpfun": DEXType.PUMP_FUN,
    "pump": DEXType.PUMP_FUN,
    "pumpportal": DEXType.PUMP_FUN,
    "raydium": DEXType.RAYDIUM,
    "raydium_amm": DEXType.RAYDIUM,
    "raydium_cpmm": DEXType.RAYDIUM,
    "raydium_v4": DEXType.RAYDIUM,
    "amm": DEXType.RAYDIUM,
    "cpmm": DEXType.RAYDIUM,
}

#: Алиасы направлений в колонке ``side``.
_SIDE_ALIASES: dict[str, TradeSide] = {
    "buy": TradeSide.BUY,
    "b": TradeSide.BUY,
    "bought": TradeSide.BUY,
    "sell": TradeSide.SELL,
    "s": TradeSide.SELL,
    "sold": TradeSide.SELL,
}


class ReplayError(RuntimeError):
    """Ошибка чтения, записи или разбора данных реплея."""


@dataclass(frozen=True, slots=True)
class ReplayPoolConfig:
    """Начальное состояние пула для реплея.

    Attributes:
        pool_address: Адрес пула.
        base_mint: Mint базового токена (мемкоина).
        dex: Платформа пула.
        quote_reserve: Начальный резерв SOL, лампорты.
        base_reserve: Начальный резерв токенов, base units.
        base_decimals: Десятичные знаки базового токена.
        quote_decimals: Десятичные знаки котируемого актива (SOL = 9).
        creator: Кошелёк создателя токена (для прямого маршрута Pump.fun).
        quote_mint: Mint котируемого актива.
    """

    pool_address: str
    base_mint: str
    dex: DEXType
    quote_reserve: int
    base_reserve: int
    base_decimals: int = PUMP_TOKEN_DECIMALS
    quote_decimals: int = SOL_DECIMALS
    creator: str | None = None
    quote_mint: str = WSOL_MINT


@dataclass(frozen=True, slots=True)
class ReplayScenario:
    """Полный сценарий прогона: пул, события и данные безопасности.

    Attributes:
        pool: Начальное состояние пула.
        events: События сделок в хронологическом порядке.
        security: Данные безопасности токена, подаваемые стратегии.
    """

    pool: ReplayPoolConfig
    events: tuple[TradeEvent, ...]
    security: TokenSecurity

    @property
    def pool_address(self) -> str:
        """Адрес пула сценария."""
        return self.pool.pool_address

    @property
    def base_mint(self) -> str:
        """Mint базового токена сценария."""
        return self.pool.base_mint

    @property
    def duration_seconds(self) -> float:
        """Длительность сценария по меткам времени событий, c."""
        if len(self.events) < 2:
            return 0.0
        span = self.events[-1].timestamp - self.events[0].timestamp
        return max(0.0, span)


class ReplayPoolState:
    """Восстанавливает состояние пула по потоку сделок.

    Каждое событие сдвигает резервы так же, как это произошло бы в пуле:
    покупка добавляет SOL и забирает токены, продажа — наоборот. Комиссия пула
    не удерживается отдельно: её эффект уже содержится в объёмах сделки.

    Резервы ограничены снизу :data:`MIN_REPLAY_RESERVE`, чтобы вырожденные
    данные не приводили к делению на ноль в котировках.
    """

    def __init__(self, config: ReplayPoolConfig) -> None:
        """Инициализирует состояние пула.

        Args:
            config: Начальные резервы и метаданные пула.
        """
        self._config = config
        self._base_reserve = max(MIN_REPLAY_RESERVE, config.base_reserve)
        self._quote_reserve = max(MIN_REPLAY_RESERVE, config.quote_reserve)
        self._updated_at = 0.0

    # --------------------------------------------------------------- Свойства
    @property
    def config(self) -> ReplayPoolConfig:
        """Начальная конфигурация пула."""
        return self._config

    @property
    def base_reserve(self) -> int:
        """Текущий резерв токенов, base units."""
        return self._base_reserve

    @property
    def quote_reserve(self) -> int:
        """Текущий резерв SOL, лампорты."""
        return self._quote_reserve

    # ------------------------------------------------------------- Публичное
    def apply(self, event: TradeEvent) -> PoolInfo:
        """Применяет событие к резервам и возвращает снимок пула.

        Args:
            event: Событие сделки (``sol_amount``/``token_amount`` — в
                человекочитаемых единицах).

        Returns:
            Снимок :class:`~models.trade_data.PoolInfo` после события.
        """
        scale = float(10**self._config.base_decimals)
        sol_lamports = round(event.sol_amount * LAMPORTS_PER_SOL)
        tokens = round(event.token_amount * scale)

        if event.side == TradeSide.BUY:
            self._quote_reserve += max(0, sol_lamports)
            self._base_reserve -= _clamp_withdraw(tokens, self._base_reserve)
        else:
            self._quote_reserve -= _clamp_withdraw(sol_lamports, self._quote_reserve)
            self._base_reserve += max(0, tokens)

        self._quote_reserve = max(MIN_REPLAY_RESERVE, self._quote_reserve)
        self._base_reserve = max(MIN_REPLAY_RESERVE, self._base_reserve)
        self._updated_at = event.timestamp
        return self.snapshot(timestamp=event.timestamp)

    def snapshot(self, *, timestamp: float | None = None) -> PoolInfo:
        """Возвращает текущий снимок пула без изменения состояния.

        Args:
            timestamp: Метка времени снимка (по умолчанию — последняя).

        Returns:
            :class:`~models.trade_data.PoolInfo` с текущими резервами.
        """
        config = self._config
        updated_at = self._updated_at if timestamp is None else timestamp
        return PoolInfo(
            dex=config.dex,
            pool_address=config.pool_address,
            base_mint=config.base_mint,
            quote_mint=config.quote_mint,
            base_decimals=config.base_decimals,
            quote_decimals=config.quote_decimals,
            base_reserve=self._base_reserve,
            quote_reserve=self._quote_reserve,
            created_at=0.0,
            updated_at=updated_at,
        )


def _clamp_withdraw(amount: int, reserve: int) -> int:
    """Ограничивает списание из резерва, оставляя его непустым.

    Args:
        amount: Желаемое списание (сырые единицы).
        reserve: Текущий резерв (сырые единицы).

    Returns:
        Списание, после которого резерв остаётся не меньше
        :data:`MIN_REPLAY_RESERVE`.
    """
    available = max(0, reserve - MIN_REPLAY_RESERVE)
    return max(0, min(max(0, amount), available))


# --------------------------------------------------------------------------- #
#  CSV: чтение и запись
# --------------------------------------------------------------------------- #
def _read_rows(path: Path) -> list[dict[str, str]]:
    """Читает CSV и проверяет наличие обязательных колонок.

    Args:
        path: Путь к CSV-файлу.

    Returns:
        Список строк файла как словарей.

    Raises:
        ReplayError: Если файл недоступен или в нём нет нужных колонок.
    """
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            missing = [name for name in REQUIRED_COLUMNS if name not in fieldnames]
            if missing:
                raise ReplayError(
                    f"в CSV {path} отсутствуют колонки: {', '.join(missing)}"
                )
            return [row for row in reader]
    except OSError as exc:
        raise ReplayError(f"не удалось прочитать CSV {path}: {exc}") from exc


def _optional_float(row: dict[str, str], column: str) -> float | None:
    """Читает необязательное числовое поле строки.

    Args:
        row: Строка CSV.
        column: Имя колонки.

    Returns:
        Значение как float или ``None``, если колонки нет/она пуста.
    """
    raw = (row.get(column) or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _require_float(row: dict[str, str], column: str, row_number: int) -> float:
    """Читает обязательное числовое поле строки.

    Args:
        row: Строка CSV.
        column: Имя колонки.
        row_number: Номер строки (для сообщения об ошибке).

    Returns:
        Значение как float.

    Raises:
        ReplayError: Если значение не является числом.
    """
    raw = (row.get(column) or "").strip()
    try:
        return float(raw)
    except ValueError as exc:
        raise ReplayError(
            f"строка {row_number}: колонка {column} не число: {raw!r}"
        ) from exc


def _parse_dex(value: str, row_number: int) -> DEXType:
    """Разбирает значение колонки ``dex`` с учётом алиасов.

    Args:
        value: Значение из CSV.
        row_number: Номер строки (для сообщения об ошибке).

    Returns:
        :class:`~models.trade_data.DEXType`.

    Raises:
        ReplayError: Если платформа не распознана.
    """
    dex = _DEX_ALIASES.get(value.strip().lower())
    if dex is None:
        raise ReplayError(f"строка {row_number}: неизвестный dex {value!r}")
    return dex


def _parse_side(value: str, row_number: int) -> TradeSide:
    """Разбирает значение колонки ``side`` с учётом алиасов.

    Args:
        value: Значение из CSV.
        row_number: Номер строки (для сообщения об ошибке).

    Returns:
        :class:`~models.trade_data.TradeSide`.

    Raises:
        ReplayError: Если направление не распознано.
    """
    side = _SIDE_ALIASES.get(value.strip().lower())
    if side is None:
        raise ReplayError(f"строка {row_number}: неизвестная сторона {value!r}")
    return side


def _row_to_event(row: dict[str, str], row_number: int) -> TradeEvent:
    """Собирает :class:`~models.trade_data.TradeEvent` из строки CSV.

    Args:
        row: Строка CSV.
        row_number: Номер строки файла (для сообщения об ошибке).

    Returns:
        Событие сделки.

    Raises:
        ReplayError: Если обязательные поля пусты или некорректны.
    """
    signature = (row.get("signature") or "").strip()
    pool_address = (row.get("pool_address") or "").strip()
    base_mint = (row.get("base_mint") or "").strip()
    if not pool_address or not base_mint:
        raise ReplayError(
            f"строка {row_number}: пустые pool_address/base_mint"
        )
    wallet = (row.get("wallet") or "").strip() or None
    return TradeEvent(
        signature=signature or f"replay-{row_number}",
        pool_address=pool_address,
        base_mint=base_mint,
        dex=_parse_dex(row.get("dex") or "", row_number),
        side=_parse_side(row.get("side") or "", row_number),
        sol_amount=_require_float(row, "sol_amount", row_number),
        token_amount=_require_float(row, "token_amount", row_number),
        wallet=wallet,
        timestamp=_require_float(row, "timestamp", row_number),
    )


def load_events_csv(
    path: str | Path, *, pool_address: str | None = None
) -> list[TradeEvent]:
    """Загружает события сделок из CSV.

    Args:
        path: Путь к CSV-файлу.
        pool_address: Если задан, берутся только события этого пула.

    Returns:
        События в хронологическом порядке.

    Raises:
        ReplayError: Если файл пуст, повреждён или не содержит событий.
    """
    rows = _read_rows(Path(path))
    events = [
        _row_to_event(row, index + 2)
        for index, row in enumerate(rows)
        if pool_address is None or row.get("pool_address") == pool_address
    ]
    if not events:
        raise ReplayError(f"в CSV {path} нет событий для указанного пула")
    events.sort(key=lambda event: event.timestamp)
    return events


def write_events_csv(path: str | Path, events: list[TradeEvent]) -> int:
    """Записывает события сделок в CSV канонического формата.

    Args:
        path: Путь к создаваемому файлу.
        events: События для записи (в текущем порядке).

    Returns:
        Число записанных строк.

    Raises:
        ReplayError: Если файл не удалось записать.
    """
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(REQUIRED_COLUMNS))
            writer.writeheader()
            for event in events:
                writer.writerow(
                    {
                        "signature": event.signature,
                        "pool_address": event.pool_address,
                        "base_mint": event.base_mint,
                        "dex": event.dex.value,
                        "side": event.side.value,
                        "sol_amount": f"{event.sol_amount:.9f}",
                        "token_amount": f"{event.token_amount:.6f}",
                        "wallet": event.wallet or "",
                        "timestamp": f"{event.timestamp:.3f}",
                    }
                )
    except OSError as exc:
        raise ReplayError(f"не удалось записать CSV {target}: {exc}") from exc
    return len(events)


# --------------------------------------------------------------------------- #
#  Сценарий бэктеста
# --------------------------------------------------------------------------- #
def scenario_from_csv(
    path: str | Path,
    *,
    initial_sol_reserve: float | None = None,
    initial_token_reserve: float | None = None,
    base_decimals: int = PUMP_TOKEN_DECIMALS,
    creator: str | None = None,
    security: TokenSecurity | None = None,
    pool_address: str | None = None,
) -> ReplayScenario:
    """Собирает сценарий бэктеста из CSV-файла.

    Начальные резервы берутся из аргументов, а при их отсутствии — из
    необязательных колонок ``pool_sol_reserve`` / ``pool_token_reserve``
    первой строки.

    Args:
        path: Путь к CSV-файлу.
        initial_sol_reserve: Начальный резерв SOL пула, SOL.
        initial_token_reserve: Начальный резерв токенов пула, токены.
        base_decimals: Десятичные знаки базового токена.
        creator: Кошелёк создателя токена (для прямого маршрута Pump.fun).
        security: Данные безопасности токена (по умолчанию — «чистый» токен).
        pool_address: Если задан, берутся только события этого пула.

    Returns:
        :class:`ReplayScenario` с событиями и начальным состоянием пула.

    Raises:
        ReplayError: Если файл пуст, резервы неизвестны или колонки битые.
    """
    rows = _read_rows(Path(path))
    selected = [
        row
        for row in rows
        if pool_address is None or row.get("pool_address") == pool_address
    ]
    if not selected:
        raise ReplayError(f"в CSV {path} нет событий для указанного пула")

    first = selected[0]
    sol_reserve = (
        initial_sol_reserve
        if initial_sol_reserve is not None
        else _optional_float(first, RESERVE_SOL_COLUMN)
    )
    token_reserve = (
        initial_token_reserve
        if initial_token_reserve is not None
        else _optional_float(first, RESERVE_TOKEN_COLUMN)
    )
    if sol_reserve is None or token_reserve is None:
        raise ReplayError(
            "неизвестны начальные резервы пула: передайте initial_sol_reserve "
            f"и initial_token_reserve либо колонки {RESERVE_SOL_COLUMN}/"
            f"{RESERVE_TOKEN_COLUMN} в CSV"
        )

    events = load_events_csv(path, pool_address=pool_address)
    config = ReplayPoolConfig(
        pool_address=(first.get("pool_address") or "").strip(),
        base_mint=(first.get("base_mint") or "").strip(),
        dex=_parse_dex(first.get("dex") or "", 2),
        quote_reserve=round(sol_reserve * LAMPORTS_PER_SOL),
        base_reserve=round(token_reserve * float(10**base_decimals)),
        base_decimals=base_decimals,
        creator=creator,
    )
    return ReplayScenario(
        pool=config,
        events=tuple(events),
        security=security or TokenSecurity(lp_burned_pct=100.0),
    )
