"""Конфигурация бота.

Все параметры читаются из переменных окружения и файла ``.env`` с помощью
``pydantic-settings``. Модуль предоставляет единый кэшированный синглтон
:data:`settings`, который импортируется остальными слоями приложения.

Пример:
    >>> from config import settings
    >>> settings.trade_amount_lamports
    500000000
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Количество лампортов в одном SOL.
LAMPORTS_PER_SOL: int = 1_000_000_000

#: Допустимые сети Solana.
SolanaNetwork = Literal["mainnet-beta", "devnet", "testnet"]

#: Шаблоны HTTP/WS endpoint-ов Helius по сети. Поддомен определяет кластер:
#: для mainnet-beta это "mainnet" (mainnet-beta.helius-rpc.com НЕ существует).
_HELIUS_HTTP_TEMPLATE = "https://{sub}.helius-rpc.com/?api-key={key}"
_HELIUS_WS_TEMPLATE = "wss://{sub}.helius-rpc.com/?api-key={key}"

#: Поддомен Helius для каждой сети.
_HELIUS_SUBDOMAINS: dict[str, str] = {
    "mainnet-beta": "mainnet",
    "devnet": "devnet",
    "testnet": "testnet",
}


class Settings(BaseSettings):
    """Единый объект конфигурации приложения.

    Поля соответствуют переменным из ``.env``. Регистр имён переменных не важен
    (``HELIUS_API_KEY`` == ``helius_api_key``). Неизвестные переменные игнорируются.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------ Сеть
    solana_network: SolanaNetwork = Field(
        default="mainnet-beta", description="Целевая сеть Solana."
    )
    helius_api_key: str = Field(default="", description="API-ключ Helius.")
    rpc_http_url: str = Field(
        default="", description="Переопределение HTTP RPC (пусто = из Helius)."
    )
    rpc_ws_url: str = Field(
        default="", description="Переопределение WS RPC (пусто = из Helius)."
    )

    # ---------------------------------------------------------------- Кошелёк
    wallet_private_key: str = Field(
        default="", description="Приватный ключ base58 (приоритетнее keypair-файла)."
    )
    wallet_keypair_path: str = Field(
        default="./keys/id.json", description="Путь к JSON keypair."
    )

    # ------------------------------------------------------- Торговые параметры
    trade_amount_sol: float = Field(
        default=0.02, gt=0, description="Размер входного ордера в SOL."
    )
    buy_slippage_pct: float = Field(
        default=15.0, ge=0, lt=100, description="Проскальзывание на вход, %."
    )
    sell_slippage_pct: float = Field(
        default=25.0, ge=0, lt=100, description="Проскальзывание на выход, %."
    )
    min_pool_sol: float = Field(
        default=30.0, ge=0, description="Минимальная ликвидность AMM-пула, SOL."
    )
    min_real_pool_sol: float = Field(
        default=40.0, ge=0,
        description=(
            "Минимальные РЕАЛЬНЫЕ резервы bonding curve, SOL "
            "(виртуальный резерв = реальные + 30 SOL, порог по виртуальным фиктивен)."
        ),
    )
    max_price_impact_pct: float = Field(
        default=5.0, gt=0, lt=100, description="Предельный Price Impact ордера, %."
    )
    token_reliable_seconds: float = Field(
        default=10.0, ge=0,
        description=(
            "Возраст токена с момента обнаружения, после которого он считается "
            "надёжным: входы/усреднения разрешены только по токенам старше "
            "этого порога. 0 — гейт отключён (торгуем сразу)."
        ),
    )
    max_entries_per_mint: int = Field(
        default=10, ge=1,
        description="Максимум входов по одному mint (вход + усреднения).",
    )
    average_only_in_profit: bool = Field(
        default=True,
        description=(
            "Усреднять только при цене >= цены входа: докупки в затухающий "
            "токен запрещены."
        ),
    )
    reentry_cooldown_seconds: float = Field(
        default=90.0, ge=0,
        description=(
            "Пауза после закрытия сделки по mint, перед новым входом, c "
            "(защита от чёрствого churn по затухающему токену)."
        ),
    )
    capital_sol: float | None = Field(
        default=None, gt=0, description="Лимит свободного капитала, SOL (None — без лимита)."
    )

    # ---------------------------------------------------------------- Память
    portfolio_db_path: str = Field(
        default="data/bot.db",
        description="Путь к SQLite-БД позиций (пусто = только память).",
    )
    portfolio_dry_db_path: str = Field(
        default="data/bot_dry.db",
        description="SQLite-БД позиций для dry-run (чтобы не смешивать с боевыми).",
    )

    # ------------------------------------------------------------------ Отправка
    execution_venue: Literal["rpc", "jito"] = Field(
        default="rpc",
        description=(
            "Канал отправки ордеров: rpc — обычная транзакция через RPC "
            "(Helius, без Jito-чаевых); jito — бандл с чаевыми и защитой от MEV."
        ),
    )

    # -------------------------------------------------------------------- Jito
    jito_tip_sol: float = Field(
        default=0.005, ge=0, description="Чаевые Jito валидатору, SOL (venue=jito)."
    )
    jito_block_engine_url: str = Field(
        default="https://frankfurt.mainnet.block-engine.jito.wtf",
        description="URL Jito Block Engine (региональный).",
    )
    jito_region: str = Field(default="frankfurt", description="Метка региона Jito.")
    jito_require_confirmation: bool = Field(
        default=True,
        description="Считать ордер исполненным только после подтверждения.",
    )
    jito_confirm_timeout_seconds: float = Field(
        default=10.0, gt=0, description="Таймаут ожидания подтверждения, c (оба venue)."
    )
    jito_confirm_poll_seconds: float = Field(
        default=0.5, gt=0, description="Интервал опроса статуса, c (оба venue)."
    )

    # ------------------------------------------------------------ Compute Budget
    compute_unit_limit: int = Field(
        default=200_000, gt=0, description="Лимит Compute Unit на транзакцию."
    )
    priority_fee_micro_lamports: int = Field(
        default=1_000_000, ge=0,
        description=(
            "Priority fee, micro-lamports per CU "
            "(1,000,000 при CU 200,000 ≈ 0.0002 SOL на транзакцию; "
            "практика GMGN/снайперов: 0.0001–0.005 SOL суммарно)."
        ),
    )

    # ------------------------------------------------------------------ DEX
    enabled_dex: str = Field(
        default="pump_fun",
        description=(
            "Включённые DEX через запятую: pump_fun и/или raydium. "
            "По умолчанию только Pump.fun (bonding curve + PumpSwap AMM)."
        ),
    )

    # ------------------------------------------------------------------ Вход
    entry_window_seconds: float = Field(
        default=5.0, gt=0, description="Окно анализа входных триггеров, c."
    )
    buy_sell_ratio_threshold: float = Field(
        default=0.70,
        ge=0,
        le=1,
        description="Минимальная доля покупок для входа.",
    )
    volume_velocity_threshold: float = Field(
        default=1.8, gt=0, description="Минимальный объём покупок за окно, SOL."
    )
    volume_velocity_cold_threshold: float = Field(
        default=2.0, gt=0,
        description=(
            "Усиленный порог объёма покупок, SOL, для «холодных» пулов "
            "(объём торгов < cold_volume_sol): отсекает вход за единичной "
            "микроволной на тонком токене."
        ),
    )
    min_entry_buy_count: int = Field(
        default=3, ge=1,
        description=(
            "Минимальное число покупок в окне входа: отсекает вход за одним "
            "кит-ордером, сформировавшим сигнал."
        ),
    )
    max_entry_price_gain_pct: float = Field(
        default=40.0, gt=0,
        description=(
            "Максимальный рост цены за окно входа, %: выше — «погоня за "
            "хвостом волны», вход запрещён."
        ),
    )

    # ------------------------------------------------------------------ Выход
    tp1_pct: float = Field(default=25.0, gt=0, description="Первый тейк-профит, %.")
    tp2_pct: float = Field(default=40.0, gt=0, description="Второй тейк-профит, %.")
    hot_volume_sol: float = Field(
        default=300.0, gt=0,
        description=(
            "Объём торгов по пулу с момента отслеживания, SOL, при котором "
            "позиция считается «горячей»: расширенные тейки/холд/трейлинг."
        ),
    )
    tp1_hot_pct: float = Field(
        default=40.0, gt=0, description="Первый тейк-профит для «горячих» пулов, %."
    )
    tp2_hot_pct: float = Field(
        default=70.0, gt=0, description="Второй тейк-профит для «горячих» пулов, %."
    )
    max_hold_hot_seconds: float = Field(
        default=600.0, gt=0,
        description="Максимальное удержание «горячей» позиции, c.",
    )
    trailing_activation_hot_pct: float = Field(
        default=25.0, ge=0,
        description=(
            "Минимальная прибыль до пика, %, для активации трейлинга "
            "на «горячих» пулах."
        ),
    )
    tp1_size_pct: float = Field(
        default=50.0, gt=0, le=100, description="Доля позиции для TP1, %."
    )
    trailing_stop_pct: float = Field(
        default=15.0, gt=0, le=100, description="Откат от пика для трейлинг-стопа, %."
    )
    trailing_activation_pct: float = Field(
        default=12.0, ge=0,
        description=(
            "Минимальная прибыль от входа до пика, %, после которой "
            "трейлинг-стоп включается (из убытка не срабатывает)."
        ),
    )
    hard_stop_loss_pct: float = Field(
        default=30.0, gt=0, le=100, description="Жёсткий стоп-лосс (базовый), % от входа."
    )
    hard_stop_hot_pct: float = Field(
        default=40.0, gt=0, le=100,
        description="Жёсткий стоп-лосс «горячих» пулов (объём >= hot_volume_sol), %.",
    )
    cold_volume_sol: float = Field(
        default=100.0, ge=0,
        description="Объём торгов пула, ниже которого позиция классифицируется «холодной», SOL.",
    )
    hard_stop_cold_pct: float = Field(
        default=22.0, gt=0, le=100,
        description="Жёсткий стоп-лосс «холодных» пулов (объём < cold_volume_sol), %.",
    )
    exit_window_seconds: float = Field(
        default=5.0, gt=0, description="Окно детекта затухания импульса, c."
    )
    exit_ratio_threshold: float = Field(
        default=0.25, ge=0, le=1, description="Порог падения Buy/Sell Ratio."
    )
    exhaustion_grace_seconds: float = Field(
        default=10.0, ge=0,
        description=(
            "Защитная пауза после входа, c: выход по затуханию объёма "
            "не срабатывает, пока позиция моложе этого порога."
        ),
    )
    large_sell_sol: float = Field(
        default=3.0, gt=0, description="Порог крупной продажи, SOL."
    )
    max_hold_seconds: float = Field(
        default=180.0, gt=0, description="Принудительный выход по таймауту, c."
    )
    silent_sweep_seconds: float = Field(
        default=45.0, gt=0,
        description=(
            "Молчание пула с открытой позицией, c, после которого свип "
            "пытается продать сразу — не дожидаясь MAX_HOLD."
        ),
    )

    # ---------------------------------------------------------- Безопасность
    security_ttl_seconds: float = Field(
        default=30.0, gt=0,
        description="TTL кэша данных безопасности токена в RPC-резолвере, c.",
    )
    min_lp_burned_pct: float = Field(
        default=90.0, ge=0, le=100, description="Минимальная доля сожжённого/залоченного LP, %."
    )
    screen_recheck_seconds: float = Field(
        default=600.0, gt=0,
        description=(
            "TTL вердикта отбора токена до повторной проверки, c "
            "(authorities могут быть сняты позже — токен может «вылечиться»)."
        ),
    )

    # ---------------------------------------------------------------- Jupiter
    use_jupiter_fallback: bool = Field(
        default=False,
        description=(
            "Jupiter fallback: запасной маршрут продажи/покупки через агрегатор "
            "для токенов, мигрировавших с bonding curve на AMM. "
            "false = торговать только токены на bonding curve; продажа позиции, "
            "в которой токен мигрировал, будет завершаться ошибкой."
        ),
    )
    jupiter_api_url: str = Field(
        default="https://lite-api.jup.ag/swap/v1",
        description="Базовый URL Jupiter Swap API v6.",
    )

    # ------------------------------------------------------------- Логирование
    log_level: str = Field(default="INFO", description="Уровень логирования.")
    log_dir: str = Field(default="logs", description="Каталог логов loguru.")

    # ------------------------------------------------------ Обслуживание пайплайна
    dashboard_enabled: bool = Field(
        default=True,
        description="Веб-дашборд (SSE): вся картина торговли в браузере.",
    )
    dashboard_host: str = Field(
        default="127.0.0.1",
        description="Адрес веб-дашборда (127.0.0.1 — только локально; для VPS-туннеля).",
    )
    dashboard_port: int = Field(
        default=8787, ge=1, le=65535, description="Порт веб-дашборда."
    )
    dashboard_interval_seconds: float = Field(
        default=2.0, gt=0, description="Интервал SSE-обновления дашборда, c."
    )
    dashboard_user: str = Field(
        default="home", description="Логин веб-дашборда (кнопки ручных сделок!)."
    )
    dashboard_password: str = Field(
        default="123",
        description="Пароль веб-дашборда; пусто = аутентификация выключена (только 127.0.0.1!).",
    )
    retry_cooldown_seconds: float = Field(
        default=2.0, ge=0,
        description="Пауза перед повторным входом по пулу после неуспешного ордера, c.",
    )
    maintenance_interval_seconds: float = Field(
        default=60.0, gt=0,
        description="Интервал обслуживания пайплайна (чистка пулов, сводка), c.",
    )
    stale_pool_seconds: float = Field(
        default=600.0, gt=0,
        description="Возраст «молчания» пула, после которого он удаляется из реестра, c.",
    )

    # ----------------------------------------------------------------- Транспорт
    rpc_timeout_seconds: float = Field(
        default=10.0, gt=0, description="Таймаут HTTP RPC-запроса, c."
    )
    rpc_max_retries: int = Field(
        default=3, ge=0, description="Число повторов RPC-запроса."
    )
    ws_initial_reconnect_delay: float = Field(
        default=0.5, gt=0, description="Начальная задержка реконнекта WS, c."
    )
    ws_max_reconnect_delay: float = Field(
        default=30.0, gt=0, description="Максимальная задержка реконнекта WS, c."
    )

    # -------------------------------------------------------------- Валидаторы
    @field_validator("solana_network", mode="before")
    @classmethod
    def _normalize_network(cls, value: object) -> object:
        """Приводит алиасы сети к каноничному значению."""
        if isinstance(value, str):
            normalized = value.strip().lower()
            aliases = {
                "mainnet": "mainnet-beta",
                "mainnet-beta": "mainnet-beta",
                "devnet": "devnet",
                "testnet": "testnet",
            }
            return aliases.get(normalized, normalized)
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        """Приводит уровень логирования к верхнему регистру."""
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("execution_venue", mode="before")
    @classmethod
    def _normalize_execution_venue(cls, value: object) -> object:
        """Приводит канал отправки к нижнему регистру.

        Args:
            value: Сырое значение (строка).

        Returns:
            Нормализованное значение или исходное.
        """
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("enabled_dex", mode="before")
    @classmethod
    def _normalize_enabled_dex(cls, value: object) -> object:
        """Нормализует список включённых DEX и отсекает пустые части.

        Args:
            value: Строка вида ``pump_fun`` или ``pump_fun,raydium``.

        Returns:
            Нормализованная строка через запятую.

        Raises:
            ValueError: Если указан неизвестный DEX.
        """
        if isinstance(value, str):
            known = {"pump_fun", "raydium"}
            parts = [part.strip().lower() for part in value.split(",") if part.strip()]
            unknown = [part for part in parts if part not in known]
            if unknown:
                raise ValueError(
                    "неизвестные DEX в ENABLED_DEX: "
                    f"{', '.join(unknown)} (допустимо: pump_fun, raydium)"
                )
            if not parts:
                return "pump_fun"
            return ",".join(parts)
        return value

    # ------------------------------------------------------------- Вычисляемые
    @computed_field  # type: ignore[prop-decorator]
    @property
    def http_rpc_url(self) -> str:
        """Итоговый HTTP RPC endpoint (переопределение или Helius)."""
        if self.rpc_http_url.strip():
            return self.rpc_http_url.strip()
        sub = _HELIUS_SUBDOMAINS.get(self.solana_network, "mainnet")
        return _HELIUS_HTTP_TEMPLATE.format(sub=sub, key=self.helius_api_key)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ws_rpc_url(self) -> str:
        """Итоговый WebSocket RPC endpoint (переопределение или Helius)."""
        if self.rpc_ws_url.strip():
            return self.rpc_ws_url.strip()
        sub = _HELIUS_SUBDOMAINS.get(self.solana_network, "mainnet")
        return _HELIUS_WS_TEMPLATE.format(sub=sub, key=self.helius_api_key)

    # ------------------------------------------------------------------ Хелперы
    def sol_to_lamports(self, amount_sol: float) -> int:
        """Переводит SOL в лампорты с округлением вниз.

        Args:
            amount_sol: Количество SOL.

        Returns:
            Целое количество лампортов.
        """
        return int(amount_sol * LAMPORTS_PER_SOL)

    def lamports_to_sol(self, lamports: int) -> float:
        """Переводит лампорты в SOL.

        Args:
            lamports: Количество лампортов.

        Returns:
            Количество SOL как float.
        """
        return lamports / LAMPORTS_PER_SOL

    @computed_field  # type: ignore[prop-decorator]
    @property
    def trade_amount_lamports(self) -> int:
        """Размер входного ордера в лампортах."""
        return self.sol_to_lamports(self.trade_amount_sol)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def jito_tip_lamports(self) -> int:
        """Размер чаевых Jito в лампортах."""
        return self.sol_to_lamports(self.jito_tip_sol)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def enabled_dex_types(self) -> list[str]:
        """Список включённых DEX (значения ``DEXType``)."""
        return [part.strip() for part in self.enabled_dex.split(",") if part.strip()]

    def dex_enabled(self, dex: object) -> bool:
        """Проверяет, включён ли DEX для обнаружения и торговли.

        Args:
            dex: Значение ``DEXType`` или его строковое значение.

        Returns:
            ``True``, если DEX входит в ``ENABLED_DEX``.
        """
        dex_value = dex.value if hasattr(dex, "value") else str(dex)
        return dex_value in self.enabled_dex_types


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Возвращает кэшированный объект настроек.

    Returns:
        Singleton :class:`Settings`, собранный из окружения.
    """
    return Settings()


#: Модульный синглтон настроек для удобного импорта.
settings: Settings = get_settings()
