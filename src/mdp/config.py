"""Runtime settings, read from ``MDP_*`` environment variables (or a ``.env`` file)."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

type SourceName = Literal["synthetic", "binance", "kraken", "engine"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MDP_", env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://mdp:mdp@localhost:5432/mdp"
    log_level: str = "INFO"

    # Canonical symbols this process handles, e.g. "BTC-USDT,ETH-USDT".
    symbols: Annotated[list[str], NoDecode] = Field(default_factory=lambda: ["BTC-USDT"])

    # Ingest
    source: SourceName = "synthetic"
    synthetic_rate: float = Field(default=20.0, gt=0, description="trades per second, all symbols")
    synthetic_seed: int = 7
    engine_url: str = Field(
        default="",
        description="ws://host/path, or redis://host:port/db#stream-key for a Redis stream",
    )
    engine_price_scale: int = 0
    engine_qty_scale: int = 0

    # Aggregate
    consumer: str = "aggregator-1"
    allowed_lateness_ms: int = Field(default=5_000, ge=0)
    idle_grace_ms: int = Field(default=2_000, ge=0)
    batch_size: int = Field(default=1_000, gt=0)

    # Repair
    repair_source: Literal["binance", "kraken"] = "binance"
    repair_interval_s: float = Field(default=60.0, gt=0)
    repair_lookback_minutes: int = Field(default=24 * 60, gt=0)

    # Serving
    api_host: str = "0.0.0.0"  # a container listens on every interface
    api_port: int = 8000
    metrics_port: int = 9100

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip().upper() for part in value.split(",") if part.strip()]
        return value
