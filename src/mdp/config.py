"""Runtime settings, read from ``MDP_*`` environment variables (or a ``.env`` file)."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MDP_", env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://mdp:mdp@localhost:5432/mdp"

    # Canonical symbols this process handles, e.g. "BTC-USDT,ETH-USDT".
    symbols: Annotated[list[str], NoDecode] = Field(default_factory=lambda: ["BTC-USDT"])

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip().upper() for part in value.split(",") if part.strip()]
        return value


def load_settings() -> Settings:
    return Settings()
