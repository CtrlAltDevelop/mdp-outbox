import pytest

from mdp.cli import build_source, parse_args
from mdp.config import Settings
from mdp.sources.binance import BinanceSource
from mdp.sources.engine import EngineSource
from mdp.sources.kraken import KrakenSource
from mdp.sources.synthetic import SyntheticSource


def settings(**overrides: object) -> Settings:
    return Settings.model_validate({"symbols": "BTC-USDT", **overrides})


def test_symbols_are_read_as_a_comma_separated_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDP_SYMBOLS", "btc-usdt, ETH-USDT,,")

    assert Settings().symbols == ["BTC-USDT", "ETH-USDT"]


@pytest.mark.parametrize(
    ("overrides", "kind"),
    [
        ({"source": "synthetic"}, SyntheticSource),
        ({"source": "binance"}, BinanceSource),
        ({"source": "kraken"}, KrakenSource),
        ({"source": "engine", "engine_url": "ws://engine:8080/trades"}, EngineSource),
        ({"source": "engine", "engine_url": "redis://engine:6379/0#trades"}, EngineSource),
    ],
)
def test_the_configured_source_is_built(overrides: dict[str, str], kind: type) -> None:

    assert isinstance(build_source(settings(**overrides)), kind)


def test_an_engine_url_without_a_transport_is_refused() -> None:

    with pytest.raises(SystemExit, match="MDP_ENGINE_URL"):
        build_source(settings(source="engine", engine_url="redis://x/0"))


def test_subcommands_parse() -> None:
    assert parse_args(["repair", "--once"]).once
    args = parse_args(["backfill", "btc-usdt", "2026-09-24T00:00:00Z", "1790244000000"])
    assert (args.symbol, args.end) == ("btc-usdt", "1790244000000")
    with pytest.raises(SystemExit):
        parse_args(["explode"])
