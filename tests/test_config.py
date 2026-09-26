import pytest

from mdp.config import Settings


def test_symbols_are_read_as_a_comma_separated_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDP_SYMBOLS", "btc-usdt, ETH-USDT,,")

    assert Settings().symbols == ["BTC-USDT", "ETH-USDT"]
