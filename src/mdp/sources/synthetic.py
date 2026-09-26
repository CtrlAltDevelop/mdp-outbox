"""A deterministic trade generator for demos, tests and benchmarks.

The same seed always produces the same trades — ids, prices, sizes, sides and
the gaps between them — so a test can assert exact candles and a benchmark can
be rerun on identical input. Only the timestamps differ between the two modes:

* **replay** (``start`` given): trades are stamped on a virtual clock starting
  at ``start`` and yielded as fast as the consumer takes them;
* **live** (``start`` omitted): the source sleeps out each gap and stamps the
  trade with the wall clock, like a real feed.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mdp.normalizer import SourceEvent
from mdp.schema import Side, Trade

_PRICE_TICK = Decimal("0.01")
_QTY_STEP = Decimal("0.00001")
_START_PRICES = {"BTC": 65_000.0, "ETH": 3_200.0, "SOL": 150.0}


@dataclass(frozen=True, slots=True)
class _Draw:
    """One generated trade, before it is given a timestamp."""

    symbol: str
    trade_id: str
    price: Decimal
    qty: Decimal
    side: Side
    gap_s: float  # time since the previous trade on any symbol


class SyntheticSource:
    def __init__(
        self,
        symbols: Sequence[str],
        *,
        seed: int = 7,
        rate: float = 20.0,
        start: datetime | None = None,
        limit: int | None = None,
        volatility: float = 0.0005,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self._symbols = list(symbols)
        self._seed = seed
        self._rate = rate
        self._start = start
        self._limit = limit
        self._volatility = volatility
        self._clock = clock
        self._sleep = sleep

    @property
    def name(self) -> str:
        return "synthetic"

    def trades(self) -> Iterator[Trade]:
        """Replay-mode trades as a plain iterator, for tests and benchmarks."""
        if self._start is None:
            raise ValueError("trades() needs a start time; live mode is async only")
        ts = self._start
        for draw in self._draws():
            ts += timedelta(seconds=draw.gap_s)
            yield self._stamp(draw, ts)

    async def events(self) -> AsyncIterator[SourceEvent]:
        if self._start is not None:
            for index, trade in enumerate(self.trades()):
                yield trade
                if index % 1000 == 999:
                    await asyncio.sleep(0)  # let the consumer's other tasks breathe
            return
        for draw in self._draws():
            await self._sleep(draw.gap_s)
            yield self._stamp(draw, self._clock())

    def _draws(self) -> Iterator[_Draw]:
        rng = random.Random(self._seed)
        prices = {s: _START_PRICES.get(s.split("-")[0], 100.0) for s in self._symbols}
        count = 0
        while self._limit is None or count < self._limit:
            symbol = self._symbols[count % len(self._symbols)]
            # A geometric random walk: returns, not prices, are normal, so the
            # price never goes negative and moves in proportion to its level.
            prices[symbol] *= math.exp(rng.gauss(0.0, self._volatility))
            yield _Draw(
                symbol=symbol,
                # One counter across symbols: an id is unique per source, not per symbol.
                trade_id=str(count + 1),
                price=Decimal(prices[symbol]).quantize(_PRICE_TICK),
                qty=max(Decimal(rng.expovariate(20.0)).quantize(_QTY_STEP), _QTY_STEP),
                side=Side.BUY if rng.random() < 0.5 else Side.SELL,
                gap_s=rng.expovariate(self._rate),  # a Poisson arrival process
            )
            count += 1

    def _stamp(self, draw: _Draw, ts: datetime) -> Trade:
        return Trade(
            source=self.name,
            symbol=draw.symbol,
            trade_id=draw.trade_id,
            price=draw.price,
            qty=draw.qty,
            side=draw.side,
            ts_exchange=ts,
            ts_ingested=ts if self._start is not None else self._clock(),
        )
