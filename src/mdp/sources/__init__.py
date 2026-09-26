"""Trade sources: each one turns a feed's wire format into normalized trades."""

from collections.abc import AsyncIterator
from typing import Protocol

from mdp.normalizer import SourceEvent


class TradeSource(Protocol):
    """Anything that yields trades (and rejections) until it is cancelled.

    A source owns its connection: it reconnects on its own and only stops when
    the task iterating it is cancelled. Rejections are yielded rather than
    raised so one garbled message never tears down a healthy connection.
    """

    @property
    def name(self) -> str: ...

    def events(self) -> AsyncIterator[SourceEvent]: ...
