import asyncio
import time
import typing

import ezmsg.core as ez
from ezmsg.baseproc.units import BaseProducerUnit
from ezmsg.util.messages.axisarray import AxisArray

from .iter import (
    XDFAxisArrayIterator,
    XDFIteratorSettings,
    XDFMultiAxArrIterator,
    XDFMultiIteratorSettings,
)

# The settings types moved to `iter.py` so the producers can own them, but they
# were importable from here first.
XDFMultiIteratorUnitSettings = XDFMultiIteratorSettings

__all__ = [
    "PlaybackClock",
    "XDFIteratorSettings",
    "XDFIteratorUnit",
    "XDFMultiIteratorSettings",
    "XDFMultiIteratorUnit",
    "XDFMultiIteratorUnitSettings",
]


class PlaybackClock:
    def __init__(
        self,
        rate: float = 1.0,
        step_dur: float = 0.005,
    ):
        """
        Create an object that provides a timer that can run at a specified rate,
        and with a specified step duration.

        Args:
            rate: Speed of playback. 1.0 is real time.
            step_dur: The duration of each step in seconds.
                Provide the duration using the unmodified rate.
        """
        self._step_dur = step_dur * rate
        self._wall_start: float = time.time() - self._step_dur / 2
        self._step_count: int = 0

    def reset(self) -> None:
        self._wall_start = time.time() - self._step_dur / 2
        self._step_count = 0

    def _get_duration(self) -> float:
        wall_elapsed = time.time() - self._wall_start
        next_elapsed = self._step_count * self._step_dur
        step_dur = max(next_elapsed - wall_elapsed, 0)
        self._step_count += 1
        return step_dur

    async def astep(self) -> None:
        await asyncio.sleep(self._get_duration())

    def step(self) -> None:
        time.sleep(self._get_duration())


class _XDFUnitBase:
    """Playback pacing and end-of-file handling, shared by both units.

    Note both subclasses name their publisher ``produce``: that is the name
    ``BaseProducerUnit`` uses, and ezmsg collects publishers per attribute, so a
    differently named one would run *alongside* the base class\'s rather than
    replacing it -- two publishers draining one producer, neither stopping.

    The producer supplies chunks as fast as they can be sliced out of memory;
    ``playback_rate`` is what turns that into a paced stream, and it is a
    property of the unit rather than of the reader.
    """

    OUTPUT_TERM = ez.OutputStream(typing.Any)

    async def initialize(self) -> None:
        await super().initialize()
        self._clock = (
            PlaybackClock(rate=self.SETTINGS.playback_rate, step_dur=self.SETTINGS.chunk_dur)
            if self.SETTINGS.playback_rate is not None
            else None
        )

    async def _finish(self) -> typing.AsyncGenerator:
        ez.logger.debug(f"File ({self.SETTINGS.filepath} :: {self.SETTINGS.select}) exhausted.")
        if self.SETTINGS.self_terminating:
            raise ez.NormalTermination
        yield self.OUTPUT_TERM, True


class XDFIteratorUnit(
    _XDFUnitBase,
    BaseProducerUnit[XDFIteratorSettings, AxisArray, XDFAxisArrayIterator],
):
    SETTINGS = XDFIteratorSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.publisher(OUTPUT_SIGNAL)
    async def produce(self) -> typing.AsyncGenerator:
        while not self.producer.exhausted:
            if self._clock is not None:
                await self._clock.astep()
            msg = await self.producer.__acall__()
            if msg is not None and msg.data.size > 0:
                yield self.OUTPUT_SIGNAL, msg
            else:
                await asyncio.sleep(0)
        async for out in self._finish():
            yield out


class XDFMultiIteratorUnit(
    _XDFUnitBase,
    BaseProducerUnit[XDFMultiIteratorSettings, AxisArray, XDFMultiAxArrIterator],
):
    SETTINGS = XDFMultiIteratorSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)

    @ez.publisher(OUTPUT_SIGNAL)
    async def produce(self) -> typing.AsyncGenerator:
        while not self.producer.exhausted:
            if self._clock is not None:
                await self._clock.astep()
            msg = await self.producer.__acall__()
            if msg is not None:
                yield self.OUTPUT_SIGNAL, msg
            else:
                await asyncio.sleep(0)
        async for out in self._finish():
            yield out
