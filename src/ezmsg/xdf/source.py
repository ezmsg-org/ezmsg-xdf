import asyncio
import os
import time
import typing
from dataclasses import field

import ezmsg.core as ez
from ezmsg.util.generator import GenState
from ezmsg.util.messages.axisarray import AxisArray

from .iter import XDFAxisArrayIterator, XDFMultiAxArrIterator


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


class XDFIteratorSettings(ez.Settings):
    filepath: typing.Union[os.PathLike, str]
    select: str
    chunk_dur: float = 1.0
    start_time: float | None = None
    stop_time: float | None = None
    rezero: bool = True
    playback_rate: float | None = None
    self_terminating: bool = False
    """
    If True, the unit will raise a :obj:`ez.NormalTermination` exception when the file is exhausted.
    Note, however, that this will terminate the pipeline even if the data published by this unit are still in transit,
    which will lead to the pipeline output being truncated before it has finished processing the stream.
    `self_terminating` should only be used when it is not important that the pipeline finish processing data, such
    as during prototyping and testing. 
    """


class XDFIteratorUnit(ez.Unit):
    STATE = GenState
    SETTINGS = XDFIteratorSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    OUTPUT_TERM = ez.OutputStream(typing.Any)

    def initialize(self) -> None:
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = XDFAxisArrayIterator(
            filepath=self.SETTINGS.filepath,
            select=self.SETTINGS.select,
            chunk_dur=self.SETTINGS.chunk_dur,
            start_time=self.SETTINGS.start_time,
            stop_time=self.SETTINGS.stop_time,
            rezero=self.SETTINGS.rezero,
        )
        if self.SETTINGS.playback_rate is not None:
            self._clock = PlaybackClock(rate=self.SETTINGS.playback_rate, step_dur=self.SETTINGS.chunk_dur)
        else:
            self._clock = None

    @ez.publisher(OUTPUT_SIGNAL)
    async def pub_chunk(self) -> typing.AsyncGenerator:
        try:
            while True:
                if self._clock is not None:
                    await self._clock.astep()
                msg = next(self.STATE.gen)
                if msg.data.size > 0:
                    yield self.OUTPUT_SIGNAL, msg
                else:
                    await asyncio.sleep(0)
        except StopIteration:
            ez.logger.debug(
                f"File ({self.SETTINGS.filepath} :: {self.SETTINGS.select}) exhausted."
            )
            if self.SETTINGS.self_terminating:
                raise ez.NormalTermination
            yield self.OUTPUT_TERM, True


class XDFMultiIteratorUnitSettings(XDFIteratorSettings):
    select: set[str] | None = None  # Override with a default
    force_single_sample: set = field(default_factory=set)


class XDFMultiIteratorUnit(ez.Unit):
    STATE = GenState
    SETTINGS = XDFMultiIteratorUnitSettings

    OUTPUT_SIGNAL = ez.OutputStream(AxisArray)
    OUTPUT_TERM = ez.OutputStream(typing.Any)

    def initialize(self) -> None:
        self.construct_generator()

    def construct_generator(self):
        self.STATE.gen = XDFMultiAxArrIterator(
            filepath=self.SETTINGS.filepath,
            select=self.SETTINGS.select,
            chunk_dur=self.SETTINGS.chunk_dur,
            start_time=self.SETTINGS.start_time,
            stop_time=self.SETTINGS.stop_time,
            rezero=self.SETTINGS.rezero,
            force_single_sample=self.SETTINGS.force_single_sample,
        )
        if self.SETTINGS.playback_rate is not None:
            self._clock = PlaybackClock(rate=self.SETTINGS.playback_rate, step_dur=self.SETTINGS.chunk_dur)
        else:
            self._clock = None

    @ez.publisher(OUTPUT_SIGNAL)
    async def pub_multi(self) -> typing.AsyncGenerator:
        try:
            while True:
                if self._clock is not None:
                    await self._clock.astep()
                msg = next(self.STATE.gen)
                if msg is not None:
                    yield self.OUTPUT_SIGNAL, msg
                else:
                    await asyncio.sleep(0)
        except StopIteration:
            ez.logger.debug(
                f"File ({self.SETTINGS.filepath} :: {self.SETTINGS.select}) exhausted."
            )
            if self.SETTINGS.self_terminating:
                raise ez.NormalTermination
            yield self.OUTPUT_TERM, True
