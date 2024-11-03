import asyncio
import os
import typing
from dataclasses import field

import ezmsg.core as ez
from ezmsg.util.generator import GenState
from ezmsg.util.messages.axisarray import AxisArray

from .iter import XDFAxisArrayIterator, XDFMultiAxArrIterator, MultiStreamMessage


class XDFIteratorSettings(ez.Settings):
    filepath: typing.Union[os.PathLike, str]
    select: str
    chunk_dur: float = 1.0
    start_time: typing.Optional[float] = None
    stop_time: typing.Optional[float] = None
    rezero: bool = True
    self_terminating: bool = True


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

    @ez.publisher(OUTPUT_SIGNAL)
    async def pub_chunk(self) -> typing.AsyncGenerator:
        try:
            while True:
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
    select: typing.Optional[set[str]] = None  # Override with a default
    force_single_sample: set = field(default_factory=set)


class XDFMultiIteratorUnit(ez.Unit):
    STATE = GenState
    SETTINGS = XDFMultiIteratorUnitSettings

    OUTPUT_MULTI = ez.OutputStream(MultiStreamMessage)
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

    @ez.publisher(OUTPUT_MULTI)
    async def pub_multi(self) -> typing.AsyncGenerator:
        try:
            while True:
                msg = next(self.STATE.gen)
                if len(msg) > 0:
                    yield self.OUTPUT_MULTI, msg
                else:
                    await asyncio.sleep(0)
        except StopIteration:
            ez.logger.debug(
                f"File ({self.SETTINGS.filepath} :: {self.SETTINGS.select}) exhausted."
            )
            if self.SETTINGS.self_terminating:
                raise ez.NormalTermination
            yield self.OUTPUT_TERM, True
