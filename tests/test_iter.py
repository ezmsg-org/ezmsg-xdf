"""Behaviour of the XDF iterators, against a generated fixture.

These pin what the iterators produce -- sample values, ordering, chunking, and
the two message fields consumers key their cached state on -- so that the
producer's contract is checked rather than merely exercised.
"""

from __future__ import annotations

import math
import pickle

import ezmsg.core as ez
import numpy as np
import pytest
from conftest import EEG_STREAM, MARKER_STREAM
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.util import replace as replace_settings

from ezmsg.xdf.iter import (
    XDFAxisArrayIterator,
    XDFIterator,
    XDFIteratorSettings,
    XDFMultiAxArrIterator,
    XDFMultiIteratorSettings,
)
from ezmsg.xdf.source import XDFIteratorUnit, XDFMultiIteratorUnit


def eeg_messages(path, **kwargs) -> list[AxisArray]:
    return list(XDFAxisArrayIterator(filepath=path, select=EEG_STREAM.name, **kwargs))


class TestTheRawIterator:
    def test_it_finds_both_streams(self, test_xdf_path):
        it = XDFIterator(filepath=test_xdf_path, chunk_dur=1.0)
        seen: set[str] = set()
        for chunk in it:
            seen |= set(chunk)
        assert seen == {EEG_STREAM.name, MARKER_STREAM.name}


class TestTheSingleStreamIterator:
    def test_it_yields_every_sample_in_order(self, test_xdf_path):
        msgs = eeg_messages(test_xdf_path, chunk_dur=0.5)
        data = np.concatenate([m.data for m in msgs], axis=0)
        assert data.shape == (EEG_STREAM.n_samples, len(EEG_STREAM.labels))
        # create_test_xdf writes a per-channel ramp: sample i, channel c == i + 1000c.
        expected = np.arange(EEG_STREAM.n_samples)[:, None] + np.arange(len(EEG_STREAM.labels))[None, :] * 1000.0
        np.testing.assert_allclose(data, expected)

    def test_chunk_dur_controls_how_much_arrives_at_once(self, test_xdf_path):
        long = eeg_messages(test_xdf_path, chunk_dur=1.0)
        short = eeg_messages(test_xdf_path, chunk_dur=0.25)
        assert len(short) > len(long)
        assert max(m.data.shape[0] for m in long) > max(m.data.shape[0] for m in short)

    def test_it_carries_the_channel_labels(self, test_xdf_path):
        msg = eeg_messages(test_xdf_path)[0]
        assert list(msg.axes["ch"].data) == list(EEG_STREAM.labels)

    def test_rezero_moves_the_first_sample_to_zero(self, test_xdf_path):
        """The fixture starts at t=10 s, so this distinguishes honouring the
        setting from ignoring it."""
        rezeroed = eeg_messages(test_xdf_path, rezero=True)[0]
        assert rezeroed.axes["time"].offset == pytest.approx(0.0, abs=1e-6)

    def test_without_rezero_the_file_clock_is_preserved(self, test_xdf_path):
        raw = eeg_messages(test_xdf_path, rezero=False)[0]
        assert raw.axes["time"].offset == pytest.approx(EEG_STREAM.t0, abs=1e-6)

    def test_the_nominal_rate_becomes_the_axis_gain(self, test_xdf_path):
        msg = eeg_messages(test_xdf_path)[0]
        assert msg.axes["time"].gain == pytest.approx(1.0 / EEG_STREAM.srate)


class TestTheMultiStreamIterator:
    @staticmethod
    def _messages(path, **kwargs) -> list[AxisArray]:
        it = XDFMultiAxArrIterator(filepath=path, chunk_dur=1.0, **kwargs)
        return [msg for msg in it if msg is not None]

    def test_both_streams_come_through(self, test_xdf_path):
        keys = {m.key for m in self._messages(test_xdf_path)}
        assert keys == {EEG_STREAM.name, MARKER_STREAM.name}

    def test_the_irregular_stream_keeps_per_sample_timestamps(self, test_xdf_path):
        markers = [m for m in self._messages(test_xdf_path) if m.key == MARKER_STREAM.name]
        assert markers, "no marker messages"
        time_ax = markers[0].axes["time"]
        assert isinstance(time_ax, AxisArray.CoordinateAxis)
        stamps = np.concatenate([m.axes["time"].data for m in markers])
        assert len(stamps) == MARKER_STREAM.n_samples
        assert np.all(np.diff(stamps) > 0), "timestamps must stay monotonic"

    def test_force_single_sample_splits_an_irregular_stream(self, test_xdf_path):
        """Without it, several events inside one ``chunk_dur`` arrive together."""
        batched = [m for m in self._messages(test_xdf_path) if m.key == MARKER_STREAM.name]
        split = [
            m
            for m in self._messages(test_xdf_path, force_single_sample={MARKER_STREAM.name})
            if m.key == MARKER_STREAM.name
        ]
        assert len(split) == MARKER_STREAM.n_samples
        assert len(split) > len(batched)
        assert all(m.data.shape[0] == 1 for m in split)


class TestMessagesArriveReadyForConsumers:
    """Two things only the source can supply, both set once per stream.

    ``stream_dim`` names the dimension messages accumulate along -- the one whose
    length is just however much of the file this chunk covered, and which a
    consumer must leave out of the state it caches against the stream's
    configuration. ``fingerprint`` is the channel axis's content digest, cached
    on the axis and pickled with it; priming it at construction spares the first
    consumer in every process from recomputing it on every message.
    """

    def test_the_single_stream_iterator_declares_its_stream_dim(self, test_xdf_path):
        assert all(m.stream_dim == "time" for m in eeg_messages(test_xdf_path))

    def test_the_multi_stream_iterator_declares_it_for_every_stream(self, test_xdf_path):
        it = XDFMultiAxArrIterator(filepath=test_xdf_path, chunk_dur=1.0)
        undeclared = sorted({m.key for m in it if m is not None and m.stream_dim != "time"})
        assert not undeclared, f"streams not declaring stream_dim='time': {undeclared}"

    def test_the_channel_axis_is_primed(self, test_xdf_path):
        msg = eeg_messages(test_xdf_path)[0]
        assert "_fingerprint" in msg.axes["ch"].__dict__
        assert msg.axes["ch"].fingerprint is not None

    def test_every_stream_of_the_multi_iterator_is_primed(self, test_xdf_path):
        it = XDFMultiAxArrIterator(filepath=test_xdf_path, chunk_dur=1.0)
        cold = sorted({m.key for m in it if m is not None and "_fingerprint" not in m.axes["ch"].__dict__})
        assert not cold, f"streams handing over a cold ch axis: {cold}"

    def test_one_axis_object_serves_the_whole_stream(self, test_xdf_path):
        """What makes priming cheap: the checksum is paid once, not per message."""
        msgs = eeg_messages(test_xdf_path, chunk_dur=0.25)
        assert len(msgs) > 1
        assert len({id(m.axes["ch"]) for m in msgs}) == 1

    def test_the_chunk_axis_is_left_cold(self, test_xdf_path):
        """Digesting per-message timestamps would be pure cost: no consumer reads
        the stream axis's fingerprint."""
        it = XDFMultiAxArrIterator(filepath=test_xdf_path, chunk_dur=1.0)
        markers = [m for m in it if m is not None and m.key == MARKER_STREAM.name]
        assert markers, "no marker messages"
        assert all("_fingerprint" not in m.axes["time"].__dict__ for m in markers)

    def test_it_all_survives_the_transport(self, test_xdf_path):
        msg = eeg_messages(test_xdf_path)[0]
        landed = pickle.loads(pickle.dumps(msg))
        assert landed.stream_dim == "time"
        assert "_fingerprint" in landed.axes["ch"].__dict__
        assert landed.axes["ch"].__dict__["_fingerprint"] == msg.axes["ch"].fingerprint


class TestTheFixtureItself:
    """The generator is test code, and a wrong fixture would make every
    assertion above agree with the wrong thing."""

    def test_pyxdf_reads_back_what_was_written(self, test_xdf_path):
        import pyxdf

        streams, header = pyxdf.load_xdf(str(test_xdf_path))
        assert header["info"]["version"] == ["1.0"]
        by_name = {s["info"]["name"][0]: s for s in streams}
        assert set(by_name) == {EEG_STREAM.name, MARKER_STREAM.name}

        eeg = by_name[EEG_STREAM.name]
        assert np.asarray(eeg["time_series"]).shape == (EEG_STREAM.n_samples, len(EEG_STREAM.labels))
        assert float(eeg["info"]["nominal_srate"][0]) == EEG_STREAM.srate
        assert eeg["time_stamps"][0] == pytest.approx(EEG_STREAM.t0)
        labels = [c["label"][0] for c in eeg["info"]["desc"][0]["channels"][0]["channel"]]
        assert labels == list(EEG_STREAM.labels)

        markers = by_name[MARKER_STREAM.name]
        assert float(markers["info"]["nominal_srate"][0]) == 0.0
        assert np.asarray(markers["time_series"]).shape == (MARKER_STREAM.n_samples, 1)

    def test_it_loads_without_pyxdf_warnings(self, test_xdf_path, caplog):
        """A fixture that logs on every load trains readers to ignore warnings."""
        import logging

        import pyxdf

        with caplog.at_level(logging.WARNING, logger="pyxdf"):
            pyxdf.load_xdf(str(test_xdf_path))
        assert not caplog.records, [r.getMessage() for r in caplog.records]

    def test_the_samples_span_more_than_one_chunk(self, test_xdf_path):
        """Otherwise the reader's chunk stitching is never exercised."""
        assert EEG_STREAM.n_samples > 32
        assert math.ceil(EEG_STREAM.n_samples / 32) > 1


class TestTheProducerContract:
    """The producers are `BaseStatefulProducer`s, which means the file open is a
    state reset rather than construction work."""

    def test_construction_does_not_read_the_file(self, test_xdf_path):
        """Deliberately unlike ezmsg-neo and ezmsg-nwb, which reset eagerly in
        ``__init__`` and so pay the whole open on the event loop during
        ``initialize``. Nothing here reads stream metadata before the first
        chunk, so there is nothing to lose by waiting."""
        it = XDFAxisArrayIterator(filepath=test_xdf_path, select=EEG_STREAM.name)
        assert it._state.reader is None
        assert next(it) is not None
        assert it._state.reader is not None

    def test_the_file_load_runs_off_the_event_loop(self, test_xdf_path):
        """``pyxdf.load_xdf`` reads and decodes the whole file. On the event loop
        that stalls every other unit in the process.

        Driven with ``asyncio.run`` rather than an async test, because this repo
        has no pytest-asyncio -- the pytest config names ``asyncio_mode`` but the
        plugin is not installed, so an ``async def`` test is silently never
        awaited and passes without running.
        """
        import asyncio
        import threading

        seen: list[int] = []

        class Spy(XDFAxisArrayIterator):
            def _reset_state(self):
                seen.append(threading.get_ident())
                super()._reset_state()

        producer = Spy(filepath=test_xdf_path, select=EEG_STREAM.name)
        assert not seen, "construction should not have opened anything"

        loop_tid: list[int] = []

        async def drive():
            loop_tid.append(threading.get_ident())
            await producer.__acall__()

        asyncio.run(drive())

        assert len(seen) == 1
        assert seen[0] != loop_tid[0], "_reset_state ran on the event-loop thread"

    def test_settings_arrive_as_keywords_or_as_a_settings_object(self, test_xdf_path):
        by_kwargs = XDFAxisArrayIterator(filepath=test_xdf_path, select=EEG_STREAM.name, chunk_dur=0.5)
        by_settings = XDFAxisArrayIterator(
            settings=XDFIteratorSettings(filepath=test_xdf_path, select=EEG_STREAM.name, chunk_dur=0.5)
        )
        assert by_kwargs.settings == by_settings.settings

    def test_pacing_settings_do_not_reopen_the_file(self, test_xdf_path):
        """``playback_rate`` and ``self_terminating`` belong to the unit, so
        changing either must not throw away a loaded file."""
        it = XDFAxisArrayIterator(filepath=test_xdf_path, select=EEG_STREAM.name)
        next(it)
        reader = it._state.reader
        it.update_settings(replace_settings(it.settings, playback_rate=2.0, self_terminating=True))
        next(it)
        assert it._state.reader is reader

    def test_changing_the_file_does_reopen(self, test_xdf_path):
        it = XDFAxisArrayIterator(filepath=test_xdf_path, select=EEG_STREAM.name)
        next(it)
        reader = it._state.reader
        it.update_settings(replace_settings(it.settings, chunk_dur=0.25))
        next(it)
        assert it._state.reader is not reader


class TestTheUnitsInAGraph:
    @staticmethod
    def _run(unit_cls, settings) -> list[AxisArray]:
        collected: list[AxisArray] = []

        class Collector(ez.Unit):
            INPUT_SIGNAL = ez.InputStream(AxisArray)

            @ez.subscriber(INPUT_SIGNAL)
            async def on_msg(self, msg: AxisArray) -> None:
                collected.append(msg)

        src, sink = unit_cls(settings), Collector()
        ez.run(SRC=src, SINK=sink, connections=((src.OUTPUT_SIGNAL, sink.INPUT_SIGNAL),))
        return collected

    def test_the_single_stream_unit_publishes_the_whole_file(self, test_xdf_path):
        msgs = self._run(
            XDFIteratorUnit,
            XDFIteratorSettings(filepath=test_xdf_path, select=EEG_STREAM.name, self_terminating=True),
        )
        assert msgs, "no messages published"
        assert sum(m.data.shape[0] for m in msgs) == EEG_STREAM.n_samples
        assert all(m.stream_dim == "time" for m in msgs)
        assert all("_fingerprint" in m.axes["ch"].__dict__ for m in msgs)

    def test_the_multi_stream_unit_publishes_both_streams(self, test_xdf_path):
        msgs = self._run(
            XDFMultiIteratorUnit,
            XDFMultiIteratorSettings(filepath=test_xdf_path, self_terminating=True),
        )
        assert {m.key for m in msgs} == {EEG_STREAM.name, MARKER_STREAM.name}
        eeg = [m for m in msgs if m.key == EEG_STREAM.name]
        assert sum(m.data.shape[0] for m in eeg) == EEG_STREAM.n_samples
        assert all(m.stream_dim == "time" for m in msgs)
