"""An interruption pyxdf finds in a stream starts a new ``AxisArray.segment``."""

import asyncio
from pathlib import Path

import ezmsg.core as ez
import numpy as np
import pytest
from conftest import MARKER_STREAM
from create_test_xdf import StreamSpec, write_test_xdf
from ezmsg.baseproc import BaseStatefulTransformer, BaseTransformerUnit, processor_state
from ezmsg.util.messages.axisarray import AxisArray, compute_config_id

from ezmsg.xdf.iter import XDFAxisArrayIterator, XDFIteratorSettings, XDFMultiAxArrIterator, XDFMultiIteratorSettings
from ezmsg.xdf.source import XDFIteratorUnit, XDFMultiIteratorUnit

PAUSE_AT = 220  # not on a chunk boundary
PAUSE_S = 8.0  # past both of pyxdf's default thresholds: 1 s, and 500 samples (5 s at 100 Hz)
PAUSED = StreamSpec(name="Paused", labels=("a", "b"), srate=100.0, n_samples=500, pause=(PAUSE_AT, PAUSE_S))


@pytest.fixture(scope="module")
def paused_xdf_path(tmp_path_factory) -> Path:
    return write_test_xdf(tmp_path_factory.mktemp("xdf") / "paused.xdf", streams=(PAUSED, MARKER_STREAM))


def _messages(path, **kwargs):
    return [m for m in XDFAxisArrayIterator(filepath=path, select=PAUSED.name, **kwargs) if m.data.shape[0]]


def test_the_chunk_spanning_a_pause_is_cut_there(paused_xdf_path):
    msgs = _messages(paused_xdf_path, chunk_dur=1.0)
    # Every sample once and in order (the fixture's first channel is a ramp).
    np.testing.assert_array_equal(np.concatenate([m.data[:, 0] for m in msgs]), np.arange(PAUSED.n_samples))
    sizes = [m.data.shape[0] for m in msgs]
    segments = [m.segment for m in msgs]
    assert sum(s for s, seg in zip(sizes, segments) if seg == 0) == PAUSE_AT
    assert segments == sorted(segments) and set(segments) == {0, 1}
    # The message that starts the new segment is stamped with its own first sample.
    first_after = next(m for m in msgs if m.segment == 1)
    assert first_after.data[0, 0] == PAUSE_AT
    assert first_after.axes["time"].offset == pytest.approx(PAUSE_AT / PAUSED.srate + PAUSE_S, abs=1e-3)


def test_a_pause_under_the_thresholds_is_not_a_segment(paused_xdf_path):
    msgs = _messages(paused_xdf_path, chunk_dur=1.0, jitter_break_threshold_seconds=60.0)
    assert len({m.segment for m in msgs}) == 1


def test_the_multi_stream_iterator_numbers_each_stream(paused_xdf_path):
    it = XDFMultiAxArrIterator(filepath=paused_xdf_path, chunk_dur=1.0)
    msgs = [m for m in it if m is not None and m.data.shape[0]]
    by_key = {key: [m.segment for m in msgs if m.key == key] for key in (PAUSED.name, MARKER_STREAM.name)}
    assert set(by_key[PAUSED.name]) == {0, 1}
    assert set(by_key[MARKER_STREAM.name]) == {0}  # an irregular stream has no interruptions to find


def assert_segment_rule(msgs):
    """Per stream: segments start at 0 and never decrease, one marker ends the stream at its data's
    segment + 1, and no number is shared by a marker and data or by two runs of data. Empty messages
    that do not change the segment (idle chunks) are not markers."""
    for key in {m.key for m in msgs}:
        stream = [(m.segment, m.data.shape[0]) for m in msgs if m.key == key]
        segs = [seg for seg, _ in stream]
        assert segs == sorted(segs)
        assert next(seg for seg, n in stream if n) == 0
        data_segs = {seg for seg, n in stream if n}
        runs = [seg for i, (seg, n) in enumerate(stream) if n and (i == 0 or stream[i - 1][0] != seg)]
        assert len(runs) == len(set(runs))
        n_markers = 0
        for (prev, n_prev), (seg, n) in zip(stream, stream[1:]):
            if not n and seg != prev:
                n_markers += 1
                assert n_prev and seg == prev + 1 and seg not in data_segs
        assert n_markers == 1


def test_every_stream_follows_the_segment_rule(paused_xdf_path):
    single = list(XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name, chunk_dur=1.0))
    assert any(m.data.shape[0] == 0 and m.segment == 0 for m in single[:-1])  # an idle chunk in the pause
    assert_segment_rule(single)
    assert_segment_rule([m for m in XDFMultiAxArrIterator(filepath=paused_xdf_path, chunk_dur=1.0) if m is not None])


@pytest.mark.parametrize("multi", [False, True])
def test_reopening_the_file_continues_numbering_upward(paused_xdf_path, multi):
    if multi:
        it = XDFMultiAxArrIterator(filepath=paused_xdf_path, chunk_dur=1.0)
    else:
        it = XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name, chunk_dur=1.0)
    first = [m for m in it if m is not None]
    it._reset_state()
    second = [m for m in it if m is not None]
    assert [m.segment for m in second] == [m.segment + 3 for m in first]


def test_a_time_range_keeps_the_segments_in_place(paused_xdf_path):
    """Trimming removes samples from the front, so pyxdf's segment indices no longer line up;
    the reader goes by time. (Trimming at all used to raise a KeyError.)"""
    msgs = _messages(paused_xdf_path, chunk_dur=1.0, start_time=1.0)
    assert msgs[0].data[0, 0] in (100, 101)  # the sample at 1.0 s, give or take pyxdf's dejitter
    assert next(m for m in msgs if m.segment != msgs[0].segment).data[0, 0] == PAUSE_AT
    # Starting after the pause, the first data kept is segment 0, and the only one.
    after = list(XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name, start_time=PAUSE_AT / 100 + 9))
    assert {m.segment for m in after if m.data.shape[0]} == {0} and after[-1].segment == 1


def test_every_message_declares_one_config_id_per_stream(paused_xdf_path, monkeypatch):
    import ezmsg.xdf.iter as xdf_iter

    calls = []
    monkeypatch.setattr(xdf_iter, "compute_config_id", lambda m: calls.append(m) or compute_config_id(m))
    msgs = [m for m in XDFMultiAxArrIterator(filepath=paused_xdf_path, chunk_dur=1.0) if m is not None]
    assert len(calls) == 2  # once per stream, not per message
    ids = {}
    for m in msgs:
        assert m.config_id != 0 and m.config_id == compute_config_id(m)
        ids.setdefault(m.key, set()).add(m.config_id)
    assert [len(v) for v in ids.values()] == [1, 1]
    assert ids[PAUSED.name] != ids[MARKER_STREAM.name]


def test_two_readers_number_a_file_the_same(paused_xdf_path):
    first = [[m.segment for m in XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name)] for _ in range(2)]
    assert first[0] == first[1]


def test_a_pause_is_one_segment_on_and_only_the_end_gets_a_marker(paused_xdf_path):
    msgs = _published(XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name, chunk_dur=1.0))
    empty = [i for i, m in enumerate(msgs) if m.data.shape[0] == 0]
    assert empty == [len(msgs) - 1]
    last, eos = msgs[-2], msgs[-1]
    assert eos.config_id == last.config_id and eos.data.shape == (0, 2)
    assert eos.axes["time"].offset == pytest.approx(last.axes["time"].offset + len(last.data) / PAUSED.srate)
    n_before = next(i for i, m in enumerate(msgs) if m.data[0, 0] == PAUSE_AT)
    segments = [m.segment for m in msgs]
    assert segments == [0] * n_before + [1] * (len(msgs) - n_before - 1) + [2]


def _published(it) -> list:
    """What the single-stream unit publishes: empty chunks only where they start a segment."""
    out = []
    for m in it:
        if m.data.shape[0] or (out and m.segment != out[-1].segment):
            out.append(m)
    return out


@pytest.mark.parametrize("multi", [False, True])
def test_the_unit_publishes_the_markers_and_a_buffering_stage_flushes(paused_xdf_path, multi):
    if multi:
        unit = XDFMultiIteratorUnit(XDFMultiIteratorSettings(filepath=paused_xdf_path, chunk_dur=1.0))
    else:
        unit = XDFIteratorUnit(XDFIteratorSettings(filepath=paused_xdf_path, select=PAUSED.name, chunk_dur=1.0))
    hold = HoldUnit(HoldSettings())
    hold.create_processor()

    async def run():
        await unit.initialize()
        published, released = [], []
        async for stream, m in unit.produce():
            if stream is unit.OUTPUT_SIGNAL:
                published.append(m)
                if m.key == PAUSED.name:
                    released += [out async for _, out in hold.on_signal(m)]
        return published, released

    published, released = asyncio.run(run())
    paused = [m for m in published if m.key == PAUSED.name]
    assert [len(m.data) == 0 for m in paused].count(True) == 1  # at the end only, not at the pause
    assert paused[-1].data.shape[0] == 0
    if multi:
        markers = [m for m in published if m.key == MARKER_STREAM.name]
        assert markers[-1].data.shape[0] == 0 and all(m.data.shape[0] for m in markers[:-1])
    # Everything the holding stage took in came out, the last of it when the final marker arrived.
    assert sum(len(m.data) for m in released) == PAUSED.n_samples


class HoldSettings(ez.Settings):
    pass


@processor_state
class HoldState:
    held: AxisArray | None = None


class Hold(BaseStatefulTransformer[HoldSettings, AxisArray, AxisArray, HoldState]):
    """Emits each message one message late; ``flush`` releases the one it holds."""

    def _reset_state(self, message: AxisArray) -> None:
        self._state.held = None

    def _process(self, message: AxisArray) -> AxisArray | None:
        out, self._state.held = self._state.held, message
        return out

    def flush(self, message: AxisArray | None = None) -> AxisArray | None:
        out, self._state.held = self._state.held, None
        return out


class HoldUnit(BaseTransformerUnit[HoldSettings, AxisArray, AxisArray, Hold]):
    SETTINGS = HoldSettings
