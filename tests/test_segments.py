"""An interruption pyxdf finds in a stream starts a new ``AxisArray.segment``."""

from pathlib import Path

import numpy as np
import pytest
from conftest import MARKER_STREAM
from create_test_xdf import StreamSpec, write_test_xdf

from ezmsg.xdf.iter import XDFAxisArrayIterator, XDFMultiAxArrIterator

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
    assert {m.segment for m in msgs} == {0}


def test_the_multi_stream_iterator_numbers_each_stream(paused_xdf_path):
    it = XDFMultiAxArrIterator(filepath=paused_xdf_path, chunk_dur=1.0)
    msgs = [m for m in it if m is not None and m.data.shape[0]]
    by_key = {key: [m.segment for m in msgs if m.key == key] for key in (PAUSED.name, MARKER_STREAM.name)}
    assert set(by_key[PAUSED.name]) == {0, 1}
    assert set(by_key[MARKER_STREAM.name]) == {0}  # an irregular stream has no interruptions to find


def test_reopening_the_file_does_not_repeat_a_segment(paused_xdf_path):
    it = XDFAxisArrayIterator(filepath=paused_xdf_path, select=PAUSED.name, chunk_dur=1.0)
    last = [m for m in it if m.data.shape[0]][-1].segment
    it._reset_state()
    assert next(it).segment == last + 1


def test_a_time_range_keeps_the_segments_in_place(paused_xdf_path):
    """Trimming removes samples from the front, so pyxdf's segment indices no longer line up;
    the reader goes by time. (Trimming at all used to raise a KeyError.)"""
    msgs = _messages(paused_xdf_path, chunk_dur=1.0, start_time=1.0)
    assert msgs[0].data[0, 0] in (100, 101)  # the sample at 1.0 s, give or take pyxdf's dejitter
    assert next(m for m in msgs if m.segment == 1).data[0, 0] == PAUSE_AT
