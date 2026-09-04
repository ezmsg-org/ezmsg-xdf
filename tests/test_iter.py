"""Behaviour of the XDF iterators, against a generated fixture.

These pin what the iterators produce -- sample values, ordering, chunking, and
the two message fields consumers key their cached state on -- so that the
producer's contract is checked rather than merely exercised.
"""

from __future__ import annotations

import math
import pickle

import numpy as np
import pytest
from conftest import EEG_STREAM, MARKER_STREAM
from ezmsg.util.messages.axisarray import AxisArray

from ezmsg.xdf.iter import XDFAxisArrayIterator, XDFIterator, XDFMultiAxArrIterator


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

    ``chunk_dim`` names the dimension messages accumulate along -- the one whose
    length is just however much of the file this chunk covered, and which a
    consumer must leave out of the state it caches against the stream's
    configuration. ``fingerprint`` is the channel axis's content digest, cached
    on the axis and pickled with it; priming it at construction spares the first
    consumer in every process from recomputing it on every message.
    """

    def test_the_single_stream_iterator_declares_its_chunk_dim(self, test_xdf_path):
        assert all(m.chunk_dim == "time" for m in eeg_messages(test_xdf_path))

    def test_the_multi_stream_iterator_declares_it_for_every_stream(self, test_xdf_path):
        it = XDFMultiAxArrIterator(filepath=test_xdf_path, chunk_dur=1.0)
        undeclared = sorted({m.key for m in it if m is not None and m.chunk_dim != "time"})
        assert not undeclared, f"streams not declaring chunk_dim='time': {undeclared}"

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
        the chunk axis's fingerprint."""
        it = XDFMultiAxArrIterator(filepath=test_xdf_path, chunk_dur=1.0)
        markers = [m for m in it if m is not None and m.key == MARKER_STREAM.name]
        assert markers, "no marker messages"
        assert all("_fingerprint" not in m.axes["time"].__dict__ for m in markers)

    def test_it_all_survives_the_transport(self, test_xdf_path):
        msg = eeg_messages(test_xdf_path)[0]
        landed = pickle.loads(pickle.dumps(msg))
        assert landed.chunk_dim == "time"
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
