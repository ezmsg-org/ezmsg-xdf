"""Write a small, self-describing XDF file for the tests.

Checked in as a generator rather than a binary blob so the fixture is readable
and adjustable: a test that needs an irregular stream, a string stream or a
particular channel layout changes an argument here instead of asking someone to
produce a new recording.

The format is the XDF specification's chunked layout, and the field-by-field
choices below are pinned against what ``pyxdf``'s reader actually consumes --
:func:`pyxdf.pyxdf._read_varlen_int`, :func:`~pyxdf.pyxdf._read_chunk3` and the
tag dispatch in :func:`~pyxdf.load_xdf` -- since that is the only reader these
files ever meet.

Run standalone to drop a file next to this script::

    python tests/create_test_xdf.py /tmp/test.xdf
"""

from __future__ import annotations

import struct
import typing
from pathlib import Path

import numpy as np

# XDF chunk tags. Boundary chunks (5) are omitted -- they only help a reader
# resynchronise after corruption, and these files are not corrupt. ClockOffset
# chunks (4) are written even though there is no clock skew to model, because
# pyxdf warns ("Segments and clock-segments differ") on a stream that has sample
# segments but no clock segments, and a fixture that logs a warning on every load
# trains readers to ignore warnings.
TAG_FILE_HEADER = 1
TAG_STREAM_HEADER = 2
TAG_SAMPLES = 3
TAG_CLOCK_OFFSET = 4
TAG_STREAM_FOOTER = 6

FORMAT_DTYPES: dict[str, np.dtype] = {
    "int8": np.dtype("<i1"),
    "int16": np.dtype("<i2"),
    "int32": np.dtype("<i4"),
    "int64": np.dtype("<i8"),
    "float32": np.dtype("<f4"),
    "double64": np.dtype("<f8"),
}


def _varlen_int(value: int) -> bytes:
    """XDF's length prefix: one byte saying how wide the count is, then the count.

    The reader accepts widths of 1, 4 and 8 only, so this picks the narrowest of
    those rather than the narrowest possible.
    """
    if value < 256:
        return b"\x01" + bytes([value])
    if value < 2**32:
        return b"\x04" + struct.pack("<I", value)
    return b"\x08" + struct.pack("<Q", value)


def _chunk(tag: int, content: bytes, stream_id: int | None = None) -> bytes:
    """One length-prefixed chunk.

    The declared length covers the tag and, where the tag has one, the stream id
    -- the reader subtracts both back off before reading the payload.
    """
    body = struct.pack("<H", tag)
    if stream_id is not None:
        body += struct.pack("<I", stream_id)
    body += content
    return _varlen_int(len(body)) + body


def _xml_channels(labels: typing.Sequence[str], unit: str, ch_type: str) -> str:
    rows = "".join(
        f"<channel><label>{label}</label><unit>{unit}</unit><type>{ch_type}</type></channel>" for label in labels
    )
    return f"<desc><channels>{rows}</channels></desc>"


def _stream_header_xml(
    name: str,
    stream_type: str,
    labels: typing.Sequence[str],
    srate: float,
    channel_format: str,
    unit: str,
    ch_type: str,
) -> bytes:
    return (
        '<?xml version="1.0"?><info>'
        f"<name>{name}</name>"
        f"<type>{stream_type}</type>"
        f"<channel_count>{len(labels)}</channel_count>"
        f"<nominal_srate>{srate:g}</nominal_srate>"
        f"<channel_format>{channel_format}</channel_format>"
        f"<created_at>0.0</created_at>"
        f"<uid>{name}-uid</uid>"
        f"<session_id>default</session_id>"
        f"<hostname>test</hostname>"
        f"{_xml_channels(labels, unit, ch_type)}"
        "</info>"
    ).encode("utf-8")


def _stream_footer_xml(first: float, last: float, n_samples: int, srate: float) -> bytes:
    return (
        '<?xml version="1.0"?><info>'
        f"<first_timestamp>{first!r}</first_timestamp>"
        f"<last_timestamp>{last!r}</last_timestamp>"
        f"<sample_count>{n_samples}</sample_count>"
        f"<measured_srate>{srate:g}</measured_srate>"
        "</info>"
    ).encode("utf-8")


def _clock_offset_chunk(stream_id: int, collection_time: float, offset: float) -> bytes:
    return _chunk(
        TAG_CLOCK_OFFSET,
        struct.pack("<d", collection_time) + struct.pack("<d", offset),
        stream_id=stream_id,
    )


def _samples_chunk(values: np.ndarray, stamps: np.ndarray, channel_format: str) -> bytes:
    """A [Samples] chunk with an explicit timestamp on every sample.

    Every sample carries its stamp rather than relying on delta decompression.
    That is more bytes than a real recorder would write, and deliberately so:
    the alternative encodes "same as last plus 1/srate", which would make a
    fixture silently agree with any reader that got the nominal rate wrong.
    """
    out = [_varlen_int(len(stamps))]
    if channel_format == "string":
        for row, stamp in zip(values, stamps):
            out.append(b"\x08" + struct.pack("<d", float(stamp)))
            for item in row:
                encoded = str(item).encode("utf-8")
                out.append(_varlen_int(len(encoded)) + encoded)
    else:
        dtype = FORMAT_DTYPES[channel_format]
        payload = np.ascontiguousarray(values, dtype=dtype)
        for row, stamp in zip(payload, stamps):
            out.append(b"\x08" + struct.pack("<d", float(stamp)))
            out.append(row.tobytes())
    return b"".join(out)


class StreamSpec(typing.NamedTuple):
    """One stream to write. ``srate=0`` marks an irregular stream."""

    name: str
    labels: tuple[str, ...]
    srate: float
    n_samples: int
    channel_format: str = "float32"
    stream_type: str = "EEG"
    unit: str = "microvolts"
    ch_type: str = "EEG"
    t0: float = 10.0
    """Nonzero on purpose: a fixture starting at 0 cannot tell a reader that
    honours ``rezero`` apart from one that ignores it."""


def _values_for(spec: StreamSpec, rng: np.random.Generator) -> np.ndarray:
    if spec.channel_format == "string":
        return np.array(
            [[f"{spec.name}-{i}-{ch}" for ch in range(len(spec.labels))] for i in range(spec.n_samples)],
            dtype=object,
        )
    # A per-channel ramp offset by channel index: every sample is identifiable,
    # so a test can assert on ordering and chunk boundaries, not just on shape.
    ramp = np.arange(spec.n_samples, dtype=np.float64)[:, None]
    offsets = np.arange(len(spec.labels), dtype=np.float64)[None, :] * 1000.0
    return (ramp + offsets).astype(FORMAT_DTYPES[spec.channel_format])


def _stamps_for(spec: StreamSpec, rng: np.random.Generator) -> np.ndarray:
    if spec.srate > 0:
        return spec.t0 + np.arange(spec.n_samples) / spec.srate
    # Irregular: monotonic but unevenly spaced, which is the whole point of the
    # format's per-sample timestamps.
    gaps = rng.uniform(0.05, 0.25, size=spec.n_samples)
    return spec.t0 + np.cumsum(gaps)


DEFAULT_STREAMS: tuple[StreamSpec, ...] = (
    StreamSpec(name="EEGSignal", labels=("Fz", "Cz", "Pz", "Oz"), srate=100.0, n_samples=250),
    StreamSpec(
        name="Markers",
        labels=("marker",),
        srate=0.0,
        n_samples=12,
        channel_format="string",
        stream_type="Markers",
        unit="none",
        ch_type="Marker",
    ),
)


def write_test_xdf(
    path: Path | str,
    streams: typing.Sequence[StreamSpec] = DEFAULT_STREAMS,
    samples_per_chunk: int = 32,
    seed: int = 0,
) -> Path:
    """Write *streams* to *path* and return it.

    Samples are split across several [Samples] chunks so the file exercises the
    reader's chunk stitching rather than arriving as one block.
    """
    rng = np.random.default_rng(seed)
    path = Path(path)

    out = [b"XDF:"]
    out.append(
        _chunk(
            TAG_FILE_HEADER,
            b'<?xml version="1.0"?><info><version>1.0</version></info>',
        )
    )

    prepared = []
    for stream_id, spec in enumerate(streams, start=1):
        values = _values_for(spec, rng)
        stamps = _stamps_for(spec, rng)
        prepared.append((stream_id, spec, values, stamps))
        out.append(
            _chunk(
                TAG_STREAM_HEADER,
                _stream_header_xml(
                    spec.name, spec.stream_type, spec.labels, spec.srate, spec.channel_format, spec.unit, spec.ch_type
                ),
                stream_id=stream_id,
            )
        )

    # Interleave the streams' sample chunks, as a real recording would have them.
    offset = 0
    while any(offset < spec.n_samples for _, spec, _, _ in prepared):
        for stream_id, spec, values, stamps in prepared:
            if offset >= spec.n_samples:
                continue
            stop = min(offset + samples_per_chunk, spec.n_samples)
            out.append(
                _chunk(
                    TAG_SAMPLES,
                    _samples_chunk(values[offset:stop], stamps[offset:stop], spec.channel_format),
                    stream_id=stream_id,
                )
            )
        offset += samples_per_chunk

    # A pair of zero-offset clock measurements per stream, bracketing its
    # samples. Zero because these timestamps are already on one clock; the
    # chunks exist so the reader sees a clock segment covering the data.
    for stream_id, spec, _, stamps in prepared:
        out.append(_clock_offset_chunk(stream_id, float(stamps[0]) - 1.0, 0.0))
        out.append(_clock_offset_chunk(stream_id, float(stamps[-1]) + 1.0, 0.0))

    for stream_id, spec, _, stamps in prepared:
        out.append(
            _chunk(
                TAG_STREAM_FOOTER,
                _stream_footer_xml(float(stamps[0]), float(stamps[-1]), spec.n_samples, spec.srate),
                stream_id=stream_id,
            )
        )

    path.write_bytes(b"".join(out))
    return path


if __name__ == "__main__":
    import sys

    target = Path(sys.argv[1] if len(sys.argv) > 1 else "test.xdf")
    write_test_xdf(target)
    print(f"wrote {target} ({target.stat().st_size} bytes)")
