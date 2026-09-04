"""Shared fixtures for the ezmsg-xdf tests.

The XDF file is generated rather than checked in as a binary -- see
``create_test_xdf.py`` for why, and for what it contains.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from create_test_xdf import DEFAULT_STREAMS, StreamSpec, write_test_xdf  # noqa: E402

# Kept alongside the fixture so tests can assert against them by name rather
# than by index into DEFAULT_STREAMS.
EEG_STREAM: StreamSpec = DEFAULT_STREAMS[0]
MARKER_STREAM: StreamSpec = DEFAULT_STREAMS[1]


@pytest.fixture(scope="session")
def test_xdf_path(tmp_path_factory) -> Path:
    """A two-stream XDF: a 100 Hz 4-channel float32 stream and an irregular
    string marker stream, both starting at t=10 s."""
    return write_test_xdf(tmp_path_factory.mktemp("xdf") / "test.xdf")
