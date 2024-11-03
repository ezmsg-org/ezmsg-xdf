from frozendict import frozendict
import numpy as np
from ezmsg.util.messages.axisarray import AxisArray

from ezmsg.xdf.iter import MultiStreamMessage


def test_multi_stream_message():
    # Create a multi-stream message
    msg = MultiStreamMessage(
        {
            "a": (
                AxisArray(
                    data=np.arange(np.prod((4, 3, 2)), dtype=float).reshape(4, 3, 2),
                    dims=["time", "ch", "feat"],
                    axes={"time": AxisArray.Axis.TimeAxis(fs=19.0)},
                ),
                AxisArray(
                    data=np.arange(np.prod((6, 3, 2)), dtype=float).reshape(6, 3, 2),
                    dims=["time", "ch", "feat"],
                    axes={"time": AxisArray.Axis.TimeAxis(fs=19.0, offset=4 / 19)},
                ),
            ),
            "b": (
                AxisArray(
                    data=np.array([["First"]]),
                    dims=["time", "ch"],
                    axes={"time": AxisArray.Axis.TimeAxis(fs=1.0, offset=0.1)},
                ),
                AxisArray(
                    data=np.array([["Second"]]),
                    dims=["time", "ch"],
                    axes={"time": AxisArray.Axis.TimeAxis(fs=1.0, offset=0.2)},
                ),
            ),
        }
    )

    assert type(msg) is frozendict
    assert "a" in msg
    assert "b" in msg
    assert type(msg["a"]) is tuple
    assert type(msg["a"][0]) is AxisArray
