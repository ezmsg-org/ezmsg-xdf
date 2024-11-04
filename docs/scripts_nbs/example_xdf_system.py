from pathlib import Path

import ezmsg.core as ez
from ezmsg.util.messages.key import FilterOnKey
from ezmsg.util.debuglog import DebugLog
import typer

from ezmsg.xdf.source import XDFMultiIteratorUnit


def main(file_path: Path):
    comps = {
        "SOURCE": XDFMultiIteratorUnit(
            file_path, rezero=True, select=None, self_terminating=True
        ),
        "SELECT": FilterOnKey("BrainVision RDA"),
        "SINK": DebugLog(),
    }
    conns = (
        (comps["SOURCE"].OUTPUT_SIGNAL, comps["SELECT"].INPUT_SIGNAL),
        (comps["SELECT"].OUTPUT_SIGNAL, comps["SINK"].INPUT),
    )
    ez.run(components=comps, connections=conns)


if __name__ == "__main__":
    typer.run(main)
