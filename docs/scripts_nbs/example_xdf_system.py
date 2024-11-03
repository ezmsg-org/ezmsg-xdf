from pathlib import Path

import ezmsg.core as ez
from ezmsg.util.debuglog import DebugLog
import typer

from ezmsg.xdf.source import XDFMultiIteratorUnit


def main(file_path: Path):
    comps = {
        "SOURCE": XDFMultiIteratorUnit(file_path, rezero=False, select=None),
        "SINK": DebugLog(),
    }
    conns = ((comps["SOURCE"].OUTPUT_MULTI, comps["SINK"].INPUT),)
    ez.run(components=comps, connections=conns)


if __name__ == "__main__":
    typer.run(main)
