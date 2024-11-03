import typing
from dataclasses import replace
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyxdf
from ezmsg.lsl.util import AxisArray
from frozendict import frozendict


MultiStreamMessage = frozendict[
    str, typing.Union[AxisArray, tuple[AxisArray, ...]]
]  # type alias


class XDFIterator:
    """
    An Iterator that yields chunks from an XDF.
    A typical offline analysis might load the entire file into memory, then perform a processing step on the entire
    recording duration, and the next step on the entire result of the first step, and so on. This might require a
    tremendous amount of memory and, if one is not careful about memory layout, can be incredibly slow. An alternative
    procedure is to load the file into memory a chunk at a time (see Note1), then pass that chunk through the entire
    processing pipeline, then proceed onto the next chunk (See Note2). We create an Iterator to provide our chunks.
    > Note1: I have not written a true lazy-loader for XDF because it has not yet been necessary as the files are all
    small. Thus, I use pyxdf.load_xdf which loads the entire raw data into memory. The processing is still done chunk-by-chunk.
    > Note2: It should be possible to start on chunk[ix+1] while chunk[ix] is still going through the pipeline. Indeed,
    this is (optionally) how it works online. However, the overhead of setting this up for offline analysis is not worth
    the gain, at least not at this stage.
    """

    def __init__(
        self,
        filepath: typing.Union[Path, str],
        select: typing.Optional[
            set[str]
        ] = None,  # If set, then the iterator yields only AxisArray of selected stream(s).
        # If None (default), then the iterator yields dicts with keys for each stream
        chunk_dur: float = 1.0,  # Attempt to chunk data into chunks of this duration.
        start_time: typing.Optional[float] = None,
        stop_time: typing.Optional[float] = None,
        rezero: bool = True,
    ):
        if isinstance(filepath, str):
            filepath = Path(filepath).expanduser()
        self._filepath = filepath
        self._select = select
        self._chunk_dur = chunk_dur
        self._rezero = rezero
        self._n_chunks = 0
        self._t0 = 0.0
        self._chunk_ix = 0
        self._last_time = 0.0
        self._metadata = {}
        self._prev_file_read_s: float = (
            0  # File read header in seconds for previous iteration
        )
        self._time_range: typing.Tuple[
            typing.Optional[float], typing.Optional[float]
        ] = (start_time, stop_time)
        self._scan_file()

    def _scan_file(self):
        # Note: For larger datafiles we wouldn't want to load the entire thing into memory with load_xdf.
        #  Instead, get a file handle, then
        #  - Scan the file for chunk boundaries and timestamps
        #  - Maintain a list of chunk boundaries
        #  - Perform timestamp corrections (maintain corrected ts in memory or use func to correct during next pass?)
        #  - Iterator operates on original chunk-boundaries, but using corrected timestamps.
        #  However, we would need a custom file parser for that. For now, we load the relatively small
        #  file into memory simply with pyxdf.load_xdf then iterate over the items in memory
        #  at a user-defined chunk boundary (`chunk_dur`).
        # Load xdf
        self._streams, fileheader = pyxdf.load_xdf(
            self._filepath,
            select_streams=None
            if (self._select is None or self._rezero)
            else [{"name": _} for _ in self._select],
        )
        self._metadata = {}
        self._file_read_s = 0
        self._prev_file_read_s = 0
        xdf_t0 = np.inf
        xdf_tmax = 0
        for strm in self._streams:
            # Convert empty data to an array for easier slicing
            if type(strm["time_series"]) is list:
                strm["time_series"] = np.array(strm["time_series"])

            # Get more digestable metadata
            info = strm["info"]
            new_meta = {
                "name": info["name"][0],
                "type": info["type"][0],
                "channel_count": int(info["channel_count"][0]),
                "nominal_srate": float(info["nominal_srate"][0]),
            }
            self._metadata[new_meta["name"]] = new_meta

            # Update time range limits
            tvec = strm["time_stamps"]
            if len(tvec) > 0:
                xdf_t0 = min(xdf_t0, tvec[0])
                xdf_tmax = max(xdf_tmax, tvec[-1])

        # Permanently modify streams' time stamps
        if self._rezero:
            for strm in self._streams:
                strm["time_stamps"] = strm["time_stamps"] - xdf_t0
            xdf_tmax -= xdf_t0
            xdf_t0 = 0

        # Adjust for provided time bounds
        for strm in self._streams:
            tvec = strm["time_stamps"]
            if len(tvec) > 0:
                b_keep = np.ones(len(tvec), dtype=bool)
                if self._time_range[0] is not None:
                    b_keep = np.logical_and(b_keep, tvec >= self._time_range[0])
                if self._time_range[1] is not None:
                    b_keep = np.logical_and(b_keep, tvec <= self._time_range[1])
                if np.any(~b_keep):
                    strm["time_stamps"] = tvec[b_keep]
                    strm["timeseries"] = strm["timeseries"][b_keep]

        # Recalculate tmax
        xdf_dur = 0
        for strm in self._streams:
            tvec = strm["time_stamps"]
            srate = float(strm["info"]["nominal_srate"][0])
            adj = (1/srate if srate > 0 else 0) - xdf_t0
            if len(tvec) > 0:
                xdf_dur = max(xdf_dur, tvec[-1] + adj)

        # Chunking
        self._n_chunks = int(np.ceil(xdf_dur / self._chunk_dur))
        self._t0 = xdf_t0

        # Drop streams that were not selected. (Could not drop earlier due to timestamp rezero)
        if self._rezero and self._select is not None:
            stream_names = [_["info"]["name"][0] for _ in self._streams]
            self._streams = [self._streams[stream_names.index(_)] for _ in self._select]
            self._metadata = {k: self._metadata[k] for k in self._select}

        print(
            f"Imported {len(self._streams)} streams from {self._filepath} "
            f"spanning {xdf_dur:.2f} s beginning at t={xdf_t0:.2f}."
        )

    @property
    def stream_meta(self) -> typing.Union[list[dict], dict]:
        return self._metadata

    @property
    def n_chunks(self) -> int:
        return self._n_chunks

    def __iter__(self):
        self._chunk_ix = 0
        return self

    def __next__(self) -> dict[str, tuple[npt.NDArray, npt.NDArray]]:
        if self._chunk_ix >= self.n_chunks:
            raise StopIteration
        else:
            out_dict = {}
            t_start, t_stop = (
                self._chunk_ix * self._chunk_dur + self._t0,
                (self._chunk_ix + 1) * self._chunk_dur + self._t0,
            )
            for strm in self._streams:
                b_chunk = np.logical_and(
                    strm["time_stamps"] >= t_start, strm["time_stamps"] < t_stop
                )
                out_tvec = strm["time_stamps"][b_chunk]
                out_data = strm["time_series"][b_chunk]
                out_dict[strm["info"]["name"][0]] = (out_data, out_tvec)
                if len(out_tvec) > 0:
                    self._last_time = max(self._last_time, out_tvec[-1])
            self._chunk_ix += 1
            return out_dict


def labels_from_strm(strm: dict) -> list[str]:
    desc = strm["info"]["desc"][0]
    if desc is not None and "channels" in desc:
        labels = [_["label"][0] for _ in desc["channels"][0]["channel"]]
    else:
        n_ch = int(strm["info"]["channel_count"][0])
        labels = [str(_ + 1) for _ in range(n_ch)]
    return labels


class XDFAxisArrayIterator(XDFIterator):
    """
    This Iterator loads only a single stream and yields a single AxisArray object per chunk.
    """

    def __init__(self, *args, select: str, **kwargs):
        kwargs["select"] = set((select,))
        super().__init__(*args, **kwargs)
        _sel = [_ for _ in self._select][0]
        fs = self._metadata[_sel]["nominal_srate"] or 1.0
        labels = labels_from_strm(self._streams[0])

        self._template = AxisArray(
            data=np.zeros(
                (0, len(labels)), dtype=self._streams[0]["time_series"].dtype
            ),
            dims=["time", "ch"],
            axes={
                "time": AxisArray.Axis.TimeAxis(fs=fs, offset=0.0),
                "ch": AxisArray.Axis.SpaceAxis(labels=labels),
            },
            key=self._streams[0]["info"]["name"][0],
        )

    def __next__(self) -> AxisArray:
        data = self._template.data
        tvec = []
        chunk_dict = super().__next__()
        # Should only be 1 in self._select
        for strm_name in self._select:
            if strm_name in chunk_dict:
                data, tvec = chunk_dict[strm_name]
                result = replace(
                    self._template,
                    data=data,
                    axes={
                        **self._template.axes,
                        "time": replace(
                            self._template.axes["time"],
                            offset=tvec[0] if len(tvec) else self._last_time,
                        ),
                    },
                )
        return result


class XDFMultiAxArrIterator(XDFIterator):
    def __init__(self, *args, force_single_sample: set = set(), **kwargs):
        super().__init__(*args, **kwargs)
        self._force_single_sample = force_single_sample
        stream_names = [_["info"]["name"][0] for _ in self._streams]

        # Create template messages for each stream
        self._templates = {}
        for stream_name, stream_meta in self._metadata.items():
            stream = self._streams[stream_names.index(stream_name)]
            labels = labels_from_strm(stream)
            fs = stream_meta["nominal_srate"]
            self._templates[stream_name] = AxisArray(
                data=np.zeros(
                    (0, stream_meta["channel_count"]), dtype=stream["time_series"].dtype
                ),
                dims=["time", "ch"],
                axes={
                    "time": AxisArray.Axis.TimeAxis(fs=fs or 1.0, offset=0.0),
                    "ch": AxisArray.Axis.SpaceAxis(labels=labels),
                },
                key=stream_name,
            )

    def __next__(self) -> MultiStreamMessage:
        # Prepare output
        chunk_dict = super().__next__()
        out_dict = {}
        for k, template in self._templates.items():
            if k in chunk_dict and len(chunk_dict[k][1]) > 0:
                data, tvec = chunk_dict[k]
                if k in self._force_single_sample:
                    msgs = []
                    for ix, _t in enumerate(tvec):
                        msgs.append(
                            replace(
                                template,
                                data=data[ix : ix + 1],
                                axes={
                                    **template.axes,
                                    "time": replace(template.axes["time"], offset=_t),
                                },
                            )
                        )
                    out_dict[k] = tuple(msgs)
                else:
                    out_dict[k] = (
                        replace(
                            template,
                            data=data,
                            axes={
                                **template.axes,
                                "time": replace(
                                    template.axes["time"],
                                    offset=tvec[0] if len(tvec) else self._last_time,
                                ),
                            },
                        ),
                    )
        return MultiStreamMessage(out_dict)
