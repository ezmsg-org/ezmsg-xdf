import asyncio
import os
import queue
import typing
from dataclasses import field
from pathlib import Path

import ezmsg.core as ez
import numpy as np
import numpy.typing as npt
import pyxdf
from ezmsg.baseproc.protocols import processor_state
from ezmsg.baseproc.stateful import BaseStatefulProducer
from ezmsg.util.messages.axisarray import AxisArray
from ezmsg.util.messages.util import replace


class XDFIterator:
    def __init__(
        self,
        filepath: Path | str,
        select: set[str] | None = None,  # If set, then the iterator yields only AxisArray of selected stream(s).
        # If None (default), then the iterator yields dicts with keys for each stream
        chunk_dur: float = 1.0,  # Attempt to chunk data into chunks of this duration.
        start_time: float | None = None,
        stop_time: float | None = None,
        rezero: bool = True,
    ):
        """
        An Iterator that yields chunks from an XDF.
        A typical offline analysis might load the entire file into memory, then perform a processing step on the entire
        recording duration, and the next step on the entire result of the first step, and so on. This might require a
        tremendous amount of memory and, if one is not careful about memory layout, can be incredibly slow. An
        alternative procedure is to load the file into memory a chunk at a time (see Note1), then pass that chunk
        through the entire processing pipeline, then proceed onto the next chunk (See Note2). We create an Iterator to
        provide our chunks.
        > Note1: I have not written a true lazy-loader for XDF because it has not yet been necessary as the files are
          all small. Thus, I use pyxdf.load_xdf which loads the entire raw data into memory. The processing is still
          done chunk-by-chunk.
        > Note2: It should be possible to start on chunk[ix+1] while chunk[ix] is still going through the pipeline.
          Indeed, this is (optionally) how it works online. However, the overhead of setting this up for offline
          analysis is not worth the gain, at least not at this stage.

        Args:
            filepath: The path to the file to load and iterate over.
            select: (Optional) A set of stream names to select. If None, then all streams are selected.
            chunk_dur: The duration of each chunk in seconds.
            start_time: Start playback at this time. If rezero is True then this is relative to the file start time.
                If rezero is False then this is relative to the original timestamps.
            stop_time: Truncate the playback to stop at this time. If rezero is True then this is relative to the file
                start time. If rezero is False then this is relative to the original timestamps.
            rezero: The absolute value of timestamps in an XDF file are useful for synchronization WITHIN file, but they
                are absolutely meaningless outside the exact XDF file like in an ezmsg application. Thus, by default we
                rezero the timestamps to start at t=0.0 for simplicity. However, there may be rare circumstances where
                one wants to compare the timestamps produced by ezmsg to timestamps produced by another XDF analysis
                tool that does not rezero. In that case, set rezero=False.
        """
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
        self._prev_file_read_s: float = 0  # File read header in seconds for previous iteration
        self._time_range: tuple[float | None, float | None] = (start_time, stop_time)
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
            select_streams=None if (self._select is None or self._rezero) else [{"name": _} for _ in self._select],
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
            adj = (1 / srate if srate > 0 else 0) - xdf_t0
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

        ez.logger.info(
            f"Imported {len(self._streams)} streams from {self._filepath} "
            f"spanning {xdf_dur:.2f} s beginning at t={xdf_t0:.2f}."
        )

    @property
    def stream_meta(self) -> list[dict] | dict:
        return self._metadata

    @property
    def n_chunks(self) -> int:
        return self._n_chunks

    @property
    def exhausted(self) -> bool:
        """True once every chunk boundary has been handed out."""
        return self._chunk_ix >= self._n_chunks

    def __iter__(self):
        self._chunk_ix = 0
        return self

    def __next__(self) -> dict[str, tuple[npt.NDArray, npt.NDArray]]:
        if self.exhausted:
            raise StopIteration
        else:
            out_dict = {}
            t_start, t_stop = (
                self._chunk_ix * self._chunk_dur + self._t0,
                (self._chunk_ix + 1) * self._chunk_dur + self._t0,
            )
            for strm in self._streams:
                b_chunk = np.logical_and(strm["time_stamps"] >= t_start, strm["time_stamps"] < t_stop)
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


def _build_template(stream: dict, name: str, n_ch: int, fs: float) -> AxisArray:
    """The message every chunk of *stream* is a `replace` of.

    Built once per stream so the `ch` axis object -- and the fingerprint cached
    on it -- is shared by every message, which is what makes priming cheap.
    """
    labels = labels_from_strm(stream)
    time_ax = (
        AxisArray.TimeAxis(fs=fs, offset=0.0)
        if fs
        else AxisArray.CoordinateAxis(data=np.array([]), dims=["time"], unit="s")
    )
    ch_ax = AxisArray.CoordinateAxis(data=np.array(labels), dims=["ch"])
    # Compute the channel fingerprint once, now. It is cached on the axis and
    # pickled with it, and every message from this stream reuses this same axis
    # object, so one checksum covers the whole file. Left cold it would be
    # computed by the first stateful consumer in this process -- and, since
    # unpickling builds a new axis object per message, by the first consumer in
    # every other process, on every message.
    ch_ax.fingerprint
    return AxisArray(
        data=np.zeros((0, n_ch), dtype=stream["time_series"].dtype),
        dims=["time", "ch"],
        axes={"time": time_ax, "ch": ch_ax},
        key=name,
        # Messages accumulate along `time`, whether the stream is regular or
        # carries per-sample timestamps; `ch` describes the stream itself.
        chunk_dim="time",
    )


def _with_time(template: AxisArray, data: npt.NDArray, tvec: npt.NDArray, fallback_t: float) -> AxisArray:
    """A chunk message: the template's data replaced, and its time axis advanced.

    An irregular stream carries every timestamp; a regular one carries only where
    the chunk starts, since its gain says the rest.
    """
    time_ax = template.axes["time"]
    if isinstance(time_ax, AxisArray.CoordinateAxis):
        t_kwargs = {"data": tvec if len(tvec) else np.array([])}
    else:
        t_kwargs = {"offset": tvec[0] if len(tvec) else fallback_t}
    return replace(
        template,
        data=data,
        axes={**template.axes, "time": replace(time_ax, **t_kwargs)},
    )


class XDFIteratorSettings(ez.Settings):
    """Settings shared by both AxisArray iterators.

    ``playback_rate`` and ``self_terminating`` belong to the unit rather than to
    the reader, and are listed in :attr:`NONRESET_SETTINGS_FIELDS` so changing
    either does not reopen the file.
    """

    filepath: typing.Union[os.PathLike, str]
    select: str = ""
    chunk_dur: float = 1.0
    start_time: float | None = None
    stop_time: float | None = None
    rezero: bool = True
    playback_rate: float | None = None
    self_terminating: bool = False
    """
    If True, the unit will raise a :obj:`ez.NormalTermination` exception when the file is exhausted.
    Note, however, that this will terminate the pipeline even if the data published by this unit are still in transit,
    which will lead to the pipeline output being truncated before it has finished processing the stream.
    `self_terminating` should only be used when it is not important that the pipeline finish processing data, such
    as during prototyping and testing.
    """


class XDFMultiIteratorSettings(XDFIteratorSettings):
    select: set[str] | None = None
    force_single_sample: set = field(default_factory=set)


@processor_state
class XDFIteratorState:
    reader: XDFIterator | None = None
    template: AxisArray | None = None


@processor_state
class XDFMultiIteratorState:
    reader: XDFIterator | None = None
    templates: dict | None = None
    pubqueue: queue.SimpleQueue | None = None


class _XDFProducerBase:
    """Shared plumbing for the two AxisArray producers.

    The file load is deliberately *not* run from ``__init__``. ezmsg-neo and
    ezmsg-nwb both reset eagerly there and so pay the whole open on the event
    loop during ``initialize``; here the first ``__acall__`` triggers
    ``_areset_state``, which puts it on a worker thread. Nothing in this
    package's public surface reads stream metadata before the first chunk, so
    there is nothing to lose by waiting.
    """

    NONRESET_SETTINGS_FIELDS = frozenset({"playback_rate", "self_terminating"})

    async def _areset_state(self) -> None:
        """Offload the sync open onto a worker thread.

        ``pyxdf.load_xdf`` reads and decodes the entire file, which for a
        several-hundred-megabyte recording is seconds of pure CPU and I/O. On the
        event loop that stalls every other unit in the process.
        """
        await asyncio.to_thread(self._reset_state)

    def _build_reader(self, select: set[str] | None) -> XDFIterator:
        return XDFIterator(
            filepath=self.settings.filepath,
            select=select,
            chunk_dur=self.settings.chunk_dur,
            start_time=self.settings.start_time,
            stop_time=self.settings.stop_time,
            rezero=self.settings.rezero,
        )


class XDFAxisArrayIterator(
    _XDFProducerBase,
    BaseStatefulProducer[XDFIteratorSettings, AxisArray, XDFIteratorState],
):
    """Loads a single stream and produces one :obj:`AxisArray` per chunk.

    ``select`` must be a single stream name, unlike :obj:`XDFIterator`.
    """

    @property
    def exhausted(self) -> bool:
        reader = self._state.reader
        return reader is not None and reader.exhausted

    def _reset_state(self) -> None:
        reader = self._build_reader({self.settings.select})
        meta = reader.stream_meta[self.settings.select]
        self._state.reader = reader
        self._state.template = _build_template(
            reader._streams[0],
            name=reader._streams[0]["info"]["name"][0],
            n_ch=meta["channel_count"],
            fs=meta["nominal_srate"],
        )

    async def _produce(self) -> AxisArray | None:
        reader = self._state.reader
        try:
            chunk_dict = next(reader)
        except StopIteration:
            return None
        data, tvec = chunk_dict.get(self.settings.select, (None, None))
        if data is None:
            return None
        return _with_time(self._state.template, data, tvec, reader._last_time)

    def __next__(self) -> AxisArray:
        result = self()
        if result is None:
            raise StopIteration
        return result


class XDFMultiAxArrIterator(
    _XDFProducerBase,
    BaseStatefulProducer[XDFMultiIteratorSettings, AxisArray, XDFMultiIteratorState],
):
    """Loads multiple streams and produces one :obj:`AxisArray` per iteration.

    Which stream a given message came from varies; read ``.key``. Returns
    ``None`` when a chunk held nothing for any stream, and raises
    ``StopIteration`` only once the file is done.

    ``force_single_sample`` names irregular-rate streams that may carry more than
    one event within ``chunk_dur``, which :obj:`AxisArray` cannot represent as a
    single message with correct timestamps; those are split one event per
    message.
    """

    @property
    def exhausted(self) -> bool:
        reader = self._state.reader
        if reader is None:
            return False
        return reader.exhausted and self._state.pubqueue.empty()

    def _reset_state(self) -> None:
        reader = self._build_reader(self.settings.select)
        stream_names = [_["info"]["name"][0] for _ in reader._streams]
        self._state.reader = reader
        self._state.pubqueue = queue.SimpleQueue()
        self._state.templates = {
            name: _build_template(
                reader._streams[stream_names.index(name)],
                name=name,
                n_ch=meta["channel_count"],
                fs=meta["nominal_srate"],
            )
            for name, meta in reader.stream_meta.items()
        }

    def _enqueue_chunk(self, chunk_dict: dict) -> None:
        reader = self._state.reader
        for name, template in self._state.templates.items():
            if name not in chunk_dict or len(chunk_dict[name][1]) == 0:
                continue
            data, tvec = chunk_dict[name]
            if name in self.settings.force_single_sample:
                for ix, stamp in enumerate(tvec):
                    self._state.pubqueue.put_nowait(_with_time(template, data[ix : ix + 1], np.array([stamp]), stamp))
            else:
                self._state.pubqueue.put_nowait(_with_time(template, data, tvec, reader._last_time))

    async def _produce(self) -> AxisArray | None:
        if self._state.pubqueue.empty():
            try:
                self._enqueue_chunk(next(self._state.reader))
            except StopIteration:
                return None
        try:
            return self._state.pubqueue.get_nowait()
        except queue.Empty:
            return None

    def __next__(self) -> AxisArray | None:
        if self.exhausted:
            raise StopIteration
        return self()
