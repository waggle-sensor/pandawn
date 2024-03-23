# ROS
from rosbag import Bag
import yaml
# generic
import numpy as np
from collections import defaultdict, namedtuple
from datetime import datetime
# plots
import matplotlib.pyplot as plt

#
# ROS bags
#
BagInfo = namedtuple("BagInfo", "beg end duration")
def bag_info(bag):
    info_dict = yaml.safe_load(bag._get_yaml_info())
    beg_ts = info_dict['start']
    end_ts = info_dict['end']
    beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
    end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
    duration = (end_ts - beg_ts) / 3600
    return BagInfo(beg=beg, end=end, duration=duration)

#
# binning
#
def sqrt_n_bins(nmin=0, nmax=3000, nbins=128):
    """ Returns quadratically-spaced energy bins."""
    bin_edges = np.linspace(np.sqrt(nmin), np.sqrt(nmax), nbins+1)
    bin_edges = bin_edges**2
    bin_centers = 0.5 * (bin_edges[1:] + bin_edges[:-1])
    return bin_edges, bin_centers

#
# helper classes 
#
class ListToListAccumulator:
    def __init__(self, topic, fields):
        self.topic = topic
        self.fields = fields
        self.acc = defaultdict(list)
        self.counts = []
        self.nseries = 0
        self.ts = 0
        
    def new_series(self):
        if self.acc[self.fields[0]] and not self.acc[self.fields[0]][-1]:
            return
        for field in self.fields:
            self.acc[field].append([])
        self.counts.append(0)
        self.nseries += 1
    
    def append(self, values):
        if values[0] < self.ts:
            print(f"error in {self.topic} series timestamp: current {values[0]}, prev {self.ts}")
            return
        self.ts = values[0]
        for i, val in enumerate(values):
            self.acc[self.fields[i]][-1].append(val)
        self.counts[-1] += 1

    def append_series(self, series):
        for i, field in enumerate(self.fields):
            self.acc[field].append(series[i])
        self.counts.append(len(series[0]))
        
    def first(self, field):
        return self.acc[field][0][0]

    def last(self, field):
        return self.acc[field][-1][-1]

    def count(self, idx):
        return self.counts[idx]
    
    def get(self, field, idx):
        return np.asarray(self.acc[field][idx])

    def n_series(self): return self.nseries
    
    def empty(self):
        return not len(self.acc[self.fields[0]][0])

class ListToHistAccumulator:
    def __init__(self, topic, fields, bin_edges):
        self.topic = topic
        self.fields = fields
        self.bin_edges = dict(zip(fields, bin_edges))        
        self.acc = defaultdict(list)
        self.counts = []
        self.ts = 0
        self.nseries = 0

    def new_series(self):
        for field in self.fields:
            self.acc[field].append(np.zeros(len(self.bin_edges[field])-1))
        self.counts.append(0)
        self.nseries += 1

    def append(self, values):
        if values[0] < self.ts:
            print(f"error in {self.topic} series timestamp: current {values[0]}, prev {self.ts}")
            return
        self.ts = values[0]
        for i, val in enumerate(values[1:]):
            self.acc[self.fields[i]][-1] += val
        self.counts[-1] += 1

    def bin_and_append(self, values):
        if values[0] < self.ts:
            print(f"error in {self.topic} series timestamp: current {values[0]}, prev {self.ts}")
            return
        self.ts = values[0]
        for i, val in enumerate(values[1:]):
            self.acc[self.fields[i]][-1] += np.histogram(val, bins=self.bin_edges[self.fields[i]])[0]
        self.counts[-1] += 1
        
    def count(self, idx):
        return self.counts[idx]
    
    def get(self, field, idx):
        return self.acc[field][idx]

    def n_series(self): return self.nseries
    
TemperatureMsgFields = namedtuple("TemperatureMsgFields", "ts temperature")
class TemperatureMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return TemperatureMsgFields(ts=ts, temperature=val)

PressureMsgFields = namedtuple("PressureMsgFields", "ts pressure")
class PressureMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return PressureMsgFields(ts=ts, pressure=val)

HumidityMsgFields = namedtuple("HumidityMsgFields", "ts humidity")
class HumidityMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return HumidityMsgFields(ts=ts, humidity=val)

RaingaugeMsgFields = namedtuple("RaingaugeMsgFields", "ts total_acc")
class RaingaugeMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.total_accumulated
        return RaingaugeMsgFields(ts=ts, total_acc=val)

RadCalibMsgFields = namedtuple("RadCalibMsgFields", "ts gain")
class RadCalibMsgReader:
    def __init__(self):
        self.dtype = [("ts", np.dtype("float64")),
                      ("tf", np.dtype("float64")),
                      ("lt", np.dtype("float64")),
                      ("thr_lower", np.dtype("float64")),
                      ("thr_upper", np.dtype("float64")),
                      ("fmin", np.dtype("float64")),
                      ("pval", np.dtype("float64")),
                      ("res", np.dtype("float64")),
                      ("plaw", np.dtype("float64")),
                      ("gain", np.dtype("float64")),
                      ("sat", np.dtype("float64")),
                      ("off", np.dtype("float64")),
                      ("w_cosmics", np.dtype("float64")),
                      ("w_511", np.dtype("float64")),
                      ("w_K", np.dtype("float64")),
                      ("w_U", np.dtype("float64")),
                      ("w_Th", np.dtype("float64")),
                      ("w_Rn", np.dtype("float64")),
                      ("alarm", np.dtype("bool"))]

    def read(self, msg):
        data = np.frombuffer(msg.message.scalars.data, self.dtype)
        return RadCalibMsgFields(ts=data["ts"][0], gain=data["gain"][0])
    
RadMsgFields = namedtuple("RadMsgFields", "tf tl channels")
class RadMsgReader:
    def __init__(self, calibrated=True):
        if calibrated:
            self.dtype = [("det_id", np.dtype("uint8")),
                          ("timestamp_det", np.dtype("float64")),
                          ("channel", np.dtype("float32"))]
        else:
            self.dtype = [("det_id", np.dtype("uint8")),
                          ("timestamp_det", np.dtype("float64")),
                          ("channel", np.dtype("uint32"))]
        
    def read(self, msg):
        data = np.frombuffer(msg.message.arrays.data, self.dtype)
        if data.size == 0: return None
        return RadMsgFields(tf=data[0]["timestamp_det"],
                            tl=data[-1]["timestamp_det"],
                            channels=data[:]["channel"])

RadDataBinMode = namedtuple("RadDataBinMode", "ts lt spectrum")
class RadDataBinModeAggregator:
    def __init__(self, int_time=1., nbins=128, bin_edges=None, calibrated=True):
        self.int_time = int_time
        self.nbins = nbins
        self.bin_edges = bin_edges
        self.calibrated = calibrated
        self.live_time = 0.
        self.spectrum = np.zeros(nbins)
        self.ts = 0.
        self.ts_agg = 0.
        self.radData = None
        
    def aggregate(self, tf, tl, channels):
        if tf < self.ts:
            print(f"error in rad series timestamps: current {tf}, prev {self.ts}")
            return False
        # this is a temp. fix related to the dbaserh connection check we do every minute
        # Removed the check from deployed plugin from 01/19/24 on
        #
        if (tl - tf) > 1.:
            return False
        #
        self.ts = tf
        if self.live_time == 0.:
            self.ts_agg = tf
        self.live_time += tl - tf
        if not self.calibrated:
            # randomize ADC channels
            channels = np.random.rand(len(channels)) + channels
        self.spectrum += np.histogram(channels, bins=self.bin_edges)[0]
        if self.live_time >= self.int_time:
            self.radData = RadDataBinMode(ts=self.ts_agg, lt=self.live_time, spectrum=self.spectrum)
            # reset aggregator
            self.live_time = 0.
            self.spectrum = np.zeros(self.nbins)
            return True
        else:
            return False

    def data(self):
        return self.radData

class RadDataBinModeRollingAggregator:
    def __init__(self, int_time=1., step_time=1., nbins=128, bin_edges=None, calibrated=True):
        self.int_time = int_time
        self.step_time = step_time
        assert (int_time % step_time) == 0, f"The int_time {int_time} must be a multiple of step_time {step_time}"
        self.nbins = nbins
        self.bin_edges = bin_edges
        self.calibrated = calibrated
        self.n_buffers = int(int_time / step_time)
        self.buffer_idx = []
        self.live_time = np.zeros(self.n_buffers)
        self.spectrum = np.zeros((self.n_buffers, nbins))
        self.ts_agg = np.zeros(self.n_buffers)
        self.ts = 0.
        self.radData = None
        
    def aggregate(self, tf, tl, channels):
        if tf < self.ts:
            print(f"error in rad series timestamps: current {tf}, prev {self.ts}")
            return False
        self.ts = tf
        if len(self.buffer_idx) < self.n_buffers:
            self.buffer_idx = [i for i in range(int(self.live_time[0] / self.step_time) + 1)]
        for idx in self.buffer_idx:
            if self.live_time[idx] == 0.:
                self.ts_agg[idx] = tf
            self.live_time[idx] += tl - tf
            if not self.calibrated:
                # randomize ADC channels
                channels = np.random.rand(len(channels)) + channels
            self.spectrum[idx] += np.histogram(channels, bins=self.bin_edges)[0]
        if self.live_time[0] >= self.int_time:
            self.radData = RadDataBinMode(ts=self.ts_agg[0], lt=self.live_time[0], spectrum=self.spectrum[0])
            # roll aggregator
            self.ts_agg = np.roll(self.ts_agg, self.n_buffers - 1, axis=0)
            self.live_time = np.roll(self.live_time, self.n_buffers - 1, axis=0)
            self.spectrum = np.roll(self.spectrum, self.n_buffers - 1, axis=0)
            # reset aggregator
            self.live_time[-1] = 0.
            self.spectrum[-1] = np.zeros(self.nbins)
            return True
        else:
            return False

    def data(self):
        return self.radData
    
SeriesListDesc = namedtuple("SeriesListDesc", "topic field thr label legend color subplot")
MarkerDesc = namedtuple("MarkerDesc", "markers ymin ymax subplots legend color ls")
class SeriesListPlot:
    def __init__(self, series):
        self._series = series

    def versus(self, field="ts", label="time [h]", norm=3600):
        self.vs_field = field
        self.vs_label = label
        self.ref = min([acc.first(field) for acc in self._series.values() if acc.acc[field][0]])
        self.norm = norm
        
    def series(self, descriptors):
        self.nsubplots = len(np.unique([desc.subplot for desc in descriptors]))
        fig, self.ax = plt.subplots(self.nsubplots, sharex=True)
        if self.nsubplots > 1:
            for desc in descriptors:
                for series_idx in range(self._series[desc.topic].n_series()):
                    x = (self._series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self._series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7)
                    if desc.thr is not None:
                        for thr in desc.thr:
                            self.ax[desc.subplot].axhline(y=thr, c="black", ls="dashed", lw=1)         
            for desc in descriptors:
                self.ax[desc.subplot].set_ylabel(desc.label)
                if desc.legend: self.ax[desc.subplot].legend(loc="upper right")
            self.ax[-1].set_xlabel(self.vs_label)
            # add max time range for shared axis
            subplots_min_ts, subplots_max_ts = [], []
            for desc in descriptors:
                subplots_min_ts.append(self._series[desc.topic].first("ts"))
                subplots_max_ts.append(self._series[desc.topic].last("ts"))
            beg_ts = min(subplots_min_ts)
            end_ts = max(subplots_max_ts)
            beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
            end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
            self.ax[0].set_title(f"{beg} - {end}")
        else:
            for desc in descriptors:
                for series_idx in range(self._series[desc.topic].n_series()):
                    x = (self._series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self._series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7)
                    if desc.thr is not None:
                        for thr in desc.thr:
                            self.ax.axhline(y=thr, c="black", ls="dashed", lw=1)        
            self.ax.set_ylabel(descriptors[0].label)
            if descriptors[0].legend: self.ax.legend(loc="upper right")
            self.ax.set_xlabel(self.vs_label)
            # add max time range for shared axis
            subplots_min_ts, subplots_max_ts = [], []
            for desc in descriptors:
                subplots_min_ts.append(self._series[desc.topic].first("ts"))
                subplots_max_ts.append(self._series[desc.topic].last("ts"))
            beg_ts = min(subplots_min_ts)
            end_ts = max(subplots_max_ts)
            beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
            end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
            self.ax.set_title(f"{beg} - {end}")
        # start all plots at 0 ref in time
        plt.gca().set_xlim(left=0)

    def add_series(self, descriptors):
        if self.nsubplots > 1:
            for desc in descriptors:
                for series_idx in range(self._series[desc.topic].n_series()):
                    x = (self._series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self._series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7)
                    if desc.thr is not None:
                        self.ax[desc.subplot].axhline(y=desc.thr, c="black", ls="dashed", lw=1)
        else:
            for desc in descriptors:
                for series_idx in range(self._series[desc.topic].n_series()):
                    x = (self._series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self._series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7)
                    if desc.thr is not None:
                        self.ax.axhline(y=desc.thr, c="black", ls="dashed", lw=1)

    def add_markers(self, descriptors):
        for desc in descriptors:
            markers = (np.asarray(desc.markers) - self.ref)/self.norm
            if desc.subplots:
                for subplot in desc.subplots:
                    for idx, marker in enumerate(markers):
                        if idx == 0:
                            self.ax[subplot].axvline(x=marker, ymin=desc.ymin, ymax=desc.ymax, color=desc.color, ls=desc.ls, lw=1, alpha=0.4, label=desc.legend)
                        else:
                            self.ax[subplot].axvline(x=marker, ymin=desc.ymin, ymax=desc.ymax, color=desc.color, ls=desc.ls, lw=1, alpha=0.4)
            else:
                for idx, marker in enumerate(markers):
                    if idx == 0:
                        self.ax.axvline(x=marker, ymin=desc.ymin, ymax=desc.ymax, color=desc.color, ls=desc.ls, lw=1, alpha=0.4, label=desc.legend)
                    else:
                        self.ax.axvline(x=marker, ymin=desc.ymin, ymax=desc.ymax, color=desc.color, ls=desc.ls, lw=1, alpha=0.4)

    def show(self):
        plt.show()

SeriesHistDesc = namedtuple("SeriesHistDesc", "topic label legend color subplot")
class SeriesHistPlot:
    def __init__(self, series):
        self._series = series

    def binning(self, field="channels", label="energy [keV]", norm=1.):
        self.bin_field = field
        self.bin_label = label
        self.norm = norm

    def series(self, descriptors):
        bin_edges = self._series[descriptors[0].topic].bin_edges[self.bin_field]
        x = 0.5 * (bin_edges[1:] + bin_edges[:-1])
        nbins = len(x)
        topics = np.unique([desc.topic for desc in descriptors])
        counts = dict(zip(topics, [0 for i in range(len(topics))]))
        histos = dict(zip(topics, [np.zeros(nbins) for i in range(len(topics))]))
        for topic in topics:
            for series_idx in range(self._series[topic].n_series()):
                counts[topic] += self._series[topic].count(series_idx)
                histos[topic] += self._series[topic].get(self.bin_field, series_idx)
        self.nsubplots = len(np.unique([desc.subplot for desc in descriptors]))
        fig, self.ax = plt.subplots(self.nsubplots, sharex=True)
        if self.nsubplots > 1:
            for desc in descriptors:
                y = histos[desc.topic]
                self.ax[desc.subplot].step(x, y, where="mid", color=desc.color, alpha=0.7, label=f"{desc.legend}, n={counts[desc.topic]}")
            for desc in descriptors:
                self.ax[desc.subplot].set_yscale("log")
                self.ax[desc.subplot].set_ylabel(desc.label)
                self.ax[desc.subplot].legend(loc="upper right")
            self.ax[-1].set_xlabel(self.bin_label)
        else:
            for desc in descriptors:
                y = histos[desc.topic]
                self.ax.step(x, y, where="mid", color=desc.color, alpha=0.7, label=f"{desc.legend}, n={counts[desc.topic]}")
            self.ax.set_yscale("log")
            self.ax.set_ylabel(descriptors[0].label)
            self.ax.legend(loc="upper right")
            self.ax.set_xlabel(self.bin_label)
            
    def show(self):
        plt.show()
