# generic
import numpy as np
from collections import defaultdict, namedtuple
from datetime import datetime
# plots
import matplotlib.pyplot as plt

#
# helper classes 
#
class ListToListAccumulator:
    def __init__(self, fields):
        self.fields = fields
        self.acc = defaultdict(list)
        self.counts = []
        
    def new_series(self):
        for field in self.fields:
            self.acc[field].append([])
        self.counts.append(0)

    def append_series(self, series):
        for i, field in enumerate(self.fields):
            self.acc[field].append(series[i])
        self.counts.append(len(series[0]))
        
    def append(self, values):
        for i, val in enumerate(values):
            self.acc[self.fields[i]][-1].append(val)
        self.counts[-1] += 1
        
    def first(self, field):
        return self.acc[field][0][0]

    def last(self, field):
        return self.acc[field][-1][-1]

    def count(self, idx):
        return self.counts[idx]
    
    def get(self, field, idx):
        return np.asarray(self.acc[field][idx])

class ListToHistAccumulator:
    def __init__(self, fields, bin_edges):
        self.fields = fields
        self.bin_edges = dict(zip(fields, bin_edges))        
        self.acc = defaultdict(list)
        self.counts = []

    def new_series(self):
        for field in self.fields:
            self.acc[field].append(np.zeros(len(self.bin_edges[field])-1))
        self.counts.append(0)

    def append(self, values):
        for i, val in enumerate(values):
            self.acc[self.fields[i]][-1] += val
        self.counts[-1] += 1

    def bin_and_append(self, values):
        for i, val in enumerate(values):
            self.acc[self.fields[i]][-1] += np.histogram(val, bins=self.bin_edges[self.fields[i]])[0]
        self.counts[-1] += 1
        
    def count(self, idx):
        return self.counts[idx]
    
    def get(self, field, idx):
        return self.acc[field][idx]   
    
TempMsgFields = namedtuple("Fields", "ts temp")
class TempMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return TempMsgFields(ts=ts, temp=val)

PressureMsgFields = namedtuple("Fields", "ts pressure")
class PressureMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return PressureMsgFields(ts=ts, pressure=val)

HumidityMsgFields = namedtuple("Fields", "ts humidity")
class HumidityMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.value
        return HumidityMsgFields(ts=ts, humidity=val)

RaingaugeMsgFields = namedtuple("Fields", "ts total_acc")
class RaingaugeMsgReader:
    def __init__(self):
        pass

    def read(self, msg):
        ts = msg.message.header.stamp.secs + 1E-9 * msg.message.header.stamp.nsecs
        val = msg.message.total_accumulated
        return RaingaugeMsgFields(ts=ts, total_acc=val)

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

class RadDataBinModeAggregator:
    def __init__(self, int_time=1., nbins=128, bin_edges=None):
        self.int_time = int_time
        self.live_time = 0.
        self.nbins = nbins
        self.bin_edges = bin_edges
        self.spectrum = np.zeros(nbins)
        self.live_time_agg = 0.
        self.spectrum_agg = np.zeros(nbins)
        self.ts_agg = 0.

    def aggregate(self, tf, tl, channels):
        if self.live_time == 0.:
            self.ts_agg = tf
        self.live_time += tl - tf
        self.spectrum += np.histogram(channels, bins=self.bin_edges)[0]
        if self.live_time > self.int_time:
            self.live_time_agg = self.live_time
            self.spectrum_agg = self.spectrum
            # reset aggregator
            self.live_time = 0.
            self.spectrum = np.zeros(self.nbins)
            return True
        else:
            return False

    def get_data(self):
        return self.ts_agg, self.live_time_agg, self.spectrum_agg

SeriesListDesc = namedtuple("SeriesListDesc", "topic field label legend color subplot")
class SeriesListPlot:
    def __init__(self, series, n):
        self.series = series
        self.n = n

    def versus(self, field="ts", label="time [h]", norm=3600):
        self.vs_field = field
        self.vs_label = label
        self.ref = min([acc.first(field) for acc in self.series.values()])
        self.norm = norm
        
    def add_series(self, descriptors):
        nsubplots = len(np.unique([desc.subplot for desc in descriptors]))
        fig, self.ax = plt.subplots(nsubplots, sharex=True)
        if nsubplots > 1:
            for series_idx in range(self.n):
                for desc in descriptors:
                    x = (self.series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self.series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax[desc.subplot].plot(x, y, color=desc.color, alpha=0.7)
            for desc in descriptors:
                self.ax[desc.subplot].set_ylabel(desc.label)
                if desc.legend: self.ax[desc.subplot].legend(loc="upper right")
            self.ax[-1].set_xlabel(self.vs_label)
            # add max time range for shared axis
            subplots_min_ts, subplots_max_ts = [], []
            for desc in descriptors:
                subplots_min_ts.append(self.series[desc.topic].first("ts"))
                subplots_max_ts.append(self.series[desc.topic].last("ts"))
            beg_ts = min(subplots_min_ts)
            end_ts = max(subplots_max_ts)
            beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
            end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
            self.ax[0].set_title(f"{beg} - {end}")
        else:
            for series_idx in range(self.n):
                for desc in descriptors:
                    x = (self.series[desc.topic].get(self.vs_field, series_idx) - self.ref) / self.norm
                    y = self.series[desc.topic].get(desc.field, series_idx)
                    if series_idx == 0:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7, label=desc.legend)
                    else:
                        self.ax.plot(x, y, color=desc.color, alpha=0.7)
            self.ax.set_ylabel(descriptors[0].label)
            if descriptors[0].legend: self.ax.legend(loc="upper right")
            self.ax.set_xlabel(self.vs_label)
            subplots_min_ts, subplots_max_ts = [], []
            for desc in descriptors:
                subplots_min_ts.append(self.series[desc.topic].first("ts"))
                subplots_max_ts.append(self.series[desc.topic].last("ts"))
            beg_ts = min(subplots_min_ts)
            end_ts = max(subplots_max_ts)
            beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
            end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
            self.ax.set_title(f"{beg} - {end}")

    def add_line(self, y=0., subplot=0):
        self.ax[subplot].axhline(y=y, c="black", ls="dashed", lw=1)

    def show(self):
        plt.show()

SeriesHistDesc = namedtuple("SeriesHistDesc", "topic label legend color subplot")
class SeriesHistPlot:
    def __init__(self, series, n):
        self.series = series
        self.n = n

    def binning(self, field="channels", label="energy [keV]", norm=1.):
        self.bin_field = field
        self.bin_label = label
        self.norm = norm

    def add_series(self, descriptors):
        bin_edges = self.series[descriptors[0].topic].bin_edges[self.bin_field]
        x = 0.5 * (bin_edges[1:] + bin_edges[:-1])
        nbins = len(x)
        topics = np.unique([desc.topic for desc in descriptors])
        counts = dict(zip(topics, [0 for i in range(len(topics))]))
        histos = dict(zip(topics, [np.zeros(nbins) for i in range(len(topics))]))
        for series_idx in range(self.n):
            for topic in topics:
                counts[topic] += self.series[topic].count(series_idx)
                histos[topic] += self.series[topic].get(self.bin_field, series_idx)
        nsubplots = len(np.unique([desc.subplot for desc in descriptors]))
        fig, self.ax = plt.subplots(nsubplots, sharex=True)
        if nsubplots > 1:
            for series_idx in range(self.n):
                for desc in descriptors:
                    y = histos[desc.topic]
                    if series_idx == 0:
                        self.ax[desc.subplot].step(x, y, where="mid", color=desc.color, alpha=0.7, label=f"{desc.legend}, n={counts[desc.topic]}")
                    else:
                        self.ax[desc.subplot].step(x, y, where="mid", color=desc.color, alpha=0.7)
            for desc in descriptors:
                self.ax[desc.subplot].set_yscale("log")
                self.ax[desc.subplot].set_ylabel(desc.label)
                self.ax[desc.subplot].legend(loc="upper right")
            self.ax[-1].set_xlabel(self.bin_label)
        else:
            for series_idx in range(self.n):
                for desc in descriptors:
                    y = histos[desc.topic]
                    if series_idx == 0:
                        self.ax.step(x, y, where="mid", color=desc.color, alpha=0.7, label=f"{desc.legend}, n={counts[desc.topic]}")
                    else:
                        self.ax.step(x, y, where="mid", color=desc.color, alpha=0.7)
            self.ax.set_yscale("log")
            self.ax.set_ylabel(descriptors[0].label)
            self.ax.legend(loc="upper right")
            self.ax.set_xlabel(self.bin_label)
            
    def show(self):
        plt.show()
