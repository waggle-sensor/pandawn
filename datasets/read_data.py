# ROS
from rosbag import Bag 
# generic
import time
import numpy as np
# PANDA
from utils import sqrt_n_bins, ListToListAccumulator, ListToHistAccumulator
from utils import TemperatureMsgReader, PressureMsgReader, HumidityMsgReader, RaingaugeMsgReader
from utils import RadMsgReader, RadDataBinModeAggregator
from utils import SeriesListDesc, SeriesListPlot, SeriesHistDesc, SeriesHistPlot
# args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs='+', type=str)
args = parser.parse_args()

#
# msg readers/aggregators
#
msgTopics = ["/sensors/env/temperature",
             "/sensors/env/pressure",
             "/sensors/env/humidity",
             "/sensors/raingauge",
             "/dbaserh/listmode",
             "/calib/listmode"]
tempMsgReader = TemperatureMsgReader()
pressureMsgReader = PressureMsgReader()
humidityMsgReader = HumidityMsgReader()
raingaugeMsgReader = RaingaugeMsgReader()
dbaserhRadMsgReader = RadMsgReader(calibrated=False)
dbaserhRadDataAgg = RadDataBinModeAggregator(int_time=1., nbins=1024, bin_edges=np.arange(1025), calibrated=False)
calRadMsgReader = RadMsgReader()
calRadDataAgg = RadDataBinModeAggregator(int_time=1., nbins=128, bin_edges=sqrt_n_bins(nmin=50, nmax=3000, nbins=128)[0])

#
# accumulators
#
series = {}
topicFields = {"/sensors/env/temperature": ["ts", "temp"],
               "/sensors/env/pressure": ["ts", "pressure"],
               "/sensors/env/humidity": ["ts", "humidity"],
               "/sensors/raingauge": ["ts", "accumulation"],
               "/calib/listmode": ["ts", "rate"]}
for topic, fields in topicFields.items():
    series[topic] = ListToListAccumulator(topic, fields)
histograms = {}
topicFields = {"uncalibrated": ["channels"],
               "calibrated": ["energies"]}
topicBins = {"uncalibrated": [np.arange(1025)],
             "calibrated": [sqrt_n_bins(nmin=50, nmax=3000, nbins=128)[0]]}
for topic, fields in topicFields.items():
    histograms[topic] = ListToHistAccumulator(topic, fields, topicBins[topic])
    
#
# loop over data files
#
for file in args.files:
    start = time.time()
    for acc in series.values(): acc.new_series()
    for acc in histograms.values(): acc.new_series()
    with Bag(file, 'r') as bag:
        print("opening file:", file)
        msgs = bag.read_messages(topics=msgTopics)
        for msg in msgs:
            # read temperature data
            if (msg.topic == "/sensors/env/temperature") and (msg.message.zone[0] == 'e'):
                fields = tempMsgReader.read(msg)
                series[msg.topic].append([fields.ts, fields.temperature])
            
            # read pressure data
            if (msg.topic == "/sensors/env/pressure") and (msg.message.zone[0] == 'e'):
                fields = pressureMsgReader.read(msg)
                series[msg.topic].append([fields.ts, fields.pressure])

            # read humidity data
            if (msg.topic == "/sensors/env/humidity") and (msg.message.zone[0] == 'e'):
                fields = humidityMsgReader.read(msg)
                series[msg.topic].append([fields.ts, fields.humidity])

            # read raingauge data
            if msg.topic == "/sensors/raingauge":
                fields = raingaugeMsgReader.read(msg)
                series[msg.topic].append([fields.ts, fields.total_acc])
            
            # read dbaserh rad data
            if msg.topic == "/dbaserh/listmode":
                fields = dbaserhRadMsgReader.read(msg)
                if fields is None: continue
                if dbaserhRadDataAgg.aggregate(fields.tf, fields.tl, fields.channels) is True:
                    ts, lt, spectrum = dbaserhRadDataAgg.data()
                    histograms["uncalibrated"].append([ts, spectrum])
            
            # read calibrated rad data
            if msg.topic == "/calib/listmode":
                fields = calRadMsgReader.read(msg)
                if fields is None: continue
                if calRadDataAgg.aggregate(fields.tf, fields.tl, fields.channels) is True:
                    ts, lt, spectrum = calRadDataAgg.data()
                    series[msg.topic].append([ts, spectrum.sum()/lt])
                    histograms["calibrated"].append([ts, spectrum])
            
        stop = time.time()
        print(f"file processed in {(stop - start)}s")

#
# plots
#
precipitation = ListToListAccumulator("/sensors/precipitation", ["ts", "precipitation"])
for i in range(series["/sensors/raingauge"].n_series()):
    ts = series["/sensors/raingauge"].get("ts", i)
    acc = series["/sensors/raingauge"].get("accumulation", i)
    ts = 0.5 * (ts[1:] + ts[:-1])
    prec = acc[1:] - acc[:-1]
    precipitation.append_series([list(ts), list(prec)])
series["/sensors/precipitation"] = precipitation

plot = SeriesListPlot(series)
plot.versus(field="ts", label="time [h]", norm=3600)
plot.series([SeriesListDesc(topic="/sensors/env/temperature", field="temp", thr=None, label="T [Celsius]", legend="", color="gray", subplot=0),
                 SeriesListDesc(topic="/sensors/env/pressure", field="pressure", thr=None, label="p [Pa]", legend="", color="gray", subplot=1),
                 SeriesListDesc(topic="/sensors/env/humidity", field="humidity", thr=None, label="H [%]", legend="", color="gray", subplot=2),
                 SeriesListDesc(topic="/sensors/precipitation", field="precipitation", thr=None, label="precipitation [AU]", legend="", color="gray", subplot=3),
                 SeriesListDesc(topic="/calib/listmode", field="rate", thr=None, label="count rate [cnts/s]", legend="", color="gray", subplot=4)])
plot.show()

plot = SeriesHistPlot(histograms)
plot.binning(field="channels", label="Channel [ADC]")
plot.series([SeriesHistDesc(topic="uncalibrated", label="entries", legend="uncalibrated", color="gray", subplot=0)])
plot.show()

plot = SeriesHistPlot(histograms)
plot.binning(field="energies", label="Energy [keV]")
plot.series([SeriesHistDesc(topic="calibrated", label="entries", legend="calibrated", color="gray", subplot=0)])
plot.show()
