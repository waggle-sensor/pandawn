# ROS
from rosbag import Bag 
# generic
import time
import numpy as np
# PANDA
from utils import ListToListAccumulator, ListToHistAccumulator
from utils import TempMsgReader, PressureMsgReader, HumidityMsgReader, RaingaugeMsgReader
from utils import RadMsgReader, RadDataBinModeAggregator
from utils import SeriesListDesc, SeriesListPlot, SeriesHistDesc, SeriesHistPlot
# args
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs='+', type=str)
args = parser.parse_args()
NFILES = len(args.files)

#
# msg readers/aggregators
#
tempMsgReader = TempMsgReader()
pressureMsgReader = PressureMsgReader()
humidityMsgReader = HumidityMsgReader()
raingaugeMsgReader = RaingaugeMsgReader()
radMsgReader = RadMsgReader(calibrated=False)
INT_TIME = 1.
NBINS = 1024
BIN_EDGES = np.arange(NBINS+1)
BIN_CENTERS = 0.5 * (BIN_EDGES[1:] + BIN_EDGES[:-1])
radDataAgg = RadDataBinModeAggregator(int_time=INT_TIME, nbins=NBINS, bin_edges=BIN_EDGES)

#
# accumulators
#
series = {}
topicFields = {"/sensors/env/temperature": ["ts", "temp"],
               "/sensors/env/pressure": ["ts", "pressure"],
               "/sensors/env/humidity": ["ts", "humidity"],
               "/sensors/raingauge": ["ts", "accumulation"],
               "/dbaserh/listmode": ["ts", "rate"]}
for topic, fields in topicFields.items():
    series[topic] = ListToListAccumulator(fields)
histograms = {}
histoFields = {"uncalibrated": ["channels"]}
histoBins = {"uncalibrated": [BIN_EDGES]}
for histo, fields in histoFields.items():
    histograms[histo] = ListToHistAccumulator(fields, histoBins[histo])

#
# loop over data files
#
for file in args.files:
    start = time.time()
    for acc in series.values(): acc.new_series()
    for acc in histograms.values(): acc.new_series()
    with Bag(file, 'r') as bag:
        print("opening file:", file)
        msgs = bag.read_messages(topics=topicFields.keys())
        for msg in msgs:
            # read temperature data
            if (msg.topic == "/sensors/env/temperature") and (msg.message.zone[0] == 'e'):
                fields = tempMsgReader.read(msg)
                series[msg.topic].append([fields.ts, fields.temp])

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
                
            # read rad data
            if msg.topic == "/dbaserh/listmode":
                fields = radMsgReader.read(msg)
                if fields is None: continue
                if radDataAgg.aggregate(fields.tf, fields.tl, fields.channels) is True:
                    ts, lt, spectrum = radDataAgg.get_data()
                    series[msg.topic].append([ts, spectrum.sum()/lt])
                    histograms["uncalibrated"].append([spectrum])
        stop = time.time()
        print(f"file processed in {(stop - start)}s")

#
# plots
#
precipitation = ListToListAccumulator(["ts", "precipitation"])
for i in range(NFILES):
    ts = series["/sensors/raingauge"].get("ts", i)
    acc = series["/sensors/raingauge"].get("accumulation", i)
    ts = 0.5 * (ts[1:] + ts[:-1])
    prec = acc[1:] - acc[:-1]
    precipitation.append_series([ts, prec])
series["/sensors/precipitation"] = precipitation
    
plot = SeriesListPlot(series, n=NFILES)
plot.versus(field="ts", label="time [h]", norm=3600)
plot.add_series([SeriesListDesc(topic="/sensors/env/temperature", field="temp", label="T [Celsius]", legend="", color="gray", subplot=0),
                 SeriesListDesc(topic="/sensors/env/pressure", field="pressure", label="p [Pa]", legend="", color="gray", subplot=1),
                 SeriesListDesc(topic="/sensors/env/humidity", field="humidity", label="H [%]", legend="", color="gray", subplot=2),
                 SeriesListDesc(topic="/sensors/precipitation", field="precipitation", label="precipitation [AU]", legend="", color="gray", subplot=3),
                 SeriesListDesc(topic="/dbaserh/listmode", field="rate", label="count rate [cnts/s]", legend="", color="gray", subplot=4)])
plot.show()

plot = SeriesHistPlot(histograms, n=NFILES)
plot.binning(field="channels", label="channel [ADC]")
plot.add_series([SeriesHistDesc(topic="uncalibrated", label="entries", legend="uncalibrated", color="gray", subplot=0)])
plot.show()
