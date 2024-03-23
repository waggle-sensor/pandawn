from rosbag import Bag
import cv2
from cv_bridge import CvBridge
import numpy as np
import matplotlib.pyplot as plt
import argparse
import os
import yaml
from collections import namedtuple
from datetime import datetime

BagInfo = namedtuple("BagInfo", "beg end duration")
def bag_info(bag):
    info_dict = yaml.safe_load(bag._get_yaml_info())
    beg_ts = info_dict['start']
    end_ts = info_dict['end']
    beg = datetime.fromtimestamp(beg_ts).strftime("%m/%d/%Y, %H:%M:%S")
    end = datetime.fromtimestamp(end_ts).strftime("%m/%d/%Y, %H:%M:%S")
    duration = (end_ts - beg_ts) / 3600
    return BagInfo(beg=beg, end=end, duration=duration)

parser = argparse.ArgumentParser()
parser.add_argument('--files', nargs='+', type=str)
args = parser.parse_args()

cvbr = CvBridge()

for file in args.files:
    with Bag(file, 'r') as bag:
        print("opening file:", file)
        info = bag_info(bag)
        print(f"begin: {info.beg}, end: {info.end}, dt: {info.duration}h")
        filename = os.path.basename(file).split('/')[-1]
        dir = os.path.join(os.getcwd(), filename)
        if not os.path.exists(dir):
            os.makedirs(dir)
        mean_dt = 0.
        n_frames = bag.get_message_count("/sensors/camera/image_color")
        msgs = bag.read_messages(topics="/sensors/camera/image_color")
        msg = next(msgs)
        first_timestamp = msg.message.header.stamp.secs + msg.message.header.stamp.nsecs * 1E-9
        prev_timestamp = first_timestamp
        for msg in msgs: 
            timestamp_secs = msg.message.header.stamp.secs
            timestamp_nsecs = msg.message.header.stamp.nsecs
            timestamp = msg.message.header.stamp.secs + msg.message.header.stamp.nsecs * 1E-9
            data = cvbr.compressed_imgmsg_to_cv2(msg.message)
            dt = timestamp - prev_timestamp
            mean_dt += dt
            prev_timestamp = timestamp
            timestamp_str = str(timestamp_secs) + str(timestamp_nsecs).zfill(9)
            #plt.imshow(data)
            #plt.suptitle("ts: " + timestamp_str)
            #plt.savefig(dir+'/'+timestamp_str+".png")
            data = cv2.cvtColor(data, cv2.COLOR_RGB2BGR) 
            cv2.imwrite(dir+'/'+timestamp_str+".png", data)
        recording_time = timestamp - first_timestamp
        mean_dt /= n_frames
        mean_rate = 1. / mean_dt
        print(f"recording time is {recording_time}s, at mean rate: {mean_rate}Hz")
        
