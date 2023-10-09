import os
from glob import glob
import shutil

import numpy as np
import pandas as pd
import datetime as dt

from tqdm import tqdm

root = os.path.join("Data")
minute_dir = os.path.join(root,"Minute")
bout_dir = os.path.join(root,"Bout")


MINUTE = 60 * 1000
DEVICE_TYPE = {0: 'NONE', 1: 'PHONE', 2: 'WEARABLE', 3: 'BOTH'}

for file in tqdm(sorted(glob(os.path.join(minute_dir, "*.csv")))):
    uid = os.path.splitext(os.path.basename(file))[0]
    df = pd.read_csv(file, index_col=None, header=0)

    # calculate device type and time diff btwn consecutive row
    # It wil be used to aggregate consecutive rows.
    wearable = np.zeros(df.shape[0])
    wearable[df["WEARABLE_STEP"].values.nonzero()] = 1

    phone = np.zeros(df.shape[0])
    phone[df["PHONE_STEP"].values.nonzero()] = 1

    device_type = phone + wearable * 2

    type_change = np.array([0, *(np.diff(device_type)!=0)])
    time_jump = np.array([0, *(df["START"].diff()[1:]//MINUTE - 1)])

    bout_idx = np.cumsum(type_change + time_jump)

    df["TYPE"] = device_type
    df["BOUT_IDX"] = bout_idx

    agg = df.groupby("BOUT_IDX").agg(
        WEARABLE_STEP=('WEARABLE_STEP', 'sum'),
        PHONE_STEP=('PHONE_STEP', 'sum'),
        START=('START', 'first'),
        UTC = ('UTC', 'first'),
        END=('START', 'last'),
        TYPE=('TYPE', 'first')
    )
    agg["END"] = agg["END"].values + MINUTE
    agg.replace({"TYPE": DEVICE_TYPE}, inplace= True)

    agg["DURATION"] = (agg["END"] - agg["START"]) //MINUTE
    # Average if Both Type else just sum
    agg["AVERAGE_STEP"] = (agg["WEARABLE_STEP"] + agg["PHONE_STEP"])/((agg["TYPE"] == 'BOTH') + 1)

    agg["READABLE_START"] = pd.to_datetime(agg["START"], unit = "ms") + pd.to_timedelta(agg["UTC"], unit = "hour")
    agg["READABLE_END"] = pd.to_datetime(agg["END"], unit = "ms") + pd.to_timedelta(agg["UTC"], unit = "hour")

    # Resort the columns as desired 
    agg[['START', 'END', 'UTC', 'READABLE_START', 'READABLE_END',
                "PHONE_STEP", "WEARABLE_STEP", "AVERAGE_STEP",
                "DURATION", "TYPE"]].to_csv(os.path.join(bout_dir, f"{uid}.csv"), index=False)