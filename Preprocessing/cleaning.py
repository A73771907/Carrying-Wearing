import os
from glob import glob
import shutil

import pandas as pd
import numpy as np

from tqdm import tqdm
import util

UTC_OFFSET = 9
MINUTE = 60 * 1000
DAY = 24 * 60 * MINUTE
DEVICE_GROUP = {360001: 'PHONE', 360003: 'WEARABLE'}

root = os.path.join("Data")
raw_dir = os.path.join(root,"Raws")
minute_dir = os.path.join(root,"Minute")


if os.path.exists(minute_dir):
    shutil.rmtree(minute_dir)
os.makedirs(minute_dir)

user_profiles = []
device_types = []
n_days = []
for uid in tqdm(sorted(os.listdir(raw_dir))):
    profile = {'UID': uid}
    # There are two types of step count csv file name due to difference of OS
    step_count = glob(os.path.join(raw_dir, uid, "**",
                      "com.samsung.shealth.tracker.pedometer_step_count.*.csv"), recursive=True)

    if len(step_count) != 1:
        print(f"{uid}: {len(step_count)} of Step Count CSV is found!")
        continue
    profile['COLLECTED_AT'] = step_count[0].split('.')[-2]

    # get Demographics
    user_profile = glob(os.path.join(raw_dir, uid, "**",
                        "com.samsung.health.user_profile.*.csv"), recursive=True)
    if len(user_profile) != 1:
        print(f"{uid}: {len(user_profile)} of User Profile CSV is found!")
        # It would not considered as invalid data.
    n_cols = pd.read_csv(user_profile[0], skiprows=[
                         0], sep=',', nrows=1).shape[1]
    user_df = pd.read_csv(
        user_profile[0],
        skiprows=[0],
        header=0,
        usecols=range(n_cols),
        index_col=None,
    )

    birth_date = user_df.query("key == 'birth_date'")['text_value']
    if birth_date.shape[0] != 1 or pd.isnull(birth_date.values[0]):
        print(f"{uid}: Can not identify BIRTH_DATE from file")
        profile['BIRTH_DATAE'] = np.nan
        profile['AGE'] = np.nan
    else:
        birth_date = birth_date.values[0]
        profile['BIRTH_DATE'] = birth_date
        profile['AGE'] = util.calcAge(
            profile['BIRTH_DATE'], profile['COLLECTED_AT'])
        if profile['AGE'] < 19:
            print(f"{uid}: Under Age({profile['AGE']}) Participant")
            continue

    gender = user_df.query("key == 'gender'")['text_value']
    if gender.shape[0] != 1 or pd.isnull(gender.values[0]):
        print(f"{uid}: Can not identify GENDER from file")
        profile['GENDER'] = np.nan
    else:
        profile['GENDER'] = gender.values[0]

    # Clean the Step count data
    n_cols = pd.read_csv(step_count[0], skiprows=[
                         0], sep=',', nrows=1).shape[1]
    step_count_df = pd.read_csv(step_count[0],
                                skiprows=[0],
                                index_col=None,
                                header=0,
                                usecols=range(n_cols),
                                encoding="utf-8")

    # remove the package name for each column
    step_count_df.columns = [val.split('.')[-1]
                             for val in step_count_df.columns]
    step_count_df.rename({
        'start_time': 'START',
        'end_time': 'END',
        'count': 'STEP',
        'deviceuuid': 'DEVICE_UUID',
    }, axis=1, inplace=True)
    step_count_df = step_count_df[['START', 'END', 'STEP', 'DEVICE_UUID']]

    # Remove zero-step count row
    step_count_df.query("STEP != 0", inplace = True)

    # Parse the timestamp into UNIX timestamp
    # UTC+0900 for South Korea
    step_count_df['UTC'] = [9] * step_count_df.shape[0]
    try:
        for column in ['START', 'END']:
            step_count_df[column] = pd.to_datetime(step_count_df[column])
            step_count_df[column] = step_count_df[column].astype(int) // 10**6
    except Exception as e:
        print(f"{uid}: Can not intepret start and end column")
        continue

    # Check Duration of each record
    # It can be 59000, 59999, and 60000
    duration = step_count_df["END"] - step_count_df["START"]
    is_one_min = (duration == MINUTE) | (duration == MINUTE - 1) | (duration == MINUTE - 1000)
    if np.sum(is_one_min) != step_count_df.shape[0]:
        print(f"{uid}: {step_count_df.shape[0] - np.sum(is_one_min)} rows have not 1 Minute Duration!( One of {set(duration).difference([MINUTE, MINUTE-1])})")
        continue

    # Remove start and end day of data collection period.
    step_count_df["LOCAL_START"] = pd.to_datetime(step_count_df["START"], unit= 'ms') + pd.to_timedelta(step_count_df["UTC"], unit = "hour")
    step_count_df["LOCAL_END"] = pd.to_datetime(step_count_df["END"], unit= 'ms') + pd.to_timedelta(step_count_df["UTC"], unit = "hour")
    step_count_df.sort_values(by="START", ascending=True, inplace=True)

    start_date = step_count_df['LOCAL_START'].dt.date.values[0]
    end_date = step_count_df['LOCAL_START'].dt.date.values[-1]
    step_count_df.query("LOCAL_START > @start_date and LOCAL_END < @end_date", inplace=True)

    # Get Device Profile
    device_profile = glob(
        os.path.join(raw_dir, uid, "**", "com.samsung.health.device_profile.*.csv"), recursive=True)
    if len(device_profile) != 1:
        print(f"{uid}: {len(device_profile)} of Device Profile CSV is found!")
        continue

    n_cols = pd.read_csv(device_profile[0], skiprows=[
                         0], sep=',', nrows=1).shape[1]
    device_profile_df = pd.read_csv(
        device_profile[0],
        skiprows=[0],
        header=0,
        usecols=range(n_cols),
        index_col=False,
    )
    device_profile_df = device_profile_df[[
        "manufacturer", "device_group", "name", "model", "deviceuuid"]]
    device_profile_df.rename({
        "manufacturer": "MANUFACTURER",
        "device_group": "DEVICE_GROUP",
        "name": "NAME",
        "model": "MODEL",
        "deviceuuid": "DEVICE_UUID"
    }, axis=1, inplace=True)
    device_profile_df.replace({"DEVICE_GROUP": DEVICE_GROUP}, inplace=True)

    # Join the device profile and step count
    df = pd.merge(step_count_df, device_profile_df, on="DEVICE_UUID")
    # Check Manufacturers of step count tracking device.
    devices = df["DEVICE_UUID"].unique()
    device_profile_df.query("DEVICE_UUID in @devices", inplace=True)
    if np.sum([not val.startswith("Samsung") for val in device_profile_df["MANUFACTURER"].values]) != 0:
        print(f"{uid}: Other Manufacturer's Device Detected!")
        continue

    # Check Both types of devices are used at least once in the period
    if len(df["DEVICE_GROUP"].unique()) < 2:
        print(
            f"{uid}: Only Device Type {df['DEVICE_GROUP'].unique()} is detected!")
        continue

    # Record Used Devices here.
    # WEARABLE_MODELS can be find in NAME column and PHONE_MODELS can be find in MODEL column.
    profile["PHONE_MODELS"] = str(list(device_profile_df.query("DEVICE_GROUP == 'PHONE'")["MODEL"].unique()))
    profile["WEARABLE_MODELS"] = str(list(device_profile_df.query("DEVICE_GROUP == 'WEARABLE'")["NAME"].unique()))

    user_profiles.append(profile)
    # Unstack Device group to show step count from phone and wearable together
    df = df[["START", "UTC", "STEP", "DEVICE_GROUP"]]
    df = df.groupby(["START","UTC","DEVICE_GROUP"]).agg(STEP = ("STEP","mean")).reset_index()
    df.set_index(["START", "UTC", "DEVICE_GROUP"], inplace=True)
    df = df.unstack(level = 2)
    df.reset_index(inplace = True)

    # Get Minimum periods.
    MIN_COLLECTION_PERIOD = 31
    local_date = (pd.to_datetime(df["START"], unit="ms") + pd.to_timedelta(df["UTC"], unit = "hours")).dt.date
    collected_dates = np.unique(local_date.values)
    collection_periods = len(collected_dates) 
    if collection_periods < MIN_COLLECTION_PERIOD:
        print(f"{uid}: Data was collected for {collection_periods} days!")
        continue
    x = sorted(collected_dates)[-MIN_COLLECTION_PERIOD:]

    # rename columns
    columns = list(zip(df.columns.get_level_values(level = 1), df.columns.get_level_values(level=0)))
    columns = [a + ("_" if a !="" else "") + b for a, b in columns]
    df.columns = columns
    df.fillna(value = 0, inplace= True)

    df["LOCAL_DATE"] = local_date
    df.query("LOCAL_DATE in @x", inplace = True)
    df.drop("LOCAL_DATE", axis = 1, inplace = True)

    df.to_csv(os.path.join(minute_dir, f"{uid}.csv"), index = False)

pd.DataFrame(user_profiles).to_csv(os.path.join(root, "meta.csv"), index=False)
