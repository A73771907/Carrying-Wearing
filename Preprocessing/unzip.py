import glob
import os, shutil
import zipfile
from tqdm import tqdm
import config

if os.path.exists(config.raw_dir):
    shutil.rmtree(config.raw_dir)
os.makedirs(config.raw_dir)

for name in tqdm(sorted(glob.glob(os.path.join(config.zip_dir, "*.zip")))):
    with zipfile.ZipFile(name, 'r') as zip_ref:
        file = os.path.splitext(os.path.basename(name))[0]
        zip_ref.extractall(os.path.join(config.raw_dir, file))