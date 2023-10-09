# Carrying and Wearing
## Understanding Missing Data in Step-count Self-Tracking Using Smartphones and Wearables in the Wild

### Data

Two versions of data are included: Minute, and Bout.
- Minute include minute level step count data, and wearable steps and phone steps of each minute are saved.
- Bout include aggregated step count data of consecutive movements (i.e., walking bouts) considering same set of tracking devices.

Moreover, device profile and demographics for all participant can be found on device_type.csv and demographics from meta.csv.

### Preprocessing

All the codes used to make above preprocessed data are included in the Preprocess folder.
- unzip.py: It was used for unzipping the participants' data which is recieved as .zip format.
- cleaning.py: Cleansing Process, which was described in Section 4.1.
- aggregate.py: Aggregating Data into Walking Bouts, which was described in Section 4.2.

### Experiments

All the codes for analysis, including supplymentary materials, is given as a single notebook, Experiment.ipynb.