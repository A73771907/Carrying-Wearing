import datetime as dt
import numpy as np

def calcAge(birth:str, collected: str):
    birth_date = dt.datetime.strptime(birth,"%Y%m%d")
    collected_date = dt.datetime.strptime(collected,"%Y%m%d%H%M")
    age = collected_date.year - birth_date.year - ((collected_date.month, collected_date.day) < (birth_date.month, birth_date.day))
    return age


def MAD(data, threshold=3):
    # Calculate the median of the data
    median = np.median(data)
    # Calculate the MAD of the data
    mad = np.median(np.abs(data - median))

    return np.abs(data- median) > threshold * mad


colors = {
    "STEP": "#000",
    "BOTH": "#2ca02c",
    "PHONE": "#1f77b4",
    "WEARABLE": "#ff7f0e"
}
def show_signifcance(pval):
    if pval < 1e-3:
        return "***"
    elif pval < 1e-2:
        return "**"
    elif pval < 5e-2:
        return "*"
    else:
        return ""
    
def calcIntensity(spm):
    if spm < 100:
        return "MPA"
    elif spm < 133:
        return "MVPA"
    else:
        return "VPA"