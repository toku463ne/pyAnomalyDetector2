from datetime import datetime
import numpy as np
import os

# converts string to epoch time
def str2epoch(datestr: str, format: str) -> int:
    dt = datetime.strptime(datestr, format)
    epoch_time = int(dt.timestamp())
    return epoch_time

def ensure_dir(path):
    """Ensure that the given directory exists. Create it if it does not exist."""
    os.makedirs(path, exist_ok=True)

def square_sum(x):
    return np.sum(np.square(x))