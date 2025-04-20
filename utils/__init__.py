from datetime import datetime
import numpy as np
import os
from typing import Tuple

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

def get_float_format(a: np.ndarray, mask_len: int) -> int:
    """
    Put mask on most changing digits with mask_len digits
    :param a: numpy array
    :param mask_len: number of digits to mask
    :return: precision for float format

    Example:
    when mask_len = 4
    if max=0.00112345 and min=0.001  then data_range=0.00012345 then mask=0.001111 format=.7g (floating start=3 + mask_len=4)
    if max=1.1234     and min=1      then data_range=0.1234     then mask=0.1111 format=.5g (digit=1 + changed floating start=0 + mask_len=4)
    if max=111234.56  and min=110000 then data_range=1234       then mask=111000 format=.6g (not changed digit=2 + mask_len=4)
    """
    # Get the maximum and minimum values of the array
    max_val = np.max(a)
    min_val = np.min(a)

    # Calculate the range of the array
    data_range = max_val - min_val

    max_digit = int(np.floor(np.log10(max_val))) + 1
    if data_range == 0:
        digits = 0
    else:
        digits = int(np.floor(np.log10(data_range))) + 1
    if digits == 0:
        return max_digit + mask_len
    elif digits < 0:
        return max_digit + mask_len
    else:
        return max_digit + mask_len
    
    

