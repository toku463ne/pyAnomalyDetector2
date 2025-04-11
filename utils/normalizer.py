

import numpy as np # noqa
import pandas as pd # noqa
from typing import List, Tuple, Dict

""" get_base_clocks:
Generates a list of base clocks based on the given start and end epochs and the unit seconds.
"""
def get_base_clocks(startep: int, endep: int, unitsecs: int) -> List[int]:
    # Adjust startep and endep to be earlier epochs that meet epoch % unitsecs == 0
    adjusted_startep = int(startep - (startep % unitsecs))
    adjusted_endep = int(endep - (endep % unitsecs))
    return list(range(adjusted_startep, adjusted_endep + int(unitsecs), int(unitsecs)))


""" fit_to_base_clocks:
Adjusts the given values to fit the base clocks by interpolating or averaging the values.

This function takes three lists: base_clocks, clocks, and values. It adjusts the values to fit the base_clocks
by either directly assigning the values if the lengths match, or by interpolating/averaging the values if the lengths differ.

Args:
    base_clocks (list[int]): The list of base clock timestamps to fit the values to.
    clocks (list[int]): The list of clock timestamps corresponding to the given values.
    values (list[float]): The list of values to be adjusted to fit the base clocks.

Returns:
    list[float]: A list of values adjusted to fit the base clocks.
"""
def fit_to_base_clocks(base_clocks: List[int], clocks: List[int], values: List[float]) -> List[float]:
    if len(clocks) == len(base_clocks):
        return values
    else:
        new_values: np.ndarray = np.zeros(len(base_clocks))  
        i: int = 0
        j: int = 0
        sum_buffer: float = 0.0
        cnt_buffer: int = 0
        len_base_clocks: int = len(base_clocks)
        len_data: int = len(clocks)

        while i < len_base_clocks and j < len_data:
            if clocks[j] > base_clocks[i]:
                new_values[i] = values[j]
                i += 1
            elif clocks[j] == base_clocks[i]:
                if cnt_buffer == 0:
                    new_values[i] = values[j]
                else:
                    sum_buffer += values[j]
                    cnt_buffer += 1
                    new_values[i] = sum_buffer / cnt_buffer
                    sum_buffer = 0
                    cnt_buffer = 0
                i += 1
                j += 1
            else:
                sum_buffer += values[j]
                cnt_buffer += 1
                j += 1

        if cnt_buffer > 0:
            new_values[i:] = sum_buffer / cnt_buffer

        if i < len_base_clocks:
            new_values[i:] = [values[-1]] * (len(base_clocks) - i)

        if j < len_data:
            new_values[-1] = (new_values[-1] + np.mean(values[j:])) / 2.0

        return new_values.tolist()

def normalize_metric_df(data: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the metric data frame by scaling the values to a range of 0 to 1.

    Args:
        data (pd.DataFrame): The input data frame containing metric values.(itemid, clock, value)
        The 'value' column will be normalized.
        The 'itemid' column is used to group the data for normalization.
        The 'clock' column is used to identify the time of the metric.
    Returns:
        pd.DataFrame: The normalized data frame.
    """
    # Normalize the values in the DataFrame
    data['value'] = data.groupby('itemid')['value'].transform(lambda x: (x - x.min()) / (x.max() - x.min()))

    # fill NaN values with 0
    data.fillna({"value": 0}, inplace=True)
    return data

def df2charts(df: pd.DataFrame, 
              itemIds: List[int], unitsecs: int =600) -> Tuple[Dict[int, pd.Series], List[int]]:
    startep = df['clock'].min()
    endep = df['clock'].max()
    base_clocks = get_base_clocks(startep, endep, unitsecs)
    # sort the base clocks
    base_clocks.sort()
    charts = {}
    for itemId in itemIds:
        #charts[itemId] = history_df[history_df['itemid'] == itemId]['value'].reset_index(drop=True)
        clocks = df[df['itemid'] == itemId]['clock'].tolist()
        values = df[df['itemid'] == itemId]['value'].tolist()
        if len(values) > 0:
            values = fit_to_base_clocks(base_clocks, clocks, values)
            charts[itemId] = pd.Series(values)
    return charts, base_clocks

def get_chart_stats(df: pd.DataFrame, itemIds: List[int]) -> Dict[int, Dict[str, float]]:
    stats = {}
    for itemId in itemIds:
        #stats[itemId] = history_df[history_df['itemid'] == itemId]['value'].reset_index(drop=True)
        values = df[df['itemid'] == itemId]['value'].tolist()
        if len(values) > 0:
            stats[itemId] = {
                'min': min(values),
                'max': max(values),
                'mean': np.mean(values),
                'std': np.std(values)
            }
    return stats
