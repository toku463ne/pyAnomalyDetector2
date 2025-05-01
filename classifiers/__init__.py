import numpy as np
import pandas as pd
from itertools import combinations
import logging

def jaccard_distance(a: pd.Series, b: pd.Series) -> float:
    """Compute Jaccard distance between two binary pd.Series"""
    intersection = ((a == 1) & (b == 1)).sum()
    union = ((a == 1) | (b == 1)).sum()
    return 1.0 if union == 0 else 1 - intersection / union

def correlation_distance(a: pd.Series, b: pd.Series) -> float:
    """Compute 1 - Pearson correlation between two time series"""
    astd = a.std()
    bstd = b.std()
    if astd == 0 or bstd == 0:
        return 1.0
    return 1 - abs(a.corr(b))


def compute_anomaly_indicators(charts: dict, charts_stats: dict, z_thresh: float = 2.0) -> dict:
    """Returns {itemid: binary anomaly indicator (0/1 Series)}"""
    indicators = {}
    for itemid, series in charts.items():
        if itemid not in charts_stats:
            continue
        z_mean = charts_stats[itemid]['mean']
        z_std = charts_stats[itemid]['std']
        if z_std == 0:
            z = pd.Series(0, index=series.index)
        else:
            z = (series - z_mean) / z_std
        indicators[itemid] = (z.abs() > z_thresh).astype(int)
    return indicators

def compute_jaccard_distance_matrix(charts: dict, charts_stats: dict, 
                                    sigma: float = 2.0) -> pd.DataFrame:
    itemids = list(charts.keys())
    N = len(itemids)

    # Step 1: Precompute anomaly indicators
    indicators = compute_anomaly_indicators(charts, charts_stats, z_thresh=sigma)

    # Step 2: Initialize distance matrix
    dist_matrix = np.zeros((N, N))

    for i, j in combinations(range(N), 2):
        id_i, id_j = itemids[i], itemids[j]
        a_i, a_j = indicators[id_i], indicators[id_j]

        d_shape = jaccard_distance(a_i, a_j)
        dist_matrix[i, j] = dist_matrix[j, i] = d_shape
        #logging.info(f"jaccard_distance {id_i} {id_j} : {d_shape}")

    return pd.DataFrame(dist_matrix, index=itemids, columns=itemids)

def compute_correlation_distance_matrix(charts: dict) -> pd.DataFrame:
    itemids = list(charts.keys())
    N = len(itemids)

    # Step 1: Initialize distance matrix
    dist_matrix = np.zeros((N, N))

    for i, j in combinations(range(N), 2):
        id_i, id_j = itemids[i], itemids[j]
        s_i, s_j = charts[id_i], charts[id_j]

        d_shape = correlation_distance(s_i, s_j)
        dist_matrix[i, j] = dist_matrix[j, i] = d_shape
        #logging.info(f"correlation_distance {id_i} {id_j} : {d_shape}")

    return pd.DataFrame(dist_matrix, index=itemids, columns=itemids)
