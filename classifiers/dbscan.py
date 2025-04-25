from typing import Dict, Tuple, List
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from models.models_set import ModelsSet

from classifiers import *


def classify_charts(conf: Dict, itemIds: List[int], endep: int,
                    ) -> Tuple[Dict[int, int], Dict[int, pd.Series], Dict[int, pd.Series]]:
    charts = {}
    chart_stats = {}
    data_sources = conf['data_sources']
    anomaly_keep_secs = conf.get('anomaly_keep_secs', 3600 * 24)
    startep = endep - anomaly_keep_secs
    for data_source_name in data_sources:
        ms = ModelsSet(data_source_name)
        stats = ms.trends_stats.get_stats_per_itemId(itemIds=itemIds)
        chart_stats.update(stats)
        charts.update(ms.history.get_charts(list(stats.keys()), startep, endep))
        

    if len(charts) > 1:
        dbscan_conf = conf.get('dbscan', {})
        jaccard_eps = dbscan_conf.get('jaccard_eps', 0.1)
        corr_eps = dbscan_conf.get('corr_eps', 0.4)
        min_samples = dbscan_conf.get('min_samples', 2)
        sigma = dbscan_conf.get('sigma', 2.0)
        
        clusters, centroids, chart_info = run_dbscan(charts,
            chart_stats=chart_stats,
            sigma=sigma,
            jaccard_eps=jaccard_eps,
            corr_eps=corr_eps,
            min_samples=min_samples,
        )
    else:
        return {}, {}, {}
    return clusters, centroids, chart_info



def run_dbscan(
    charts: Dict[int, pd.Series],
    chart_stats: Dict[int, pd.Series],
    sigma: float = 2.0,
    jaccard_eps: float = 0.1,
    corr_eps: float = 0.4,
    min_samples: int = 2,
) -> Tuple[Dict[int, int], Dict[int, pd.Series], Dict[int, pd.Series]]:
    """
    Classify charts using DBSCAN with Correlation Distance.

    Parameters:
        charts (dict): Dictionary of itemId to chart data.
        eps (float): Maximum distance between two samples for them to be considered as in the same neighborhood.
        min_samples (int): Minimum number of samples in a neighborhood to form a core point.

    Returns:
        Tuple[Dict[int, int], Dict[int, pd.Series]]: Clusters and centroids.
    """
    distance_matrix = compute_jaccard_distance_matrix(charts, chart_stats, sigma=sigma)
    matrix_size = (distance_matrix.max().max() - distance_matrix.min().min())
    # Ensure the distance matrix values are normalized between 0 and 1
    if matrix_size > 1:
        distance_matrix = (distance_matrix - distance_matrix.min().min()) / matrix_size
    
    # Convert chart data to a NumPy array
    data = np.array([chart.values for chart in charts.values()])

    # Run DBSCAN with precomputed distance matrix
    db = DBSCAN(eps=jaccard_eps, min_samples=min_samples, metric='precomputed').fit(distance_matrix)

    # chart_ids per labels
    db_groups = {}
    chart_ids = list(charts.keys())
    for i, label in enumerate(db.labels_):
        if label not in db_groups:
            db_groups[label] = []
        db_groups[label].append(chart_ids[i])

    clusters = {chart_id: db.labels_[i] for i, chart_id in enumerate(charts.keys())}
    max_cluster_id = max(db_groups.keys())

    # classify each db_group by DBSCAN using correlation distance
    for label, group in db_groups.items():
        # Skip noise points
        if label == -1:
            continue

        # Skip groups with only one chart
        if len(group) < 2:
            continue

        # Calculate the distance matrix for the group
        group_distance_matrix = compute_correlation_distance_matrix({chart_id: charts[chart_id] for chart_id in group})
        matrix_size = (group_distance_matrix.max().max() - group_distance_matrix.min().min())
        # Ensure the distance matrix values are normalized between 0 and 1
        if matrix_size > 1:
            group_distance_matrix = (group_distance_matrix - group_distance_matrix.min().min()) / matrix_size
        
        # Run DBSCAN on the group
        db_group = DBSCAN(eps=corr_eps, min_samples=min_samples, metric='precomputed').fit(group_distance_matrix)
        # Update labels for the group
        for i, chart_id in enumerate(group):
            if db_group.labels_[i] == -1:
                clusters[chart_id] = -1
            else:
                # Assign a new cluster id based on the max_cluster_id
                clusters[chart_id] = max_cluster_id + db_group.labels_[i] + 1
        max_cluster_id = max(clusters.values())
    
    
    # Extract centroids (mean of points in each cluster)
    centroids = {}
    for cluster_id in range(max_cluster_id+1):
        if cluster_id == -1:  # Skip noise points
            continue
        cluster_points = data[np.array(list(clusters.values())) == cluster_id]
        if len(cluster_points) > 0:
            centroids[cluster_id] = np.mean(cluster_points, axis=0)

    return clusters, centroids, charts