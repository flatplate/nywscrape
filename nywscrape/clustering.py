# This is the pipeline for the whole clustering side of things
# This is implemented here to be cleaned up later and used with
# other cooler stuff like the Pipeline class.
from typing import List, Tuple, Any

import hdbscan
import numpy as np
from sklearn.cluster import AgglomerativeClustering


def cluster_hdbscan(vectors: List[Tuple[int, Any]]):
    clustering = hdbscan.HDBSCAN(min_samples=1,
                                 metric='euclidean',
                                 min_cluster_size=2,
                                 cluster_selection_method='leaf',
                                 cluster_selection_epsilon=0.25).fit([v / np.linalg.norm(v) for _, v in vectors])
    # clustering = DBSCAN(min_samples=1,
    #                     metric='euclidean',
    #                     eps=0.6).fit([v / np.linalg.norm(v) for _, v in vectors])
    return list(zip(clustering.labels_, [k for k, _ in vectors]))


def cluster_hac(vectors: List[Tuple[int, Any]]):
    clustering = AgglomerativeClustering(distance_threshold=0.37,
                                         affinity='cosine',
                                         n_clusters=None,
                                         linkage='complete').fit([v for _, v in vectors])
    return list(zip(clustering.labels_, [k for k, _ in vectors]))
