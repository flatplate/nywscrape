import logging

from nywscrape.database.database import get_cluster_sentences, get_sentence_vectors
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import numpy as np

last_clustering_id = -1

def sentence_similarity(clusters, clustering_id, conn):
    skip_cluster_id = set()
    skip_cluster_id.add(-1)

    sentence_vector_cache = defaultdict(dict)
    logging.info("Getting sentence vectors")
    for doc_id, sent_id, doc in get_sentence_vectors(conn, clustering_id):
        sentence_vector_cache[doc_id][sent_id] = doc

    logging.info("Transforming the structure of cluster ids")
    real_clusters = defaultdict(list)
    for cluster_id, doc_id in clusters:
        real_clusters[cluster_id].append(doc_id)

    logging.info("Getting the most similar sentences")
    for cluster_id, doc_ids in real_clusters.items():
        if cluster_id in skip_cluster_id:
            continue
        skip_cluster_id.add(cluster_id)
        similarities = defaultdict(dict)
        for doc_id1 in doc_ids:
            for doc_id2 in doc_ids:
                if doc_id1 != doc_id2:
                    similarities[doc_id1][doc_id2] = cosine_similarity(
                        [v for v in sentence_vector_cache[doc_id1].values()],
                        [v for v in sentence_vector_cache[doc_id2].values()]
                    )

        for doc_id in doc_ids:
            for sent_id in sentence_vector_cache[doc_id]:
                # Produce doc_id, sent_id, doc_id, sent_id, similarity
                # For each doc_id that is != current_doc_id
                for other_doc_id in doc_ids:
                    if doc_id != other_doc_id:
                        row = similarities[doc_id][other_doc_id][sent_id]
                        other_sent_id = np.argmax(row)
                        sim = row[other_sent_id]
                        yield doc_id, sent_id, other_doc_id, other_sent_id, sim
