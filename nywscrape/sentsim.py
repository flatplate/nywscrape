from nywscrape.database.database import get_cluster_sentences
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict
import numpy as np


def sentence_similarity(clusters, clustering_id, conn):
    skip_cluster_id = set()
    skip_cluster_id.add(-1)

    for cluster_id, _ in clusters:
        if cluster_id in skip_cluster_id:
            continue
        skip_cluster_id.add(cluster_id)
        sents = sorted(get_cluster_sentences(conn, clustering_id, cluster_id))
        doc_ids = {d for d, _, _ in sents}
        similarities = defaultdict(dict)
        for doc_id1 in doc_ids:
            for doc_id2 in doc_ids:
                if doc_id1 != doc_id2:
                    similarities[doc_id1][doc_id2] = cosine_similarity(
                        [v for d, _, v in sents if d == doc_id1],
                        [v for d, _, v in sents if d == doc_id2]
                    )

        for i, (doc_id, sent_id, _) in enumerate(sents):
            # Produce doc_id, sent_id, doc_id, sent_id, similarity
            # For each doc_id that is != current_doc_id
            for other_doc_id in doc_ids:
                if doc_id != other_doc_id:
                    row = similarities[doc_id][other_doc_id][sent_id]
                    other_sent_id = np.argmax(row)
                    sim = row[other_sent_id]
                    yield doc_id, sent_id, other_doc_id, other_sent_id, sim
