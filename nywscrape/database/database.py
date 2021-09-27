import logging
import pickle
from typing import Generator, Optional, Type, List, Tuple

import psycopg2
from psycopg2._psycopg import AsIs
from psycopg2.extensions import connection, cursor
from datetime import datetime, timedelta

from psycopg2.extras import DictCursor

from nywscrape.model.RawArticle import RawArticle

database_config = {
    "dbname": "nywscrape",
    "user": "nywscrape",
    "password": "nywscrapepassword",  # TODO Read this from config
    "host": "localhost",
    "port": 5433
}


def connect(dbconfig: dict = None) -> Optional[connection]:
    if dbconfig is None:
        dbconfig = database_config

    try:
        return psycopg2.connect(**dbconfig)
    except Exception as e:
        logging.error("Could not connect to database", e)
        return None


def get_raw_articles(conn: connection, after: datetime = None) -> Generator[Type[RawArticle], None, None]:
    cur: cursor = conn.cursor(cursor_factory=DictCursor)
    if after is None:
        after = datetime.now() - timedelta(days=3)

    cur.execute("""SELECT distinct on (c.id)
                        id, 
                        date_publish, 
                        source_domain, 
                        title, 
                        maintext, 
                        description, 
                        image_url, 
                        authors, 
                        language 
                    FROM currentversions c
                        LEFT OUTER JOIN document_vector dv ON dv.doc_id = c.id
                        LEFT OUTER JOIN sentence_vector sv ON sv.doc_id = c.id
                    WHERE c.date_publish > %s AND c.date_publish < now()
                        AND c.language = 'en'
                        AND (dv.doc_id IS NULL OR sv.doc_id IS NULL)""",
                (after,))

    for res in cur:
        yield RawArticle(**res)


def delete_duplicates(conn: connection) -> None:
    """
    This is a method to remove duplicate articles. It probably doesn't find all the duplicates.
    It removes the articles who have > 90% levenshtein similarity with their neighbor when ordered
    by article text.
    :param conn: psycopg2 connection to the database
    :return:     None
    """

    sql = """with dupe_score as (
        select  
            1 - levenshtein(left(maintext, 255), left(lag(maintext) over (order by left(maintext, 255)), 255))::float / 255 as duplication_score, 
            *,
            lag(maintext) over (order by left(maintext, 255)) as other from currentversions c
        order by left(maintext, 255)
        limit 1000000
        )
        , dupes as (select id from dupe_score where duplication_score > 0.9)
        delete from currentversions c2 where id in (select * from dupes)"""
    cur: cursor = conn.cursor()
    cur.execute(sql)

def get_document_vectors(conn: connection, after: datetime = None):
    # TODO Test the return type
    if after is None:
        after = datetime.now() - timedelta(days=3)

    sql = """SELECT dv.doc_id, dv.doc FROM document_vector dv
             INNER JOIN currentversions cv ON dv.doc_id = cv.id
             WHERE cv.date_publish > %s AND cv.date_publish < now()"""
    cur = conn.cursor()
    cur.execute(sql, (after,))

    vectors = cur.fetchall()
    return [(k, pickle.loads(v)) for k, v in vectors]

def write_clusters(conn: connection, clusters: List[Tuple[int, int]]):
    cur = conn.cursor()
    sql = "INSERT INTO clusters DEFAULT VALUES RETURNING id"
    cur.execute(sql)
    clustering_id = cur.fetchone()[0]
    sql = "INSERT INTO document_cluster (clustering, doc_id, cluster_id) VALUES (%s, %s, %s)"
    cur.executemany(sql, [(clustering_id, int(doc_id), int(cluster_id)) for cluster_id, doc_id in clusters])
    return clustering_id

def get_cluster_sentences(conn: connection, clustering_id: int, cluster_id: int):
    if clustering_id is None:
        clustering_id = AsIs("(SELECT max(id) FROM clusters)")
    cur = conn.cursor()
    sql = """SELECT sv.doc_id, sv.sentence_id, sv.doc FROM document_cluster dc
             INNER JOIN sentence_vector sv ON sv.doc_id = dc.doc_id
             WHERE dc.clustering = %s AND dc.cluster_id = %s
             ORDER BY sv.doc_id, sv.sentence_id
             """
    cur.execute(sql, (clustering_id, int(cluster_id)))
    return [(doc_id, sent_id, pickle.loads(doc)) for doc_id, sent_id, doc in cur.fetchall()]

def get_sentence_vectors(conn: connection, clustering_id: int):
    if clustering_id is None:
        clustering_id = AsIs("(SELECT max(id) FROM clusters)")
    cur = conn.cursor()
    sql = """SELECT sv.doc_id, sv.sentence_id, sv.doc FROM document_cluster dc
             INNER JOIN sentence_vector sv ON sv.doc_id = dc.doc_id
             WHERE dc.clustering = %s
             """
    cur.execute(sql, (clustering_id,))
    sents = cur.fetchall()
    logging.info("Fetched sentence vectors")
    return [(doc_id, sent_id, pickle.loads(doc)) for doc_id, sent_id, doc in sents]

def insert_sentence_similarities(conn: connection, sentence_similarities: List[Tuple[int, int, int, int, float]], clustering_id: int):
    cur = conn.cursor()
    sql = """INSERT INTO sentence_sim (clustering, document_1, sentence_1, document_2, sentence_2, similarity)
             VALUES (%s, %s, %s, %s, %s, %s)"""
    cur.executemany(sql, [(clustering_id, int(d1), int(s1), int(d2), int(s2), float(sim)) for d1, s1, d2, s2, sim in sentence_similarities])
    conn.commit()
