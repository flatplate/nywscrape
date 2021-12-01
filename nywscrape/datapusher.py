import logging

import psycopg2
from psycopg2.extras import execute_values

# For local testing, automatically updates the database
database_config_nywscrape = {
    "dbname": "nywscrape",
    "user": "nywscrape",
    "password": "nywscrapepassword",
    "host": "localhost",
    "port": 5433
}
real_db_url = 'postgresql://postgres:JLhKkxA70N6zg9uoMyge@168.119.224.47:5432/postgres'


publishers = {
    "theblaze": 1,
    "cnn": 2,
    "foxnews": 3,
    "nytimes": 4,
    "npr": 5,
    "washingtonpost": 6,
    "huffpost": 7,
    "bbc": 8,
    "politico": 9,
    "abcnews": 10,
    "breitbart": 11,
    "apnews": 12,
    "vox": 13,
    "washingtontimes": 14,
    "usatoday": 15
}

def load_data():
    print("LOADING DATA")
    """
    This is for loading the data from the analysis/scraping
    database.
    """
    try:
        print("Connecting to the nywscrape database")
        conn = psycopg2.connect(**database_config_nywscrape)
    except Exception as e:
        logging.error("Could not connect to nywscrape database.\n{}".format(str(e)))
        return
    print("Connected to the nywscrape database")
    sql = """
        SELECT 
            cv.id, 
            cv.url, 
            cv.title, 
            cv.description, 
            cv.authors, 
            cv.image_url, 
            cv.source_domain,
            cv.date_publish,
            dc.cluster_id
        FROM clusters c 
            INNER JOIN document_cluster dc ON c.id = dc.clustering
            INNER JOIN currentversions cv ON cv.id = dc.doc_id
        WHERE c.id = (SELECT max(id) FROM clusters)
        ORDER BY cv.date_publish DESC
    """
    cur = conn.cursor()
    cur.execute(sql)
    articles = cur.fetchall()
    print("Got articles")

    sentence_sql = """
        SELECT 
            sv.doc_id,
            sv.sentence_id,
            sv.original
        FROM clusters c 
            INNER JOIN document_cluster dc ON c.id = dc.clustering
            INNER JOIN currentversions cv ON cv.id = dc.doc_id
            INNER JOIN sentence_vector sv ON sv.doc_id = cv.id
        WHERE c.id = (SELECT max(id) FROM clusters)
    """
    cur.execute(sentence_sql)
    sentences = cur.fetchall()
    print("Got sentences")

    similarity_sql = """
        SELECT 
            ss.document_1,
            ss.sentence_1,
            ss.document_2,
            ss.sentence_2,
            ss.similarity
        FROM clusters c 
            INNER JOIN document_cluster dc ON c.id = dc.clustering
            INNER JOIN currentversions cv ON cv.id = dc.doc_id
            INNER JOIN sentence_vector sv ON sv.doc_id = cv.id
            INNER JOIN sentence_sim ss ON ss.document_1 = sv.doc_id AND ss.sentence_1 = sv.sentence_id and ss.clustering = c.id
        WHERE c.id = (SELECT max(id) FROM clusters) 
    """
    cur.execute(similarity_sql)
    similarities = cur.fetchall()
    print("Got similarities")

    print("Creating staging schema")
    conn_realdb = psycopg2.connect(real_db_url)
    cur_realdb = conn_realdb.cursor()
    cur_realdb.execute("CREATE SCHEMA staging")
    cur_realdb.execute("CREATE TABLE staging.articles_article (LIKE public.articles_article) ")
    cur_realdb.execute("CREATE TABLE staging.publisher (LIKE public.publisher INCLUDING ALL)")
    cur_realdb.execute("INSERT INTO staging.publisher (SELECT * FROM public.publisher)")
    cur_realdb.execute("CREATE TABLE staging.feedback_device (LIKE public.feedback_device INCLUDING ALL)")
    cur_realdb.execute("INSERT INTO staging.feedback_device (SELECT * FROM public.feedback_device)")
    cur_realdb.execute("CREATE TABLE staging.feedback_paragraph (LIKE public.feedback_paragraph INCLUDING ALL)")
    cur_realdb.execute("INSERT INTO staging.feedback_paragraph (SELECT * FROM public.feedback_paragraph)")
    cur_realdb.execute("CREATE TABLE staging.articles_sentence (LIKE public.articles_sentence)")
    cur_realdb.execute("CREATE TABLE staging.clusters_cluster (LIKE public.clusters_cluster)")
    cur_realdb.execute("CREATE TABLE staging.clusters_article (LIKE public.clusters_article)")
    cur_realdb.execute("CREATE TABLE staging.sentences_sentence (LIKE public.sentences_sentence)")
    cur_realdb.execute("CREATE TABLE staging.sentences_sentence_similarity "
                       "(LIKE public.sentences_sentence_similarity)")
    print("Created staging schema")
    # Transform cluster ids to unique ones
    current_cluster_id = max([article[-1] for article in articles]) + 1
    articles = [list(article) for article in articles]
    for article in articles:
        article[6] = get_publisher_id(article[1])
        if article[-1] == -1:
            article[-1] = current_cluster_id
            current_cluster_id += 1

    article_insert_sql = """INSERT INTO staging.articles_article 
                            (id, url, title, description, authors, image, publisher, publish_time)
                            VALUES %s"""
    execute_values(cur_realdb, article_insert_sql, [article[:-1] for article in articles])
    print("Inserted articles")
    sentence_insert_sql = """INSERT INTO staging.articles_sentence (article_id, id, text) VALUES %s"""
    execute_values(cur_realdb, sentence_insert_sql, sentences)

    print("Inserted sentences")
    # Select representative articles of clusters
    added_clusters = set()
    cluster_tuples = []
    for article in articles:
        cluster_id = article[-1]
        if cluster_id not in added_clusters:
            added_clusters.add(cluster_id)
            cluster_tuples.append((cluster_id, article[2], article[3], article[5], article[7], 1.0))

    cluster_insert_sql = """INSERT INTO staging.clusters_cluster (id, title, description, image, published, importance)
                            VALUES %s"""
    execute_values(cur_realdb, cluster_insert_sql, cluster_tuples)
    print("Inserted clusters")

    cluster_article_insert_sql = """INSERT INTO staging.clusters_article (id, cluster_id) VALUES %s"""
    execute_values(cur_realdb, cluster_article_insert_sql, [(article[0], article[-1]) for article in articles])
    print("Inserted cluster articles")

    sentence_insert_sql = """INSERT INTO staging.sentences_sentence (document_id, id, text) VALUES %s"""
    execute_values(cur_realdb, sentence_insert_sql, sentences)
    print("Inserted sentences")

    similarity_insert_sql = """INSERT INTO staging.sentences_sentence_similarity (
                                   first_sentence_document_id, 
                                   first_sentence_sentence_id,
                                   second_sentence_document_id,
                                   second_sentence_sentence_id,
                                   similarity
                               ) VALUES %s"""
    execute_values(cur_realdb, similarity_insert_sql, similarities)
    print("Inserted similarities")

    cur_realdb.execute("DROP SCHEMA PUBLIC CASCADE")
    cur_realdb.execute("ALTER SCHEMA staging RENAME TO public")
    conn_realdb.commit()
    print("Commited changes")

def get_publisher_id(url):
    url_main_part = url.split("/")[2]
    for key, value in publishers.items():
        if key in url_main_part:
            return value
    return None
