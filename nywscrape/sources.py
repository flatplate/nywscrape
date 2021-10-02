import logging
import time

from nywscrape.clustering import cluster_hdbscan as cluster
from nywscrape.database.database import connect, get_raw_articles, get_document_vectors, write_clusters, \
    insert_sentence_similarities
from nywscrape.datapusher import load_data
from nywscrape.model.RawArticle import RawArticle
from nywscrape.pipeline.core import SimpleQueuePipeline
from nywscrape.pipeline.preprocessing.strategy.minimal import MinimalCleaningStrategy
from nywscrape.pipeline.sentence.paragraph_splitter import ParagraphSplittingStrategy
from nywscrape.pipeline.storage.postgres import PostgresStorageSink
from nywscrape.pipeline.vectorization import SentenceTransformerVectorizationStrategy
from nywscrape.sentsim import sentence_similarity
import torch
import schedule

logging.basicConfig(level="INFO")

device = torch.device("cuda:0")
# torch.set_num_threads(10)


def main():
    with connect() as conn:
        pipeline = SimpleQueuePipeline(batch_mode=True)

        cleaning = MinimalCleaningStrategy()
        sentence = ParagraphSplittingStrategy()
        sentence_cleaning = MinimalCleaningStrategy(input_='sentence', output='cleaned_sentence')
        sentence_vectorizer = SentenceTransformerVectorizationStrategy(input_='cleaned_sentence',
                                                                       output="sentence_vector", device=device)
        document_vectorizer = SentenceTransformerVectorizationStrategy(input_='cleaned_document',
                                                                       output="document_vector",
                                                                       model_name=r"C:\Users\ural_\Projects\news_comparison\all-mpnet-base-v2-finetuned",
                                                                       device=device)
        sentence_vector_postgres_sink = PostgresStorageSink(conn=conn, input_='sentence_vector',
                                                            tablename='sentence_vector',
                                                            field_metadata_mapping={
                                                                "doc_id": ["doc", "doc", "id"],
                                                                "sentence_id": ["sentence_id"],
                                                                "original": ["original"]
                                                            })
        document_vector_postgres_sink = PostgresStorageSink(conn=conn, input_='document_vector',
                                                            tablename='document_vector',
                                                            field_metadata_mapping={
                                                                "doc_id": ["doc", "id"],
                                                            })
        pipeline.add_element(cleaning)
        pipeline.add_element(sentence)
        pipeline.add_element(sentence_vectorizer)
        pipeline.add_element(document_vectorizer)
        pipeline.add_element(sentence_cleaning)
        pipeline.add_element(sentence_vector_postgres_sink)
        pipeline.add_element(document_vector_postgres_sink)

        article: RawArticle
        articles = [article for article in get_raw_articles(conn)]
        total_articles = len(articles)

        # if total_articles < 10:
        #     return

        logging.info("Total articles to parse: {}".format(total_articles))
        start = time.time()
        batch_size = 200
        for i in range(0, len(articles), batch_size):
            batch = articles[i: i+batch_size]
            pipeline.process_batch('raw', list(zip([article.maintext for article in batch], [{"doc": doc} for doc in batch])))
            pipeline.run_all()
        logging.info("Processed {} articles in {} seconds".format(total_articles, time.time() - start))
        start = time.time()

        vectors = get_document_vectors(conn)
        logging.info("Clustering {} articles".format(len(vectors)))
        clusters = cluster(vectors)
        logging.info("Clustered {} articles into {} clusters in {} seconds".format(len(vectors), len(set([c[0] for c in clusters])), time.time() - start))
        logging.info("Writing clusters to the database")
        clustering_id = write_clusters(conn, clusters)
        logging.info("Wrote clusters to the database")
        logging.info("Calculating and inserting sentence similarities")
        insert_sentence_similarities(conn, [d for d in sentence_similarity(clusters, clustering_id, conn)], clustering_id)
        logging.info("Inserted sentence similarities")
        logging.info("Pushing the data")
        load_data()
        logging.info("Finished pushing data")



if __name__ == '__main__':
    main()
    schedule.every(15).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
