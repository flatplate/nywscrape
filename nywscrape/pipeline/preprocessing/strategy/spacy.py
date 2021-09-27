import logging
from typing import List

import spacy

class SpacyDocumentProcessingStrategy:
    def __init__(self, model="en_core_web_sm", max_text_length=10000):
        self.input = "raw"
        self.output = "spacy_document"
        self.nlp = spacy.load(model)
        self.max_text_length = max_text_length

    def process(self, document: str, document_metadata: dict):
        """
        This function turns raw spacy objects, that can be used for
        preprocessing or sentence splitting down the line. Batching
        documents works better for this processor.
        :param document: Single document to process as a text object
        :return:         Spacy document object for the given document
        """
        return [(self.nlp(document[:self.max_text_length]), document_metadata)]

    def process_batch(self, documents: List[str], document_metadata: List[dict]):
        """
        This function returns raw spacy objects, and processes the
        documents in batches. This is more performant than processing
        the documents one by one
        :param documents:           The batch of documents to process
        :param document_metadata:   Metadata about the documents
        :return:                    List of spacy documents
        """
        batch_size = 200
        results = []
        for i in range(0, len(documents), batch_size):
            logging.info("Spacy processing batch {}".format(i))
            batch = documents[i:i+batch_size]
            results.extend(self.nlp.pipe([document[:self.max_text_length] for document in batch], batch_size=batch_size))
        return zip(results, document_metadata)