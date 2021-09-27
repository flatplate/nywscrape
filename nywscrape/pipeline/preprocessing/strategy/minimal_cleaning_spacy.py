from typing import List

import spacy
import re


class MinimalSpacyCleaningStrategy:
    def __init__(self, stopwords=None):
        self.input = "spacy_document"
        self.output = "cleaned_document"
        self.stopwords = stopwords

    def process(self, spacy_doc: spacy.language.Doc, document_metadata: dict):
        return [(self._process(spacy_doc), document_metadata)]

    def process_batch(self, spacy_docs: List[spacy.language.Doc], document_metadata: List[dict]):
        return zip([self._process(doc) for doc in spacy_docs], document_metadata)

    def _process(self, spacy_doc: spacy.language.Doc):
        sentence_list = []
        for sentence in spacy_doc.sents:
            token_list = []
            for token in sentence:
                cleaned_token = self._clean_token(token.text)
                if cleaned_token:
                    token_list.append(cleaned_token)
            if token_list:
                sentence_list.append(token_list)
        return ". ".join([" ".join(sentence) for sentence in sentence_list])

    def _clean_token(self, token: str):
        if not token.strip():
            return None

        cleaned_token = re.sub(r"[^a-z0-9]", "", token.lower())

        if cleaned_token == "":
            return None
        return cleaned_token




