import re
from typing import List

from spacy.language import Doc
from spacy.tokens import Span


class SentenceSplittingStrategy:
    def __init__(self):
        self.input = 'spacy_document'
        self.output = 'cleaned_sentence'

    def process(self, document: Doc, metadata: dict):
        """
        This returns a list of sentences and a list of metadata
        since it transforms a single document to multiple
        sentences. TODO ensure that this is compatible
        :param document:  Spacy document
        :param metadata:  Metadata for the document
        :return:          List of sentences and metadata
        """
        processed_sentences = [self._process_sentence(sent) for sent in document.sents]
        sents, startends = list(zip(*processed_sentences))
        return zip(sents, [{**metadata, "start": start, "end": end, "original": document.text[start:end], "sentence_id": i}
                           for i, (start, end) in enumerate(startends)])

    def process_batch(self, documents: List[Doc], metadata: List[dict]):
        temp = [(real_sent, real_meta)
                for document, metadata_ in zip(documents, metadata)
                for real_sent, real_meta in self.process(document, metadata_)]
        return temp

    def _process_sentence(self, sentence: Span):
        cleaned_tokens = [self._clean_token(token.text) for token in sentence]
        cleaned_sentence = " ".join([token for token in cleaned_tokens if token])
        return cleaned_sentence, (sentence.start_char, sentence.end_char)

    def _clean_token(self, token: str):
        # TODO extract this to a utility function, this is duplicate code
        if not token.strip():
            return None

        cleaned_token = re.sub(r"[^a-z0-9]", "", token.lower())

        if cleaned_token == "":
            return None
        return cleaned_token
