import re
from typing import List


class MinimalCleaningStrategy:
    def __init__(self, input_="raw", output="cleaned_document"):
        self.input = input_
        self.output = output

    def process(self, document: str, metadata: dict):
        preprocessed = document.lower()
        preprocessed = re.sub(r"[^a-z0-9\. ]", " ", preprocessed)
        preprocessed = re.sub(r"\s\s+", " ", preprocessed)
        return [(preprocessed, {**metadata, "original": document})]

    def process_batch(self, documents: List[str], metadata: List[dict]):
        return [(newdoc, newmeta)
                for doc, meta in zip(documents, metadata)
                for newdoc, newmeta in self.process(doc, meta)]
