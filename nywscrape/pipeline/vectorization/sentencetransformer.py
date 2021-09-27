from typing import List

import torch
from sentence_transformers import SentenceTransformer

class SentenceTransformerVectorizationStrategy:
    def __init__(self, model_name="all-mpnet-base-v2", input_="cleaned_document", output='document_vector', device=None):
        if device is None:
            device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        self.input = input_
        self.output = output
        self.model = SentenceTransformer(model_name, device=device)

    def process(self, document: str, document_metadata: dict):
        return [(self.model.encode(document), document_metadata)]

    def process_batch(self, documents: List[str], document_metadata: List[dict]):
        vecs = self.model.encode(documents)
        return zip([vec for vec in vecs], document_metadata)