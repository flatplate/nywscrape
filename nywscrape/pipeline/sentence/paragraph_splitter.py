import re

class ParagraphSplittingStrategy:
    def __init__(self, input_='raw', output='sentence'):
        self.input = input_
        self.output = output
        self._pattern = r"\n"

    def process(self, document: str, metadata: dict):
        matches = [0] + [m.start() + 1 for m in re.finditer(self._pattern, document)] + [len(document)]
        sents = [(document[start:end], start, end) for start, end in zip(matches[:-1], matches[1:])]
        return [sent[0] for sent in sents], [{"doc": metadata, "start": start, "end": end, "sentence_id": i}
                                             for i, (_, start, end) in enumerate(sents)]

    def process_batch(self, documents, metadata):
        res = [self.process(doc, meta) for doc, meta in zip(documents, metadata)]
        return [(sentdoc, sentmeta) for doc, meta in res for sentdoc, sentmeta in zip(doc, meta) if len(sentdoc.strip()) > 0]
