import logging


class CommandLineSink:
    def __init__(self, input_):
        self.input = input_
        self.output = None

    def process(self, document, meta):
        logging.info("Received document: {}, {}".format(document, meta))

    def process_batch(self, documents, meta):
        for d, m in zip(documents, meta):
            self.process(d, m)