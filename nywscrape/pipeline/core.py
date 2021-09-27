import logging
from queue import Queue, Empty
from collections import defaultdict


class SimpleQueuePipeline:
    def __init__(self, batch_mode=False, batch_size=200):
        self.queue = Queue()
        self.listeners = defaultdict(list)
        self.sources = []
        self.batch_mode = batch_mode
        self.batch_size = batch_size

    def add_element(self, pipeline_element):
        if pipeline_element.input is None:
            self.sources.append(pipeline_element)
        else:
            self.listeners[pipeline_element.input].append(pipeline_element)

    def run_all(self):
        while not self.queue.empty():
            self.step()
        logging.info("Queue is empty")

    def step(self):
        try:
            key, item = self.queue.get(block=False)
            if self.batch_mode:
                self.process_batch(key, item)
            else:
                self.process(key, item)
            logging.info("Finished step for {}".format(key))
        except Empty:
            for source in self.sources:
                self.queue.put((source.output, source.process(None, None)))

    def process(self, key, item):
        for listener in self.listeners[key]:
            results = listener.process(item[0], item[1])
            if listener.output is not None:
                for result in results:
                    self.queue.put((listener.output, result))

    def process_batch(self, key, items):
        docs, metas = list(zip(*items))
        logging.info("Processing batch {}, batch size: {}".format(key, len(docs)))
        for listener in self.listeners[key]:
            logging.info("Batching with {}".format(listener))
            results = listener.process_batch(docs, metas)

            if listener.output is not None:
                self.queue.put((listener.output, results))

        logging.info("Batch finished for {}".format(key))
        logging.info("Queue size: {}".format(self.queue.qsize()))
        return