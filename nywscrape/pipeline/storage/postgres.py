from psycopg2._psycopg import Binary, AsIs
from psycopg2.extensions import connection
import pickle

from psycopg2.extras import execute_values


class PostgresStorageSink:
    def __init__(self, conn: connection, tablename: str, field_metadata_mapping: dict, input_: str):
        self.conn = conn
        self.tablename = tablename
        self.field_metadata_mapping = field_metadata_mapping
        self.input = input_
        self.output = None
        pass

    def process(self, document, metadata):
        """
        Document will be saved as a pickle and metadata
        must TODO metadata processor
        :param document:
        :param metadata:
        :return:
        """
        pickled = pickle.dumps(document)
        keys, values = list(zip(*self._generate_mapped_metadata(metadata).items()))
        keys += ("doc",)
        values += (Binary(pickled),)
        sql = 'INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING'
        cur = self.conn.cursor()
        cur.execute(sql, (AsIs(self.tablename), AsIs(",".join(keys)), values))
        self.conn.commit()

    def process_batch(self, documents, metadata):
        keys_values = [{**self._generate_mapped_metadata(met), "doc": Binary(pickle.dumps(doc))} for doc, met in zip(documents, metadata)]
        sql = "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING".format(self.tablename, ",".join(keys_values[0].keys()))
        cur = self.conn.cursor()

        execute_values(cur, sql, [tuple(kv.values()) for kv in keys_values])
        self.conn.commit()

    def _generate_mapped_metadata(self, metadata):
        return {k: self._get_in_dict_recursive(metadata, v)
                for k, v in self.field_metadata_mapping.items()}

    def _get_in_dict_recursive(self, metadata, keys):
        if type(keys) == str:
            return metadata[keys] if keys in metadata else None
        if not keys:
            return metadata
        if keys[0] not in metadata:
            return None
        return self._get_in_dict_recursive(metadata[keys[0]], keys[1:])

