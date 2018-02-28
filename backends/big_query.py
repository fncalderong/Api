# -*- coding: utf-8 -*-
import logging
from google.cloud import bigquery
from google.appengine.api import background_thread


class BqClient:
    def __init__(self):
        self.client = bigquery.Client()
    """
        def insert_row(self, data_set, table, rows):
        t = threading.Thread(target=self._insert_row, args=(data_set, table, rows))
        logging.info("Mensaje encolado")
        t.start()
    """
    def insert_row(self, data_set, table, data):
        background_thread.start_new_background_thread(self._insert_row, [data_set, table, data])

    def _insert_row(self, data_set, table, data):
        try:
            logging.info("DATA: %s" % data)
            dataset = self.client.dataset(data_set)
            _table = dataset.table(table)
            _table.reload()
            if not isinstance(data, list):
                logging.info("Insertnado mensaje en -> %s - %s" % (data_set, table))
                reg = []
                for column in _table.schema:
                    reg.append(data.get(column.name.lower(), None))
                inserted = _table.insert_data([tuple(reg)], ignore_unknown_values=True)
                logging.info("Mensaje insertado schema: %s:%s -> data:%s -> result:%s" %
                             (data_set, table, [tuple(reg)], inserted))
            else:
                logging.info("Insertnado lista de mensajes en -> %s - %s" % (data_set, table))
                rows = []
                for x in data:
                    reg = []
                    for column in _table.schema:
                        reg.append(x.get(column.name.lower(), None))
                    rows.append(tuple(reg))
                inserted = _table.insert_data(rows, ignore_unknown_values=True)
                logging.info("Mensaje insertado schema: %s:%s -> data:%s mensajes -> result:%s" %
                             (data_set, table, len(rows), inserted))
        except Exception as e:
            logging.error("Error insertando data en BigQuery: %s" % e)
            pass
