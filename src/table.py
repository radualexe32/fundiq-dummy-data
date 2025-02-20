import numpy as np
import mysql.connector as mysql
from mysql.connector import pooling
import os


class Table:
    def __init__(self, config):
        self.config = config
        self.pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            **self.config,
        )
        self.max_packet_size = int(os.getenv("DB_ALLOWED_PACKET_SIZE", "16777216"))
        self.estimated_row_size = 200
        self.batch_size = self.max_packet_size // self.estimated_row_size

    def batch_insert_data(self, query, data):
        if not data:
            return

        for i in range(0, len(data), self.batch_size):
            batch = data[i : i + self.batch_size]
            self._insert_batch(query, batch)

    def _insert_batch(self, query, batch):
        cnx = self.pool.get_connection()
        cursor = cnx.cursor()

        try:
            prepared_data = [
                tuple(
                    None
                    if (
                        value == ""
                        or value == "NULL"
                        or (isinstance(value, float) and np.isnan(value))
                    )
                    else value
                    for value in row
                )
                for row in batch
            ]

            cursor.executemany(query, prepared_data)
            cnx.commit()
        except mysql.Error as e:
            print(f"Error: {e}")
            cnx.rollback()
        finally:
            cursor.close()
            cnx.close()
