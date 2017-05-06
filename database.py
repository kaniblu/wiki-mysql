# encoding: utf-8

import pymysql


class Database(object):
    def __init__(self, **kwargs):
        self.db_params = kwargs
        self.db = pymysql.connect(**kwargs)

    def execute_script(self, path):
        with self.cursor() as c:
            with open(path, "r") as f:
                c.execute(f.read())

    @property
    def is_connected(self):
        return self.db is not None and self.db.open

    def connect(self):
        self.db = pymysql.connect(**self.db_params)

    def cursor(self):
        if not self.is_connected:
            self.connect()

        return self.db.cursor()

    def close(self):
        if self.is_connected:
            self.db.close()

    def commit(self):
        if self.is_connected:
            self.db.commit()