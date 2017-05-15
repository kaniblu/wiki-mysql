# encoding: utf-8

import time
import logging
import inspect

import pymysql


class Database(object):
    def __init__(self, max_retries=10, **kwargs):
        self.max_retries = max_retries
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

    def execute(self, func):
        for i in range(self.max_retries):
            try:
                with self.cursor() as c:
                    ret = func(c)
                    return ret
            except pymysql.OperationalError as e:
                if e.args[0] == 2006:
                    logging.warning("received 2006 operational error.")
                    logging.warning("retrying {}-th time after 1 sec..."
                                    .format(i + 1))
                    time.sleep(1)
                    self.connect()
                    continue
                else:
                    raise e

    def insert(self, table_name, column_map,
               auto_column=None, ignore_errors=False):
        assert column_map

        tname = table_name
        ins_cols, params = zip(*column_map.items())
        value_sql = ("%s", ) * len(params)

        if auto_column is not None:
            ins_cols = (auto_column, ) + ins_cols
            value_sql = ("NULL", ) + value_sql

        ins_cols = ["`{}`".format(c) for c in ins_cols]
        ins_cols = ", ".join(ins_cols)
        value_sql = ", ".join(value_sql)

        sql = "INSERT INTO `{}` ({}) value ({})".format(
            tname, ins_cols, value_sql
        )

        def _insert(cursor):
            ret = cursor.execute(sql, params)

            if auto_column is not None:
                return cursor.lastrowid
            else:
                return ret

        try:
            return self.execute(_insert)
        except pymysql.InternalError as e:
            logging.warning("An internal error occurred during insertion")
            logging.warning("failed insertion:")

            frame = inspect.currentframe()
            _, _, _, values = inspect.getargvalues(frame)

            for param, value in values.items():
                logging.warning("    {}={}".format(param, value))
                
            logging.exception(e)

            if ignore_errors:
                return None
            else:
                raise e