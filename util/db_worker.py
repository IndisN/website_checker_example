# coding: utf-8

import os
import logging

from psycopg2.extras import RealDictCursor

import psycopg2


class DbWorker:
    def __init__(
            self, user=None, host=None, port=None,
            password=None, password_fname=None, db_name=None, ssl_mode=None,
    ):
        self.user = user
        self.host = host
        self.port = port
        self.password = password
        if self.password is None:
            self._password_from_fname(password_fname)
        self.db_name = db_name
        self.ssl_mode = ssl_mode

        self.service_uri = f'postgres://{self.user}:{self.password}@{self.host}:{self.port}' \
                           f'/{self.db_name}?sslmode={self.ssl_mode}'

        self.db_conn = None

    def _password_from_fname(self, fname):
        if not os.path.exists(fname):
            raise FileNotFoundError(fname)
        self.password = open(fname).read().rstrip('\n')

    def __enter__(self):
        logging.info('connect to %s', self.service_uri)
        self.db_conn = psycopg2.connect(self.service_uri)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logging.info('close connection')
        self.db_conn.close()

    def execute(self, sql_query, sql_vars=None, expect_result=False, commit=True):
        result = None
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            logging.info('execute query:\n\t%s\n\twith vars %s', sql_query, sql_vars)
            cursor.execute(sql_query, sql_vars)
            if expect_result:
                result = cursor.fetchall()
        if commit:
            self.db_conn.commit()
            logging.info('commited')
        return result
