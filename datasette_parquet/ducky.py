import asyncio
import time
import sqlite3
import duckdb
from datasette.database import Database, Results
from .ddl import create_views
from .rewrite import rewrite, NO_OP_SQL

class Row:
    def __init__(self, columns, tpl):
        self.columns = columns
        self.tpl = tpl

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.tpl[key]
        else:
            return self.tpl[self.columns[key]]

def fixup_params(sql, parameters):
    if isinstance(parameters, dict) and 'csrftoken' in parameters:
        # Writable canned queries send this ... probably a bug?
        parameters.pop('csrftoken')

    # Sometimes we skip queries that DuckDB can't handle, eg DATE(...) facet queries.
    # If the old query had parameters, sending them with the new query will
    # cause an assertion to fail. So return an empty list of parameters.
    if sql == NO_OP_SQL:
        return sql, []

    if isinstance(parameters, (tuple, list)):
        return sql, parameters
    else:
        new_params = []
        for i, (k, v) in enumerate((parameters or {}).items()):
            new_params.append(v)
            sql = sql.replace(':' + k, '${}'.format(i + 1))

        #print('new sql: {}'.format(sql))
        #print('new params: {}'.format(new_params))
        return sql, new_params

class ProxyCursor:
    def __init__(self, conn, existing_cursor=None):
        self.conn = conn

        if existing_cursor:
            self.cursor = existing_cursor
        else:
            self.cursor = self.conn.cursor()

    def execute(self, sql, parameters=None):
        #print('# params={} sql={}'.format(parameters, sql))
        sql = rewrite(sql)
        sql, parameters = fixup_params(sql, parameters)

        #print('## params={} sql={}'.format(parameters, sql))
        t = time.time()
        rv = self.cursor.execute(sql, parameters)
        #print('took {}'.format(time.time() - t))
        return rv

    def fetchall(self):
        tpls = self.cursor.fetchall()
        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return [Row(columns, tpl) for tpl in tpls]

    def __iter__(self):
        return self

    def __next__(self):
        rv = self.cursor.fetchone()

        if rv == None:
            raise StopIteration

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i
        rv = Row(columns, rv)
        return rv

    def __getattr__(self, name):
        return getattr(self.cursor, name)

class ProxyConnection:
    def __init__(self):
        conn = duckdb.connect()
        self.conn = conn

    def __enter__(self):
        pass

    def __exit__(self, exc_type,exc_value, exc_traceback):
        pass

    def execute(self, sql, parameters=None):
        #print('! params={} sql={}'.format(parameters, sql))
        sql = rewrite(sql)
        sql, parameters = fixup_params(sql, parameters)
        #print('!! params={} sql={}'.format(parameters, sql))
        rv = self.conn.execute(sql, parameters)

        return ProxyCursor(self.conn, rv)

    def fetchall(self):
        raise Exception('TODO: ProxyConnection.fetchall is not implemented')

    def set_progress_handler(self, handler, n):
        pass

    def cursor(self):
        return ProxyCursor(self.conn)

class DuckDatabase(Database):
    def __init__(self, ds, directory):
        super().__init__(ds)

        conn = ProxyConnection()

        for create_view_stmt in create_views(directory):
            conn.conn.execute(create_view_stmt)

        #print(conn.conn.execute('install httpfs;').fetchall())
        #print(conn.conn.execute('load httpfs;').fetchall())

        self.conn = conn

    @property
    def size(self):
        # TODO: implement this? Not sure if it's useful.
        return 0

    async def execute_fn(self, fn):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    async def execute_write_fn(self, fn, block=True):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        # We lie, we'll always block.
        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

