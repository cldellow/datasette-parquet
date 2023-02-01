import asyncio
import time
from datasette.database import Database, Results
import re
import sqlite3
import sqlglot
import duckdb
from .ddl import create_views

table_xinfo_re = re.compile('^PRAGMA table_xinfo[(](.+)[)]')

NO_OP_SQL = 'SELECT 0 WHERE 1 = 0'


class Row:
    def __init__(self, columns, tpl):
        self.columns = columns
        self.tpl = tpl

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.tpl[key]
        else:
            print('columns={} key={} tpl={}'.format(self.columns, key, self.tpl))
            return self.tpl[self.columns[key]]


def set_progress_handler(handler, n):
    print('ignoring set_progress_handler')

def rewrite(sql):
    #print('rewrite: {}'.format(sql))

    if ' DATE(' in sql or ' date(' in sql:
        sql = NO_OP_SQL

    sql = sql.replace('<> ""', "<> ''")
    sql = sql.replace('!= ""', "!= ''")

    # This errors if the column is not valid JSON.
    # Can probably wrap it with JSON_VALID(...) and be safe?
    # DuckDB doesn't support json_each, though, so that's not enough
    if ' json_type(' in sql or ' JSON_TYPE(' in sql:
        sql = NO_OP_SQL

    sql = sql.replace('"????-??-*"', "'????-??-*'")

    if ' GLOB ' in sql or ' glob ' in sql:
        print('WARNING: rewriting a GLOB to a LIKE, this is almost certainly wrong')
        sql = sql.replace('GLOB', ' LIKE ').replace(' glob ', ' LIKE ')

    if sql == 'PRAGMA schema_version':
        sql = 'SELECT 0'

    if sql == 'select 1 from sqlite_master where tbl_name = "geometry_columns"':
        sql = NO_OP_SQL

    if sql == 'select name from sqlite_master where type="table"':
        sql = "select name from sqlite_master where type='table'"

    # Thwart https://github.com/simonw/datasette/blob/0b4a28691468b5c758df74fa1d72a823813c96bf/datasette/utils/__init__.py#L1120-L1127
    if sql.startswith('explain '):
        # This triggers a fallback path where it assumes the params based on a regex.
        # An alternative: use sqlglot to find params, then return a table shape
        # that matches what datasette is expecting.
        raise sqlite3.DatabaseError()

    # DuckDB doesn't support table_xinfo, so use table_info with a faked hidden column
    m = table_xinfo_re.search(sql)
    if m:
        sql = 'SELECT *, 0 FROM pragma_table_info({})'.format(m.group(1))

    # DuckDB doesn't support this pragma.
    # Luckily, Parquet doesn't have foreign keys, so just return no rows
    if sql.startswith('PRAGMA foreign_key_list'):
        sql = NO_OP_SQL

    # DuckDB doesn't support this pragma.
    # Luckily, Parquet doesn't have indexes, so just return no rows
    if sql.startswith('PRAGMA index_list'):
        sql = NO_OP_SQL

    # This is some query to discover if FTS is enabled?
    if 'VIRTUAL TABLE%USING FTS' in sql:
        sql = NO_OP_SQL

    # Transpile queries, eg [test] is not a valid way to quote a table
    # in DuckDB.
    #print('before transpile: {}'.format(sql))
    if not sql.startswith('PRAGMA'):
        sql = sqlglot.transpile(sql, read='sqlite', write='duckdb')[0]

    #print('after transpile: {}'.format(sql))

    return sql

def fixup_params(sql, parameters):

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
        print('ProxyCursor.execute')
        # Duckdb doesn't support schema_version; for
        # Parquet files, that's OK - they'll never change.
        sql = rewrite(sql)
        sql, parameters = fixup_params(sql, parameters)

        print('params={} sql={}'.format(parameters, sql))
        return self.cursor.execute(sql, parameters)

    def fetchall(self):
        print('ProxyCursor.fetchall')
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
        print(rv)
        return rv


    def __getattr__(self, name):
        return getattr(self.cursor, name)

class ProxyConnection:
    def __init__(self):
        conn = duckdb.connect()
        self.conn = conn

    def execute(self, sql, parameters=None):
        print('ProxyConnection.execute')
        sql = rewrite(sql)
        sql, parameters = fixup_params(sql, parameters)
        rv = self.conn.execute(sql, parameters)

        return ProxyCursor(self.conn, rv)

    def fetchall(self):
        print('ProxyConnection.fetchall: TODO: actually fetch')
        return []

    # TODO: re-enable this
    def set_progress_handler(self, handler, n):
        print('ProxyConnection: ignoring set_progress_handler')
        pass

    def cursor(self):
        print('ProxyConnection.cursor called')
        return ProxyCursor(self.conn)

class DuckDatabase(Database):
    def __init__(self, ds, directory):
        super().__init__(ds)

        #conn = duckdb.connect()
        #conn.set_progress_handler = set_progress_handler
        conn = ProxyConnection()

        for create_view_stmt in create_views(directory):
            conn.conn.execute(create_view_stmt)

        self.conn = conn
        pass

    @property
    def size(self):
        # TODO: implement this? Not sure what we'd return.
        return 0

    async def execute_fn(self, fn):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    # Datasette expects all tables to have either a pk or a rowid,
    # parquet files don't meet that criteria.
    #async def get_view_definition(self, view):
    #    return await self.get_table_definition(view, "table")
