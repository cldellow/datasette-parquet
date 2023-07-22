import time
import re
import typing

from duckdb import BinderException 

from .rewrite import rewrite, NO_OP_SQL
from . import exceptions


# a regular expression to find the literal wrapped by the double quoted string
# GOOD for DuckDB, works fine:  
# `WHERE column = 'some value'` 
# 
# BAD for DuckDB, triggers duckdb.BinderException:
# `WHERE column = "some value"` 
DOUBLE_QUOTES_AROUND_LITERALS_PATTERN = r'(?<![A-Za-z0-9_])"(?:[^"]*(?:"[^"]*)*[^"]*)"(?![A-Za-z0-9_])' # noqa

def find_matching_double_quote_usage(ex: BinderException) -> typing.List[str]:
    """
    Accepts a BinderException from DuckDB, and returns a list of
    matching strings in double quotes
    """
    # should return 'Binder Error: Referenced column "LITERAL_IN_DOUBLE_QUOTES" '
    referenced_column_message = ex.args[0].split(' not found in FROM clause')[0]
    pattern = DOUBLE_QUOTES_AROUND_LITERALS_PATTERN

    # find the offending values wrapped in double quotes
    matches = re.findall(pattern, referenced_column_message)
    return matches


# A collection of classes to provide a facade that mimics the sqlite3 DB-API
# interface.
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
    if isinstance(parameters, dict):
        # On the custom SQL page, Datasette jams any query parameter it finds
        # into the parameters for the backend. DuckDB is strict on unexpected
        # parameters, so try to remove them

        new_parameters = {}
        for k, v in parameters.items():
            if ':{}'.format(k) in sql:
                new_parameters[k] = v

        parameters = new_parameters

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
        try:
            rv = self.cursor.execute(sql, parameters)
        except BinderException as ex:
            matches = find_matching_double_quote_usage(ex)
            if matches:
                raise exceptions.DoubleQuoteForLiteraValue(matches)
            else:
                # continue raising the original BinderException
                raise

        #print('took {}'.format(time.time() - t))
        return rv

    def fetchone(self):
        tpl = self.cursor.fetchone()

        if not tpl:
            return tpl

        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return Row(columns, tpl)

    def fetchmany(self, size=1):
        tpls = self.cursor.fetchmany(size)
        columns = {}
        for i, x in enumerate(self.cursor.description):
            columns[x[0]] = i

        return [Row(columns, tpl) for tpl in tpls]

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
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        pass

    def __exit__(self, exc_type,exc_value, exc_traceback):
        pass

    def execute(self, sql, parameters=None):
        #print('! params={} sql={}'.format(parameters, sql))
        sql = rewrite(sql)
        #print('! rewritten sql={}'.format(sql))
        sql, parameters = fixup_params(sql, parameters)
        #print('!! params={} sql={}'.format(parameters, sql))
        try:
            rv = self.conn.execute(sql, parameters)
        except BinderException as ex:
            matches = find_matching_double_quote_usage(ex)
            if matches:
                raise exceptions.DoubleQuoteForLiteraValue(matches)
            else:
                # continue raising the original BinderException
                raise
            
        return ProxyCursor(self.conn, rv)

    def fetchall(self):
        raise Exception('TODO: ProxyConnection.fetchall is not implemented')

    def set_progress_handler(self, handler, n):
        pass

    def cursor(self):
        return ProxyCursor(self.conn)


