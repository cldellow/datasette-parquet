import re
import sqlglot
import sqlite3
table_xinfo_re = re.compile('^PRAGMA table_xinfo[(](.+)[)]')
table_info_square_re = re.compile('^PRAGMA table_info[(]\[(.+)][)]')

NO_OP_SQL = 'SELECT 0 WHERE 1 = 0'


def rewrite(sql):
    # print('rewrite: {}'.format(sql))

    if ' DATE(' in sql or ' date(' in sql:
        sql = NO_OP_SQL

    sql = sql.replace('<> ""', "<> ''")
    sql = sql.replace('!= ""', "!= ''")

    # This throws a duckdb exception if the column is not valid JSON,
    # but Datasette is only expecting SQLite exceptions.
    #
    # We could maybe translate that specific exception? Very sleazy.
    #
    # DuckDB also doesn't support json_each, though, so why bother?
    if ' json_type(' in sql or ' JSON_TYPE(' in sql:
        sql = NO_OP_SQL

    sql = sql.replace('"????-??-*"', "'????-??-*'")

    # TODO: once sqlglot #1066 lands, we can remove this
    if ' GLOB ' in sql or ' glob ' in sql:
        print('WARNING: rewriting a GLOB to a LIKE, this is almost certainly wrong')
        sql = sql.replace(' GLOB ', ' LIKE ').replace(' glob ', ' LIKE ')

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

    m = table_info_square_re.search(sql)
    if m:
        sql = 'PRAGMA table_info("{}")'.format(m.group(1))

    # DuckDB doesn't support this pragma.
    # Luckily, Parquet doesn't have foreign keys, so just return no rows
    if sql.startswith('PRAGMA foreign_key_list'):
        sql = NO_OP_SQL

    # DuckDB doesn't support this pragma.
    # Luckily, Parquet doesn't have indexes, so just return no rows
    if sql.startswith('PRAGMA index_list'):
        sql = NO_OP_SQL

    if sql.startswith('PRAGMA recursive_triggers'):
        sql = NO_OP_SQL

    # This is some query to discover if FTS is enabled?
    if 'VIRTUAL TABLE%USING FTS' in sql:
        sql = NO_OP_SQL

    # Transpile queries, eg [test] is not a valid way to quote a table
    # in DuckDB.
    #print('before transpile: {}'.format(sql))
    if not sql.startswith('PRAGMA') and not sql.startswith('COPY ') and not "from '" in sql:
        sql = sqlglot.transpile(sql, read='sqlite', write='duckdb')[0]

    #print('after transpile: {}'.format(sql))

    return sql

