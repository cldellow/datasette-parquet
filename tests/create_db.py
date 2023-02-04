import duckdb
import os

def sql():
    f = open('tests/fixtures.sql')
    rv = f.read()
    f.close()
    return rv

def create_dbs(prefix = 'trove'):
    fname = '{}/fixtures.duckdb'.format(prefix)
    try:
        os.remove(fname)
    except FileNotFoundError:
        pass
    c = duckdb.connect(fname)
    c.execute(sql()).fetchall()

    c.execute('''COPY fixtures TO '{}/fixtures.parquet' (FORMAT PARQUET);'''.format(prefix));

if __name__ == '__main__':
    create_dbs()
