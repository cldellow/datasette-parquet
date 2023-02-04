import duckdb

def sql():
    f = open('tests/fixtures.sql')
    rv = f.read()
    f.close()
    return rv

def go():
    c = duckdb.connect(':memory:')
    c.execute(sql()).fetchall()

    c.execute('''COPY fixtures TO 'trove/fixtures.parquet' (FORMAT PARQUET);''');
    c.execute('''COPY fixtures TO 'fixtures/fixtures.parquet' (FORMAT PARQUET);''');

if __name__ == '__main__':
    go()
