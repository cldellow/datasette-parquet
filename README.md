# datasette-parquet

[![PyPI](https://img.shields.io/pypi/v/datasette-parquet.svg)](https://pypi.org/project/datasette-parquet/)
[![Changelog](https://img.shields.io/github/v/release/cldellow/datasette-parquet?include_prereleases&label=changelog)](https://github.com/cldellow/datasette-parquet/releases)
[![Tests](https://github.com/cldellow/datasette-parquet/workflows/Test/badge.svg)](https://github.com/cldellow/datasette-parquet/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cldellow/datasette-parquet/blob/main/LICENSE)

Support DuckDB, Parquet, CSV and [JSON Lines](https://jsonlines.org/) files in Datasette. Depends on DuckDB.

There is a demo at https://dux.fly.dev/parquet

Compare a query using [Parquet on DuckDB](https://dux.fly.dev/parquet/geonames_stats) vs the same query on [SQLite](https://dux.fly.dev/geonames/geonames_stats). The DuckDB query is ~3-5x faster. On a machine with more than 1 core, DuckDB would outperform by an even higher margin.

## Installation

Install this plugin in the same environment as Datasette.

    datasette install datasette-parquet

## Usage

You can use this plugin to access a DuckDB file, or a directory of CSV/Parquet/JSON files.

### DuckDB file

To mount the `/data/mydb.duckdb` file as a database called `mydb`, create a metadata.json like:

```
{
  "plugins": {
    "datasette-parquet": {
      "mydb": {
        "file": "/data/mydb.duckdb"
      }
    }
  }
}
```


### Directory of CSV/Parquet/JSON files

Say you have a directory of your favourite CSVs, newline-delimited JSON and parquet
files that looks like this:

```
/data/census.csv
/data/books.tsv
/data/tweets.jsonl
/data/geonames.parquet
/data/sales/january.parquet
/data/sales/february.parquet
```

You can expose these in a Datasette database called `trove` by something
like this in your `metadata.json`:

```
{
  "plugins": {
    "datasette-parquet": {
      "trove": {
        "directory": "/data",
        "watch": true
      }
    }
  }
}
```

Then launch Datasette via `datasette --metadata metadata.json`

You will have 5 views in the `trove` database: `census`, `books`, `tweets`, `geonames` and `sales`.
The `sales` view will be the union of all the files in that directory -- this works for all of the file types, not just Parquet.

Because you passed the `watch` option with a value of `true`, Datasette will automatically discover when
files are added or removed, and create or remove views as needed.

### Common options

These options can be used in either mode.

`httpfs` - set to `true` to enable the [HTTPFS extension](https://duckdb.org/docs/extensions/httpfs.html)

## Caveats

> **Warning**
>
> You know that old canard, that if it walks like a duck and quacks like a duck, it's probably a duck? This plugin tries to teach DuckDB to walk like SQLite and talk like SQLite. It's difficult, and frankly, I just winged this part. If you come across broken features, let me know and I'll try to fix them up.

- No timeouts: A core feature of Datasette is that it's safe to let the unwashed masses run arbitrary queries. This is because the data is immutable, and there are timeouts to prevent runaway CPU usage. DuckDB does not currently support timeouts. Think carefully about letting anonymous users use a Datasette instance with this plugin.
    - You will likely want to [disable facet suggestions from the CLI](https://docs.datasette.io/en/stable/settings.html#suggest-facets), or install [datasette-ui-extras](https://github.com/cldellow/datasette-ui-extras), which disables facet suggestions.
- Joining with existing data: This plugin uses DuckDB, not SQLite. This means that you cannot join against your existing SQLite tables.
- Read-only: the data in the files can only be queried, not changed.
- Performance: the files are queried in-place. Performance will be limited by the file type -- parquet files have a zippy binary format, but large CSV and JSONL files might be slow.
- Facets: DuckDB supports a different set of syntax than SQLite. This means some Datasette features are incompatible, and will be disabled for DuckDB-backed files.

## Technical notes

This plugin has a mix of accidental complexity and essential complexity.
The essential complexity comes from things like "DuckDB supports a different
dialect of SQL". The accidental complexity comes from things like "it's
called the _Law_ of Demeter, Colin, not the Strongly Held Opinion
of Demeter".

This is a loose journal of things I ran into:

- DuckDB's Python API is similar to the `sqlite3` module's interface, but not
  the same. Datasette expects to talk to an interface that conforms to `sqlite3`,
  so this plugin crufts up some proxy objects to give a "convincing" facade.
  I mostly YOLOd this part. I wouldn't trust it for write queries, or for
  reading sensitive data.
    - DuckDB doesn't have the concept of a separate cursor class.
    - sqlite3's cursor is an iterable
    - Datasette uses sqlite3.Row objects, which support indexing by name
    - sqlite3 supports parameterized queries like `execute('SELECT :p', {'p': 123})`.
      These need to be rewritten to use numbered parameters and a list.

- SQLite supports slightly different syntax than DuckDB. We use [sqlglot](https://github.com/tobymao/sqlglot)
  to transpile queries into DuckDB's dialect.
    - In homage to MySQL, SQLite supports string literals delimited by double
      quotes. Datasette uses this feature, see https://github.com/simonw/datasette/issues/2001
    - In homage to SQL Server, SQLite supports quoting identifiers with square
      brackets. Datasette uses this feature, see https://github.com/simonw/datasette/issues/2013

- Unfortunately, using sqlglot brings its own challenges: it doesn't recognize
  the `GLOB` operator, see https://github.com/tobymao/sqlglot/issues/1066

- Datasette passes extraneous parameters to the sqlite3 connection. A writable
  canned query will post a `csrftoken` for security purposes, which ends up
  as part of the query parameters. DuckDB is strict on the parameters matching
  the SQL query, so it fails.

- Datasette expects some SQLite internals to be around, like certain `PRAGMA ...` functions,
  or the shape of the `EXPLAIN` output. We work around this by detecting those
  queries and telling bald-faced lies to Datasette.

- Datasette expects `json_type(...)` to throw a `sqlite3.OperationalError` on invalid
  JSON, but DuckDB will (of course) throw its own type: `duckdb.InvalidInputException`

- DuckDB is missing some functions from SQLite: `json_each(...)`, `date(...)`

- `rowid` columns in SQLite are stable identifiers. This is not true in DuckDB.

- SQLite's Python interface supports interrupting long-running queries. DuckDB's
  C API supports this, too, but it has not yet been exposed to the Python API.
  See https://github.com/duckdb/duckdb/issues/5938 and https://github.com/duckdb/duckdb/pull/3749

- Datasette's CustomJSONEncoder only expects objects of the sort that SQLite can
  store. DuckDB has native support for the `date` type, which requires patching.

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:

    cd datasette-parquet
    python3 -m venv venv
    source venv/bin/activate

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
