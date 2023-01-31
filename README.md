# datasette-parquet

[![PyPI](https://img.shields.io/pypi/v/datasette-parquet.svg)](https://pypi.org/project/datasette-parquet/)
[![Changelog](https://img.shields.io/github/v/release/cldellow/datasette-parquet?include_prereleases&label=changelog)](https://github.com/cldellow/datasette-parquet/releases)
[![Tests](https://github.com/cldellow/datasette-parquet/workflows/Test/badge.svg)](https://github.com/cldellow/datasette-parquet/actions?query=workflow%3ATest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/cldellow/datasette-parquet/blob/main/LICENSE)

Support Parquet, CSV and [JSON Lines](https://jsonlines.org/) files in Datasette. Depends on DuckDB.

## Installation

Install this plugin in the same environment as Datasette.

    datasette install datasette-parquet

## Usage

Say you have a directory of your favourite CSVs, newline-delimited JSON and parquet
files that looks like this:

```
/mnt/files/census.csv
/mnt/files/books.tsv
/mnt/files/tweets.jsonl
/mnt/files/geonames.parquet
/mnt/files/sales/january.parquet
/mnt/files/sales/february.parquet
```

You can expose these in a Datasette database called `trove` by something
like this in your `metadata.json`:

```
{
  "databases": {
    "trove": {
      "plugins": {
        "datasette-parquet": {
          "directory": "/mnt/files"
        }
      }
    }
  }
}
```

Then launch Datasette via `datasette --metadata metadata.json`

You will have 5 views in the `trove` database: `census`, `books`, `tweets`, `geonames` and `sales`.
The `sales` view will be the union of all the files in that directory -- this works for all of the file types, not just Parquet.

## Caveats

> **Note**
>
> You will likely want to disable facets, or install [datasette-ui-extras](https://github.com/cldellow/datasette-ui-extras), which disables facet suggestions.
> See the note on `No timeouts` for more information.

> **Warning**
>
> You know that old canard, that if it walks like a duck and quacks like a duck, it's probably a duck? This plugin tries to teach DuckDB to walk like SQLite and talk like SQLite. That turns out to be ducking hard! If you come across broken features, let me know and I'll try to fix them up.

- No timeouts: A core feature of Datasette is that it's safe to let the unwashed masses run arbitrary queries. This is because the data is immutable, and there are timeouts to prevent runaway CPU usage. DuckDB does not currently support timeouts (LINK TO ISSUE). Think carefully about letting anonymous users use a Datasette instance with this plugin.
- Joining with existing data: This plugin uses DuckDB, not SQLite. This means that you cannot join against your existing SQLite tables.
- Read-only: the data in the files can only be queried, not changed.
- Performance: the files are queried in-place. Performance will be limited by the file type -- parquet files have a zippy binary format, but large CSV and JSONL files might be slow.
- Facets: DuckDB supports a different set of syntax than SQLite. This means some Datasette features are incompatible, and will be disabled for DuckDB-backed files.

## Development

To set up this plugin locally, first checkout the code. Then create a new virtual environment:

    cd datasette-parquet
    python3 -m venv venv
    source venv/bin/activate

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
