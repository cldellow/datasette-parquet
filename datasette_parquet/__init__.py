from datasette import hookimpl
from .ducky import DuckDatabase

@hookimpl
def startup(datasette):
    db = DuckDatabase(datasette)

    datasette.add_database(db, 'parquet')
