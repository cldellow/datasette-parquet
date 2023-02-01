from datasette import hookimpl
from .ducky import DuckDatabase

PLUGIN_NAME = 'datasette-parquet'

@hookimpl
def startup(datasette):
    config = datasette.plugin_config(
        PLUGIN_NAME
    )

    if not config:
        return

    for db_name, options in config.items():
        print('db={} options={}'.format(db_name, options))
        if not 'directory' in options:
            raise Exception('datasette-parquet: expected directory key for db {}'.format(db))

        directory = options['directory']
        db = DuckDatabase(datasette, directory)
        datasette.add_database(db, db_name)
