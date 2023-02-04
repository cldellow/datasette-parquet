from datasette import hookimpl

PLUGIN_NAME = 'datasette-parquet'

@hookimpl
def startup(datasette):
    config = datasette.plugin_config(
        PLUGIN_NAME
    )

    if not config:
        return

    from .ducky import DuckDatabase
    from .patches import monkey_patch

    monkey_patch()

    for db_name, options in config.items():
        if not 'directory' in options:
            raise Exception('datasette-parquet: expected directory key for db {}'.format(db))

        directory = options['directory']
        db = DuckDatabase(datasette, directory)
        datasette.add_database(db, db_name)
