import asyncio
import duckdb
from .debounce import debounce
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler
from datasette.database import Database, Results
from .ddl import create_views
from .winging_it import ProxyConnection

class SchemaEventHandler(FileSystemEventHandler):
    """React to files being added/removed from the watched directory."""

    def __init__(self, reload):
        super().__init__()

        self.reload = reload

    @debounce(1)
    def on_event(self):
        self.reload()

    def on_moved(self, event):
        super().on_moved(event)
        self.on_event()

    def on_created(self, event):
        super().on_created(event)
        self.on_event()

    def on_deleted(self, event):
        super().on_deleted(event)
        self.on_event()

    def on_modified(self, event):
        super().on_modified(event)
        self.on_event()

def create_directory_connection(directory):
    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)

    for create_view_stmt in create_views(directory):
        conn.conn.execute(create_view_stmt)

    return conn

class DuckDatabase(Database):
    def __init__(self, ds, directory=None, file=None, httpfs=None, watch=None):
        super().__init__(ds)

        self.engine = 'duckdb'

        if directory:
            conn = create_directory_connection(directory)

            def reload():
                self.conn.conn.close()
                self.conn = create_directory_connection(directory)

            event_handler = SchemaEventHandler(reload)
            observer = Observer()
            observer.schedule(event_handler, directory, recursive=True)
            observer.start()
        elif file:
            raw_conn = duckdb.connect(file, read_only=True)
            conn = ProxyConnection(raw_conn)
        else:
            raise Exception('must specify directory or file')

        if httpfs:
            conn.conn.execute('install httpfs;').fetchall()
            conn.conn.execute('load httpfs;').fetchall()

        self.conn = conn

    @property
    def size(self):
        # TODO: implement this? Not sure if it's useful.
        return 0

    async def execute_fn(self, fn):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

    async def execute_write_fn(self, fn, block=True):
        if self.ds.executor is None:
            raise Exception('non-threaded mode not supported')

        def in_thread():
            return fn(self.conn)

        # We lie, we'll always block.
        return await asyncio.get_event_loop().run_in_executor(
            self.ds.executor, in_thread
        )

