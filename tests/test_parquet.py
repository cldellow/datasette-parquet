from datasette.app import Datasette
from .create_db import create_dbs
import pytest
import duckdb
from datasette_parquet.winging_it import ProxyConnection

@pytest.fixture(scope="session")
def datasette():
    create_dbs('./fixtures')
    metadata = {
        'plugins': {
            'datasette-parquet': {
                'trove': {
                    'directory': './fixtures'
                },
                'duckdb': {
                    'file': './fixtures/fixtures.duckdb'
                }

            }
        }
    }

    return Datasette(
        [],
        memory=True,
        metadata=metadata,
    )

@pytest.mark.asyncio
async def test_plugin_is_installed(datasette):
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-parquet" in installed_plugins

@pytest.mark.asyncio
async def test_file_mode(datasette):
    response = await datasette.client.get('/duckdb')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_directory_mode(datasette):
    response = await datasette.client.get('/trove')
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_json_works(datasette):
    response = await datasette.client.get("/trove/fixtures.json?_size=max&_labels=on&_shape=objects")
    assert response.status_code == 200
    assert response.json()['rows'] == [{'date': '2023-01-01', 'ts': '2023-01-02T03:04:05' }]

@pytest.mark.asyncio
async def test_extraneous_parameters(datasette):
    response = await datasette.client.get("/trove?sql=select+%2A+from+fixtures&_hide_sql=1")
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_sql_json(datasette):
    response = await datasette.client.get("/trove.json?sql=select+%2A+from+fixtures&_hide_sql=1")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_duckdb_table(datasette):
    response = await datasette.client.get("/duckdb/fixtures")
    assert response.status_code == 200

def test_fetchone():
    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)
    fetched = conn.execute('SELECT 1 AS col').fetchone()
    assert fetched['col'] == 1


@pytest.mark.asyncio
def test_catch_double_quote_usage_for_literal(datasette):

    # copy a parquet file across from trove/userdata1.parquet
    
    import pathlib
    import shutil
    
    
    working_dir = pathlib.Path().cwd()
    fixtures_dir = pathlib.Path().cwd() / 'fixtures' 
    userdata1_path = pathlib.Path().cwd() / 'trove' / 'userdata1.parquet'
    userdata_parquet_fixture_path = fixtures_dir / 'userdata1.parquet'
    assert userdata1_path.exists()
    
    userdata_parquet_fixture_path.unlink(missing_ok=True)
    assert not userdata_parquet_fixture_path.exists()

    # with our watch taking place this should mean our parquet table is now accessible
    shutil.copy(str(userdata1_path), str(fixtures_dir))

    raw_conn = duckdb.connect()
    conn = ProxyConnection(raw_conn)

    # explodey_string_with_double_quotes = '''

    # SELECT * from userdata1 WHERE first_name = "Amanda"

    # '''
    explodey_string_with_double_quotes = 'SELECT * from "./trove/userdata1.parquet" WHERE first_name = "Amanda"'

    # TODO: add assert for exception being thrown
    # breakpoint()
    result = conn.execute(explodey_string_with_double_quotes).fetchall()
    
    

