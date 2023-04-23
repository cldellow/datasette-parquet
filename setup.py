from setuptools import setup
import os

VERSION = "0.6"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-parquet",
    description="Read Parquet files in Datasette",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Colin Dellow",
    url="https://github.com/cldellow/datasette-parquet",
    project_urls={
        "Issues": "https://github.com/cldellow/datasette-parquet/issues",
        "CI": "https://github.com/cldellow/datasette-parquet/actions",
        "Changelog": "https://github.com/cldellow/datasette-parquet/releases",
    },
    license="Apache License, Version 2.0",
    classifiers=[
        "Framework :: Datasette",
        "License :: OSI Approved :: Apache Software License"
    ],
    version=VERSION,
    packages=["datasette_parquet"],
    entry_points={"datasette": ["parquet = datasette_parquet"]},
    install_requires=["datasette", "duckdb", "sqlglot", "watchdog"],
    extras_require={"test": ["pytest", "pytest-asyncio", "pytest-watch"]},
    python_requires=">=3.7",
)
