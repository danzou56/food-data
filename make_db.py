import argparse
import inspect
import json
import pathlib
import sqlite3
from os.path import isdir, isfile, getmtime

import numpy as np
import pandas as pd

import FoodTables.FoodTable
from FoodTables.FoundationTable import FoundationTable
from FoodTables.MarketTable import MarketTable

VOLUME_CONVERSIONS_TABLE = "volume_conversions"
MASS_CONVERSIONS_TABLE = "mass_conversions"
FOOD_TABLE = "food"
TABLES = [FoundationTable, MarketTable]


def reset_table(con: sqlite3.Connection, query: str, table_name: str):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(query.replace("${TABLE_NAME}", table_name))


def clear_table(con: sqlite3.Connection, table_name: str):
    con.execute(f"DELETE FROM {table_name} WHERE TRUE")


def get_most_recent(path: pathlib.Path):
    sorted_paths = sorted(path.iterdir())
    if not sorted_paths:
        return None
    else:
        return sorted_paths[-1]


def get_create_queries(path: pathlib.Path):
    with open(path, 'r') as f:
        schema_json = json.loads('\n'.join(f.readlines()))
        return {e["tableName"]: e["createSql"] for e in schema_json["database"]["entities"]}


def main():
    parser = argparse.ArgumentParser(description="Populate sqlite database with USDA data")
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("schema_dir", type=pathlib.Path)

    args = parser.parse_args()

    if not isdir(args.schema_dir):
        raise Exception("Schema directory wasn't a directory!")
    newest_schema = get_most_recent(args.schema_dir)
    if newest_schema is None:
        raise Exception("Schema directory was empty!")
    
    _con = None
    get_con = lambda: sqlite3.connect(args.output) if _con is None else _con
    FoodTables.FoodTable.DB_PATH = args.output
    
    db_lm = getmtime(args.output) if isfile(args.output) else 0
    schema_lm = getmtime(newest_schema)
    tables_lm = max([getmtime(inspect.getfile(table)) for table in TABLES])
    conv_lm = getmtime("conversions.csv")

    # Don't bother remaking the database if it's newer than the schema
    # To force recreation, touch the schema JSON
    if schema_lm >= db_lm:
        queries = get_create_queries(newest_schema)
        # TODO ensure queries keys give exactly the expected tables
        for table_name, query in queries.items():
            reset_table(get_con(), query, table_name)

        update_volume_table(get_con())
        update_food_table(get_con())
    else:
        if conv_lm >= db_lm:
            clear_table(get_con(), VOLUME_CONVERSIONS_TABLE)
            update_volume_table(get_con())
        if tables_lm >= db_lm:
            clear_table(get_con(), FOOD_TABLE)
            update_food_table(get_con())

    get_con().close()


def update_volume_table(con):
    pd.read_csv(
        "conversions.csv",
        dtype={"id": "string", "name": "string", "equiv": np.float64}
    ).to_sql(
        name=VOLUME_CONVERSIONS_TABLE,
        con=con,
        if_exists="append",
        index=False
    )


def update_mass_table(con):
    pass


def update_food_table(con):
    pd.concat(
        [table().get_df() for table in TABLES],
        ignore_index=True
    ).to_sql(
        name=FOOD_TABLE,
        con=con,
        if_exists="append",
        index=False
    )


if __name__ == '__main__':
    main()
