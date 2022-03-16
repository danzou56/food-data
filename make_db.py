import argparse
import json
import pathlib
import sqlite3

import numpy as np
import pandas as pd

import FoodTables.FoodTable
from FoodTables.FoundationTable import FoundationTable
from FoodTables.MarketTable import MarketTable

VOLUME_CONVERSIONS_TABLE = "volume_conversions"
MASS_CONVERSIONS_TABLE = "mass_conversions"
FOOD_TABLE = "food"


def reset_db(con: sqlite3.Connection, query: str, table_name: str):
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(query.replace("${TABLE_NAME}", table_name))


def get_create_queries(path: pathlib.Path):
    sorted_paths = sorted(path.iterdir())
    if not sorted_paths:
        raise Exception("Schmea directory was empty!")

    last_path = sorted_paths[-1]
    with open(last_path, 'r') as f:
        schema_json = json.loads('\n'.join(f.readlines()))
        return {e["tableName"]: e["createSql"] for e in schema_json["database"]["entities"]}


def main():
    parser = argparse.ArgumentParser(description="Populate sqlite database with USDA data")
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("schema_dir", type=pathlib.Path)

    args = parser.parse_args()

    con = sqlite3.connect(args.output)
    FoodTables.FoodTable.DB_PATH = args.output

    queries = get_create_queries(args.schema_dir)
    # TODO ensure queries keys give exactly the expected tables
    for table_name, query in queries.items():
        reset_db(con, query, table_name)

    u_df = pd.read_csv("conversions.csv", dtype={"id": "string", "name": "string", "equiv": np.float64})
    u_df.to_sql(
        name=VOLUME_CONVERSIONS_TABLE,
        con=con,
        if_exists="append",
        index=False
    )

    tables = [FoundationTable(), MarketTable()]
    pd.concat(
        [table.get_df() for table in tables],
        ignore_index=True
    ).to_sql(
        name=FOOD_TABLE,
        con=con,
        if_exists="append",
        index=False
    )


if __name__ == '__main__':
    main()
