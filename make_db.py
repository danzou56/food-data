import numpy as np
import pandas as pd
import sqlite3

from FoodTables.FoundationTable import FoundationTable

VOLUME_CONVERSIONS_TABLE = "volume_conversions"
MASS_CONVERSIONS_TABLE = "mass_conversions"
FOOD_TABLE = "food"

con = sqlite3.connect("data.sqlite")


def reset_db():
    con.execute(f"DROP TABLE IF EXISTS {VOLUME_CONVERSIONS_TABLE}")
    con.execute(f"CREATE TABLE IF NOT EXISTS `{VOLUME_CONVERSIONS_TABLE}` "
                f"(`id` TEXT NOT NULL, `name` TEXT NOT NULL, `equiv` REAL NOT NULL, PRIMARY KEY(`id`))")


if __name__ == '__main__':
    reset_db()

    u_df = pd.read_csv("conversions.csv", dtype={"id": "string", "name": "string", "equiv": np.float64})
    u_df.to_sql(
        name=VOLUME_CONVERSIONS_TABLE,
        con=con,
        if_exists="append",
        index=False
    )

    ft = FoundationTable()
    ft_df = ft.get_df()
    ft_df.to_sql(
        name=FOOD_TABLE,
        con=sqlite3.connect("data.sqlite"),
        if_exists="replace",
        index=False
    )
