import numpy as np
import pandas as pd
import sqlite3

from FoodTables.FoundationTable import FoundationTable

if __name__ == '__main__':
    u_df = pd.read_csv("conversions.csv", dtype={"id": "string", "name": "string", "equiv": np.float64})
    u_df.to_sql(
        name="unit_conversions",
        con=sqlite3.connect("data.sqlite"),
        if_exists="replace",
        index=False
    )

    ft = FoundationTable()
    ft_df = ft.get_df()
    ft_df.to_sql(
        name="food",
        con=sqlite3.connect("data.sqlite"),
        if_exists="replace",
        index=False
    )
