import numpy as np
import pandas as pd
import sqlite3

if __name__ == '__main__':
    df = pd.read_csv("units.csv", dtype={"id": "string", "name": "string", "equiv": np.float64})
    df.drop(columns="name").to_sql(
        name="conversions",
        con=sqlite3.connect("data.sqlite"),
        if_exists="replace",
        index=False
    )

