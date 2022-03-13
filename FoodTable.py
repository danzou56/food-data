import os
import numpy as np
import pandas as pd
import sqlite3

FOOD_DATA_VERSION = "2021-10-28"
VERSION = FOOD_DATA_VERSION.replace("-", "")
DATA_PATH = os.path.join("data", FOOD_DATA_VERSION)
DB_PATH = "data.sqlite"


class FoodTable:
    base_df = None
    units_df = None
    _data_con = None

    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type

        if FoodTable._data_con is None:
            FoodTable._data_con = sqlite3.connect(DB_PATH)

        # if FoodTable.units_df is None:
        #     FoodTable.units_df = pd.read_sql_table(table_name="conversions", con=FoodTable._data_con)

        # TODO: drop name column of units_df
        if FoodTable.base_df is None:
            units_df = pd.read_csv(os.path.join(DATA_PATH, "measure_unit.csv"), dtype="string")
            # units_df.loc[units_df["name"] == "Tablespoons", "name"] = "tablespoon"
            conversions_df = pd.read_sql_query(sql="SELECT * FROM conversions", con=FoodTable._data_con)
            units_df = units_df.merge(
                conversions_df,
                on="id",
                how="outer"
            )

            food_df = pd.read_csv(os.path.join(DATA_PATH, "food.csv"), dtype="string").drop(
                columns=["food_category_id", "publication_date"])
            portions_df = pd.read_csv(os.path.join(DATA_PATH, "food_portion.csv"), dtype="string").drop(
                columns=["min_year_acquired", "data_points", "footnote"])

            FoodTable.base_df = food_df.merge(portions_df, on="fdc_id") \
                .merge(units_df.rename(columns={"id": "measure_unit_id", "name": "unit_name"}), on="measure_unit_id") \
                .drop(columns=["measure_unit_id"])

        self.df: pd.DataFrame = FoodTable.base_df[FoodTable.base_df["data_type"] == data_type]
        self._clean()
        self.df = self.df.dropna(subset=["equiv"])
        self.df["gram_weight"] = self.df["gram_weight"].astype(np.float64)
        self.df["amount"] = self.df["amount"].astype(np.float64)
        self.df["g_per_ml"] = self.df["gram_weight"] / self.df["amount"] * self.df["equiv"]
        self.df = self.df.drop(columns=["amount", "gram_weight", "unit_name", "equiv"])

    def _clean(self):
        pass
