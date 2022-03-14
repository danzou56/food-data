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
        self._df = None

        if FoodTable._data_con is None:
            FoodTable._data_con = sqlite3.connect(DB_PATH)

        if FoodTable.base_df is None:
            FoodTable._init_base_df()

        self._dirty_df: pd.DataFrame = FoodTable.base_df[FoodTable.base_df["data_type"] == data_type]

    @staticmethod
    def _init_base_df():
        units_df = pd.read_csv(os.path.join(DATA_PATH, "measure_unit.csv"), dtype="string")
        # units_df.loc[units_df["name"] == "Tablespoons", "name"] = "tablespoon"
        conversions_df = pd.read_sql_query(sql="SELECT * FROM unit_conversions", con=FoodTable._data_con)
        units_df = units_df.merge(
            conversions_df,
            on="id",
            how="outer"
        )
        FoodTable.units_df = units_df.copy()
        units_df = units_df.drop(columns="name")

        food_df = pd.read_csv(os.path.join(DATA_PATH, "food.csv"), dtype="string").drop(
            columns=["food_category_id", "publication_date"])
        portions_df = pd.read_csv(os.path.join(DATA_PATH, "food_portion.csv"), dtype="string").drop(
            columns=["min_year_acquired", "data_points", "footnote"])

        FoodTable.base_df = food_df.merge(portions_df, on="fdc_id") \
            .merge(units_df.rename(columns={"id": "measure_unit_id"}), on="measure_unit_id") \
            .drop(columns=["measure_unit_id"])

    def _clean(self):
        pass

    def _make(self):
        self._df = self._dirty_df.copy()
        self._clean()
        self._df = self._df.dropna(subset=["equiv"])
        self._df["gram_weight"] = self._df["gram_weight"].astype(np.float64)
        self._df["amount"] = self._df["amount"].astype(np.float64)
        self._df["g_per_ml"] = self._df["gram_weight"] / self._df["amount"] * self._df["equiv"]
        self._df = self._df.drop(columns=["amount", "gram_weight", "equiv"])

    def get_df(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df

        self._make()
        assert self._df is not None
        return self._df