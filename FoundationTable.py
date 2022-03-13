import numpy as np

from FoodTable import FoodTable
from utils import get_joinner


class FoundationTable(FoodTable):
    def __init__(self):
        super(FoundationTable, self).__init__("foundation", "foundation_food")

    def _clean(self):
        # this is the only instance where portion_description is non-nan so modify the description to
        # drop the portion_description column
        self._df.loc[self._df["id"] == "119620", "description"] = "Cheese, cheddar, shredded"
        self._df.loc[self._df["id"] == "119620", "portion_description"] = np.nan
        # make sure there's no more non-nans, just in case data changes in future
        assert len(self._df.dropna(subset="portion_description")) == 0
        self._df = self._df.drop(columns="portion_description")
        self._df["description"] = self._df["description"].apply(lambda s: s.strip())
        self._df = self._df.apply(get_joinner("description", "modifier"), axis=1).drop(columns="modifier")
