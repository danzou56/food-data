import numpy as np

from FoodTable import FoodTable
from utils import get_joinner


class FoundationTable(FoodTable):
    def __init__(self):
        super(FoundationTable, self).__init__("foundation", "foundation_food")

    def _clean(self):
        # this is the only instance where portion_description is non-nan so modify the description to
        # drop the portion_description column
        self.df.loc[self.df["id"] == "119620", "description"] = "Cheese, cheddar, shredded"
        self.df.loc[self.df["id"] == "119620", "portion_description"] = np.nan
        # make sure there's no more non-nans, just in case data changes in future
        assert len(self.df.dropna(subset="portion_description")) == 0
        self.df = self.df.drop(columns="portion_description")
        self.df["description"] = self.df["description"].apply(lambda s: s.strip())
        self.df = self.df.apply(get_joinner("description", "modifier"), axis=1).drop(columns="modifier")


if __name__ == '__main__':
    ft = FoundationTable()
    print("Done")