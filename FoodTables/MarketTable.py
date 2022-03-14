import numpy as np

from FoodTables.FoodTable import FoodTable
from FoodTables.utils import get_joinner


class MarketTable(FoodTable):
    def __init__(self):
        super(MarketTable, self).__init__("market_acquistion", "market_acquistion")

    def _clean(self):
        self._df = self._df.apply(get_joinner("description", "modifier"), axis=1) \
            .drop(columns=["portion_description", "modifier"])
