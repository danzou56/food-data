import pandas as pd


def get_joinner(col1, col2):
    def joinner(row):
        if pd.isna(row[col1]):
            row[col1] = row[col2].strip(", ")
        elif pd.isna(row[col2]):
            row[col1] = row[col1].strip(", ")
        else:
            row[col1] = row[col1].strip(", ") + ", " + row[col2].strip(", ")
        return row
    return joinner

VOLUME_UNITS = """cup
tablespoon
teaspoon
liter
milliliter
cubic inch
cubic centimeter
gallon
pint
fl oz""".split("\n")
UNITS = """

"""

def is_real_unit(possible_unit):
    possible_unit = possible_unit.lower()
    return False

def is_volume_unit(unit):
    return unit.lower() in VOLUME_UNITS

def clean_unit(unit):
    if pd.isna(unit):
        return unit
    unit = unit.strip(", ")
    #return unit
    if is_volume_unit(unit):
        return unit
    return None
    for real_unit in VOLUME_UNITS:
        if real_unit in unit:
            #print(f"Discarding extra information: {unit} becoming {real_unit}")
            return real_unit
    #print(f"{unit} wasn't a real unit")
    return None
