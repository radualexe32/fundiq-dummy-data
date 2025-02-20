import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from table import Table
from indicator import ReturnIndicators, VolIndicators, MptIndicators
from enum import Enum
import os

load_dotenv()


def get_env_var(var_name, default=None):
    return os.environ.get(var_name, default)


def get_connection(database: str):
    return {
        "user": get_env_var("DB_USER"),
        "password": get_env_var("DB_PASSWORD"),
        "host": get_env_var("DB_HOST"),
        "port": get_env_var("DB_PORT"),
        "database": database,
    }


class TimePeriod(Enum):
    MONTHLY = 2
    QUARTERLY = 3


class HorizonPeriod(Enum):
    MTD = ("mtd_horizon", 1)
    QTD = ("qtd_horizon", 2)
    SATD = ("satd_horizon", 3)
    YTD = ("ytd_horizon", 4)
    H1M = ("one_month_horizon", 5)
    H3M = ("three_months_horizon", 6)
    H6M = ("six_months_horizon", 7)
    H9M = ("nine_months_horizon", 8)
    H1Y = ("one_year_horizon", 9)
    H2Y = ("two_years_horizon", 10)
    H3Y = ("three_years_horizon", 11)
    H5Y = ("five_years_horizon", 12)
    H7Y = ("seven_years_horizon", 13)
    H10Y = ("ten_years_horizon", 14)

    @classmethod
    def from_string(cls, value):
        return next((item for item in cls if item.value[0] == value), None)


def main():
    pass


if __name__ == "__main__":
    main()
