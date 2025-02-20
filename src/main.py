import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from table import Table
from datetime import date
import calendar
from indicator import generate_indicators_dataframe
from enum import Enum
import mysql.connector as mysql
import os

load_dotenv()

COMPUTED_DATES = np.array(
    [date(2024, month, calendar.monthrange(2024, month)[1]) for month in range(1, 13)]
)


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


def get_table_names():
    config = get_connection("raduale_fundiq_ultimate")
    cnx = mysql.connect(**config)
    cursor = cnx.cursor()
    query = "SELECT IndicatorRatioCode, TableName FROM 1d_1a_Results_IndicatorRatioType"
    cursor.execute(query)
    ratio_tables = cursor.fetchall()
    ratio_tables_dict = {str(ratio[0]).lower(): str(ratio[1]) for ratio in ratio_tables}
    cursor.close()
    cnx.close()
    return ratio_tables_dict


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
    fund_group_ids = [i for i in range(1, 11)]
    model_portfolio_ids = [i for i in range(1, 8)]

    n_dates = len(COMPUTED_DATES)
    n_time = len(TimePeriod)
    n_horizon = len(HorizonPeriod)

    total_rows_fund_groups = len(fund_group_ids) * n_dates * n_time * n_horizon
    total_rows_model_portfolios = (
        len(model_portfolio_ids) * n_dates * n_time * n_horizon
    )
    total_rows = total_rows_fund_groups + total_rows_model_portfolios

    random_indicators = generate_indicators_dataframe(total_rows)

    extra_columns = []

    for fg in fund_group_ids:
        for d in COMPUTED_DATES:
            for time in TimePeriod:
                for horizon in HorizonPeriod:
                    extra_columns.append(
                        {
                            "date": d,
                            "periodicity": time.value,
                            "time_horizon": horizon.value[1],
                            "fund_group_id": fg,
                            "model_portfolio_id": None,
                        }
                    )

    for mp in model_portfolio_ids:
        for d in COMPUTED_DATES:
            for time in TimePeriod:
                for horizon in HorizonPeriod:
                    extra_columns.append(
                        {
                            "date": d,
                            "periodicity": time.value,
                            "time_horizon": horizon.value[1],
                            "fund_group_id": None,
                            "model_portfolio_id": mp,
                        }
                    )

    extra_df = pl.DataFrame(
        extra_columns,
        schema={
            "date": pl.Date,
            "periodicity": pl.Int32,
            "time_horizon": pl.Int32,
            "fund_group_id": pl.Int64,
            "model_portfolio_id": pl.Int64,
        },
    )

    combined_df = extra_df.hstack(random_indicators)
    tables = get_table_names()

    keys_to_update = [
        "twr",
        "mwr",
        "twt",
        "mwt",
        "mdd",
        "mhw",
        "psd",
        "dsd",
        "usd",
        "rtd",
        "rdd",
        "rud",
        "sor",
        "shr",
        "rrr",
        "ifr",
        "trr",
        "alf",
        "bet",
    ]

    insertion_data_by_table = {}
    for key in keys_to_update:
        target_table = tables.get(key)
        if target_table is not None:
            insertion_data_by_table[target_table] = {
                "fund_groups": [],
                "model_portfolios": [],
            }

    for row in combined_df.to_dicts():
        date_val = row["date"]
        periodicity = row["periodicity"]
        time_horizon = row["time_horizon"]

        if row["model_portfolio_id"] is not None:
            row_type = "model_portfolios"
            id_value = row["model_portfolio_id"]
        else:
            row_type = "fund_groups"
            id_value = row["fund_group_id"]

        for key in keys_to_update:
            if key not in row:
                continue
            indicator_value = row.get(key)
            target_table = tables.get(key)
            if target_table is None:
                continue

            insertion_tuple = (
                str(id_value) if id_value is not None else None,
                time_horizon,
                periodicity,
                date_val,
                indicator_value,
                None,
                None,
            )

            insertion_data_by_table[target_table][row_type].append(insertion_tuple)

    config_fund_groups = get_connection("raduale_strader_fund_data_results_fundgroup")
    table_obj_fund_groups = Table(config_fund_groups)

    config_model_portfolios = get_connection(
        "raduale_strader_fund_data_results_portfliomodel"
    )
    table_obj_model_portfolios = Table(config_model_portfolios)

    query_template = (
        "INSERT INTO {} ({}, CalculationHorizonID, Input_PeriodicityID, Date, IndicatorRatioValue, BenchmarkSymbol, BenchmarkSymbol_RF) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    for table_name, type_data in insertion_data_by_table.items():
        if type_data["fund_groups"]:
            query = query_template.format(table_name, "FundGroupName_ID")
            print(
                f"Inserting {len(type_data['fund_groups'])} rows into {table_name} for fund groups"
            )
            table_obj_fund_groups.batch_insert_data(query, type_data["fund_groups"])
        if type_data["model_portfolios"]:
            query = query_template.format(table_name, "ModelPortfolio_ID")
            print(
                f"Inserting {len(type_data['model_portfolios'])} rows into {table_name} for model portfolios"
            )
            table_obj_model_portfolios.batch_insert_data(
                query, type_data["model_portfolios"]
            )


if __name__ == "__main__":
    main()
