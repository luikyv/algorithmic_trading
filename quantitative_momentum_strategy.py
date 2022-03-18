"""Quantitative Momentum Strategy Script"""
from time import sleep

import numpy as np
import pandas as pd
import requests
from scipy import stats

import config

######################################## Variables ########################################

NUMBER_OF_MONTHS_TO_ANALYSE = 6
ALPHA_VANTAGE_API_MONTHLY_URL = (
    "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={stock}&apikey="
    + config.ALPHA_VANTAGE_API_KEY
)

######################################## Functions ########################################


def set_hqm_columns() -> list[str]:
    hqm_columns = [
        "Stock",
    ]
    for idx in range(1, NUMBER_OF_MONTHS_TO_ANALYSE + 1):
        hqm_columns += [
            f"Closing Price on Month-{idx}",
            f"Return on Month-{idx}",
            f"Return Percentile on Month-{idx}",
        ]
    return hqm_columns + [
        "HQM Score",
    ]


def calculate_rate_change(now: str, then: str) -> float:
    """Calculate the price change between two months
    The prices are given as strings
    """
    return (float(now) - float(then)) / float(then)


def calculate_percentile(row: pd.Series, hqm_df: pd.DataFrame) -> pd.Series:

    for idx in range(1, 1 + NUMBER_OF_MONTHS_TO_ANALYSE):
        row[f"Return Percentile on Month-{idx}",] = (
            stats.percentileofscore(hqm_df[f"Return on Month-{idx}"], row[f"Return on Month-{idx}"]) / 100
        )
    return row


def calculate_hqm_score(row: pd.Series) -> pd.Series:

    percentile_sum = 0
    for idx in range(1, 1 + NUMBER_OF_MONTHS_TO_ANALYSE):
        percentile_sum += row[f"Return Percentile on Month-{idx}"]
    row["HQM Score"] = percentile_sum / NUMBER_OF_MONTHS_TO_ANALYSE
    return row


def create_hqm_df(number_of_stocks: int) -> pd.DataFrame:

    hqm_columns = set_hqm_columns()
    close_price_row = "5. adjusted close"
    stocks = pd.read_csv("sp_500_stocks.csv")
    hqm_df = pd.DataFrame(columns=hqm_columns)

    for idx, stock in enumerate(stocks["Ticker"][:number_of_stocks]):

        if idx > 4 and idx % 5 == 0:
            sleep(70)
        print(f"Requesting information about stock: {stock}")
        r = requests.get(url=ALPHA_VANTAGE_API_MONTHLY_URL.format(stock=stock))
        if r.status_code == 200:
            try:
                monthly_data: dict = r.json()["Monthly Adjusted Time Series"]
                months = list(monthly_data.keys())
                row_data = [stock]
                for month, previous_month in zip(
                    months[1 : 1 + NUMBER_OF_MONTHS_TO_ANALYSE],
                    months[2 : 2 + NUMBER_OF_MONTHS_TO_ANALYSE],
                ):
                    row_data += [
                        monthly_data[month][close_price_row],  # Closing Price
                        calculate_rate_change(
                            monthly_data[month][close_price_row],
                            monthly_data[previous_month][close_price_row],
                        ),  # Return on Month
                        None,  # Return Percentile
                    ]
                row_data += [None]  # HQM Score

                hqm_df = hqm_df.append(
                    pd.Series(
                        row_data,
                        index=hqm_columns,
                    ),
                    ignore_index=True,
                )
            except KeyError:
                pass

    # Chage price columns type to float
    hqm_df[[f"Closing Price on Month-{idx}" for idx in range(1, 1 + NUMBER_OF_MONTHS_TO_ANALYSE)]].astype(float)
    # Calculate metrics
    hqm_df = hqm_df.apply(lambda row: calculate_percentile(row, hqm_df), axis=1).apply(calculate_hqm_score, axis=1)

    return hqm_df


######################################## Main ########################################


def main() -> None:

    hqm_df: pd.DataFrame = create_hqm_df(number_of_stocks=100)
    hqm_df.sort_values(by="HQM Score", ascending=False, inplace=True)

    writer = pd.ExcelWriter("analysis_tables/momentum_strategy.xlsx", engine="xlsxwriter")
    hqm_df.to_excel(writer, sheet_name="Momentum Strategy", index=False)
    writer.save()


if __name__ == "__main__":
    main()
