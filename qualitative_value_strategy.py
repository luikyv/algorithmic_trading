"""Qualitative Value Strategy Script"""
from time import sleep

import pandas as pd
import requests
from scipy import stats

import config

######################################## Variables ########################################

ALPHA_VANTAGE_API_STOCK_OVERVIEW = (
    "https://www.alphavantage.co/query?function=OVERVIEW&symbol={stock}&apikey=" + config.ALPHA_VANTAGE_API_KEY
)
ALPHA_VANTAGE_API_STOCK_CLOSE_PRICE = (
    "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={stock}&apikey="
    + config.ALPHA_VANTAGE_API_KEY
)

######################################## Functions ########################################


def set_rv_columns() -> list[str]:
    return [
        "Stock",
        # "Price",
        "PE Ratio",  # Price to Earnings Ratio
        "PE Ratio Percentile",
        "PB Ratio",  # Price to Book Ratio
        "PB Ratio Percentile",
        "PS Ratio",
        "PS Ratio Percentile",
        "EV/EBITDA",
        "EV/EBITDA Percentile",
        # "EV/GP",
        # "EV/GP Percentile",
        "RV Score",  # Robust Value Score
    ]


def calculate_percentile(row: pd.Series, rv_df: pd.DataFrame) -> pd.Series:

    row["PE Ratio Percentile"] = stats.percentileofscore(rv_df[f"PE Ratio"], row[f"PE Ratio"]) / 100
    row["PB Ratio Percentile"] = stats.percentileofscore(rv_df[f"PB Ratio"], row[f"PB Ratio"]) / 100
    row["PS Ratio Percentile"] = stats.percentileofscore(rv_df[f"PS Ratio"], row[f"PS Ratio"]) / 100
    row["EV/EBITDA Percentile"] = stats.percentileofscore(rv_df[f"EV/EBITDA"], row[f"EV/EBITDA"]) / 100
    return row


def create_rv_df(number_of_stocks: int) -> pd.DataFrame:

    stocks = pd.read_csv("sp_500_stocks.csv")
    rv_cols = set_rv_columns()
    rv_df = pd.DataFrame(columns=rv_cols)

    for idx, stock in enumerate(stocks["Ticker"][:number_of_stocks]):

        # After requesting 5 times, the api blocks our calls during 60 s
        if idx > 4 and idx % 5 == 0:
            sleep(65)
        print(f"Requesting information about stock: {stock}")
        r = requests.get(url=ALPHA_VANTAGE_API_STOCK_OVERVIEW.format(stock=stock))
        if r.status_code == 200:
            overview_data: dict = r.json()
            try:
                row = [
                    stock,
                    # None,
                    float(overview_data["PERatio"]),
                    None,
                    float(overview_data["PriceToBookRatio"]),
                    None,
                    float(overview_data["PriceToSalesRatioTTM"]),
                    None,
                    float(overview_data["EVToEBITDA"]),
                    None,
                    # float(overview_data["PERatio"]),
                    # None,
                    None,
                ]
                rv_df = rv_df.append(
                    pd.Series(
                        row,
                        index=rv_cols,
                    ),
                    ignore_index=True,
                )
            except (ValueError, KeyError):
                pass

    rv_df = rv_df.apply(lambda row: calculate_percentile(row=row, rv_df=rv_df), axis=1)
    rv_df["RV Score"] = rv_df[
        [
            "PE Ratio Percentile",
            "PB Ratio Percentile",
            "PS Ratio Percentile",
            "EV/EBITDA Percentile",
        ]
    ].mean(
        axis=1,
    )  # Calculate rv score
    return rv_df


######################################## Main ########################################


def main() -> None:

    hqm_df: pd.DataFrame = create_rv_df(number_of_stocks=100)
    hqm_df.sort_values(by="RV Score", ascending=True, inplace=True)

    writer = pd.ExcelWriter("analysis_tables/value_strategy.xlsx", engine="xlsxwriter")
    hqm_df.to_excel(writer, sheet_name="Value Strategy", index=False)
    writer.save()


if __name__ == "__main__":
    main()
