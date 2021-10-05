# spy_csv_data.py

from datetime import datetime as dt
import os
import sys
sys.path.append(os.path.join('..', 'pricing'))

import pandas as pd

from alpha_vantage import AlphaVantage


if __name__ == "__main__":
    # Create an AlphaVantage API instance
    av = AlphaVantage()

    # Download the SPY ETF OHLCV data from 1998-01-02 to 2018-01-31
    start_date = dt(1998, 1, 2)
    end_date = dt(2018, 1, 31)

    print("Obtaining SPY data from AlphaVantage and saving as CSV...")
    spy = av.get_daily_historic_data('SPY', start_date, end_date)
    spy.to_csv("SPY.csv", index=True)
