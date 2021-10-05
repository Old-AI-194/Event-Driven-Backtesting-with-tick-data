# appl_csv_data.py

from datetime import datetime as dt
import os
import sys
sys.path.append(os.path.join('..', 'pricing'))

import pandas as pd

from alpha_vantage import AlphaVantage


if __name__ == "__main__":
    # Create an AlphaVantage API instance
    av = AlphaVantage()

    # Download the Apple Group OHLCV data from 1998-01-02 to 2008-12-31
    start_date = dt(1998, 1, 2)
    end_date = dt(2008, 12, 31)

    print("Obtaining Apple data from AlphaVantage and saving as CSV...")
    aapl = av.get_daily_historic_data('AAPL', start_date, end_date)
    aapl.to_csv("AAPL.csv", index=True)
