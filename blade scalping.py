# mac.py

from datetime import datetime as dt

import numpy as np
import pandas as pd

from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio
from datetime import timezone
from dateutil import tz


class BSStrategy(Strategy):
    """
    Carries out a basic Moving Average Crossover strategy with a
    short/long simple weighted moving average. Default short/long
    windows are 100/400 periods respectively.
    """

    def __init__(
        self, bars, events, short_window=10, mid_window= 21, long_window=50, min_diff = 1, min_deep = 0, plus = 0, sl = 5, tp = 10, ts_step = 5
    ):
        """
        Initialises the Moving Average Cross Strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        short_window - The short moving average lookback.
        long_window - The long moving average lookback.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = short_window
        self.mid_window = mid_window
        self.long_window = long_window
        self.use_window = 4 * self.long_window
        self.min_diff = min_diff * 0.0001
        self.min_deep = min_deep * 0.0001
        self.plus = plus * 0.0001
        self.sl = sl * 0.0001
        self.tp = tp * 0.0001
        self.ts_step = ts_step * 0.0001

        # Set to True if a symbol is in the market
        self.bought = self._calculate_initial_bought()
        #Dat bien luu gia tri Po,Min,Max cua gia
        self.omm_price = dict( (k,v) for k, v in [(s, [0,0,0]) for s in self.symbol_list] ) #Xem xet de 0 hay None

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to 'OUT'.
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def _calculate_min_max_price(self, symbol, tick_price):
        """
        Luu gia vao lenh, max, min sau khi co tin hieu xuat hien.
        """
        if self.omm_price[symbol][0] != 0:
            self.omm_price[symbol][1] = max(self.omm_price[symbol][1],tick_price)
            self.omm_price[symbol][2] = min(self.omm_price[symbol][2],tick_price)

    def calculate_signals(self, event):
        """
        Generates a new set of signals based on the MAC
        SMA with the short window crossing the long window
        meaning a long entry and vice versa for a short entry.    

        Parameters
        event - A MarketEvent object. 
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars_value = pd.Series(self.bars.get_latest_bars_values(
                    s, "close", N=self.use_window
                ))
                tick_date = self.bars.get_latest_tick_datetime(s)
                tick_price = self.bars.get_latest_tick_value(s, "tick")

                self._calculate_min_max_price(s, tick_price) #De y xem nap tham so tick_price da dung chua
                po_price = self.omm_price[s][0]
                max_price = self.omm_price[s][1]
                min_price = self.omm_price[s][2]

                if bars_value is not None and len(bars_value) == self.use_window:
                    last_bar_price = bars_value.iloc[-1]
                    short_ema = bars_value.ewm(span=self.short_window, adjust=False).mean().iloc[-1]
                    mid_ema = bars_value.ewm(span=self.mid_window, adjust=False).mean().iloc[-1]
                    long_ema = bars_value.ewm(span=self.long_window, adjust=False).mean().iloc[-1]
                    hour_et = tick_date.replace(tzinfo=timezone.utc).astimezone(tz=tz.gettz('America/New_York')).hour
                    hour_ld = tick_date.replace(tzinfo=timezone.utc).astimezone(tz=tz.gettz('Europe/London')).hour

                    symbol = s
                    cur_date = dt.utcnow()
                    sig_dir = ""

                    if short_ema > last_bar_price > mid_ema > long_ema and self.bought[s] == "OUT" and hour_ld >= 8 and hour_et < 17 and short_ema - last_bar_price >= self.min_deep and short_ema - mid_ema >= self.min_diff and mid_ema - long_ema >= short_ema - mid_ema + self.plus:
                        print("LONG: %s" % tick_date)
                        sig_dir = 'LONG'
                        signal = SignalEvent(1, symbol, cur_date, sig_dir, 1.0)
                        self.events.put(signal)
                        for i in range(3):
                            self.omm_price[s][i] = tick_price
                        self.bought[s] = 'LONG'

                    elif short_ema < last_bar_price < mid_ema < long_ema and self.bought[s] == "OUT" and hour_ld >= 8 and hour_et < 17 and last_bar_price - short_ema >= self.min_deep and mid_ema - short_ema >= self.min_diff and long_ema - mid_ema >= mid_ema - short_ema + self.plus:
                        print("SHORT: %s" % tick_date)
                        sig_dir = 'SHORT'
                        signal = SignalEvent(1, symbol, cur_date, sig_dir, 1.0)
                        self.events.put(signal)
                        self.bought[s] = 'SHORT'
                        for i in range(3):
                            self.omm_price[s][i] = tick_price

                    elif self.bought[s] == "LONG" and (tick_price >= po_price + self.tp or (max_price - po_price >= self.ts_step and tick_price <= po_price) or (max_price - po_price < self.ts_step and tick_price <= po_price - self.sl)):
                        print("CLOSE_LONG: %s" % tick_date)
                        sig_dir = 'EXIT'
                        signal = SignalEvent(1, symbol, cur_date, sig_dir, 1.0)
                        self.events.put(signal)
                        for i in range(3):
                            self.omm_price[s][i] = 0
                        self.bought[s] = 'OUT'

                    elif self.bought[s] == "SHORT" and (tick_price <= po_price - self.tp or (po_price - min_price >= self.ts_step and tick_price >= po_price) or (po_price - min_price < self.ts_step and tick_price >= po_price + self.sl)):
                        print("CLOSE_SHORT: %s" % tick_date)
                        sig_dir = 'EXIT'
                        signal = SignalEvent(1, symbol, cur_date, sig_dir, 1.0)
                        self.events.put(signal)
                        for i in range(3):
                            self.omm_price[s][i] = 0
                        self.bought[s] = 'OUT'

if __name__ == "__main__":
    csv_dir = 'D:/'
    symbol_list = ['EURUSD']
    initial_capital = 100000.0
    heartbeat = 0.0
    start_date = dt(2020, 12, 31, 0, 0, 0)

    backtest = Backtest(
        csv_dir, symbol_list, initial_capital, heartbeat, 
        start_date, HistoricCSVDataHandler, SimulatedExecutionHandler, 
        Portfolio, BSStrategy
    )
    backtest.simulate_trading()
