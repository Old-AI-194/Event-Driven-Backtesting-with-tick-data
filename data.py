# data.py

from abc import ABCMeta, abstractmethod
import os

import numpy as np
import pandas as pd

from event import MarketEvent


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the 
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low, 
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.latest_bars_data = {}
        self.current_bar = {}
        self.downsamp = {}

        self.continue_backtest = True       
        self.bar_index = 0

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        for s in self.symbol_list:
            # Load file csv theo tung dong mot
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.csv_dir, '%s.csv' % s),
                header=0, index_col=0, parse_dates=True,
                names=['datetime', 'tick'], chunksize = 1
            )
            self.latest_symbol_data[s] = []
            self.latest_bars_data[s] = []
            self.current_bar[s] = pd.DataFrame()

    def _get_new_tick(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield b

    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Tra ve data cua N bar gan nhat vao trong mot list, moi mot bar la mot DataFrame.
        """
        try:
            bars_list = self.latest_bars_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    """
    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]
    """

    def get_latest_tick_datetime(self, symbol):
        """
        Tra ve gia tri timestamp cua tick moi nhat duoc xu ly.
        """
        try:
            last_tick = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return last_tick[-1].iloc[0].name

    """
    def get_latest_bar_datetime(self, symbol):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]
    """

    def get_latest_tick_value(self, symbol, val_type):
        """
        Tra ve gia tri 'val_type' (open, high, low, close, Tick) cua tick cuoi cung.
        """
        try:
            last_tick = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return last_tick[-1].iloc[0][val_type]

    """
    def get_latest_bar_value(self, symbol, val_type):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
    """

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Tra ve gia tri O/H/L/C (val_type = open/high/low/close) cua N bar truoc do.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([b.iloc[0][val_type] for b in bars_list])

    """
    def get_latest_bars_values(self, symbol, val_type, N=1):
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])
    """


    def update_ticks(self):
        """
        Luu data tick, tao va luu data bar sau moi lan lap.
        """
        for s in self.symbol_list:
            self.downsamp[s] = None
            try:
                tick = next(self._get_new_tick(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if tick is not None:
                    self.latest_symbol_data[s] = [tick]
                    #self.latest_symbol_data[s].append(tick)
                    self.current_bar[s] = self.current_bar[s].append(tick)
                    self.downsamp[s] = list(self.current_bar[s].resample("5T", label='left', closed='left'))
                    if len(self.downsamp[s]) > 1:
                        self.latest_bars_data[s].append(self.downsamp[s][0][1].resample("5T", label='left', closed='left').ohlc().droplevel(0, axis=1))
                        self.current_bar[s] = self.current_bar[s].iloc[[-1]]
        self.events.put(MarketEvent())
