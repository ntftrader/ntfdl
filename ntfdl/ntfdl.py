"""
NTF Downloader

Downloads trade-, position- and history data from Netfonds.

Returned data in pandas dataframes

Additional data through get_info() and get_news()/get_history_news()

"""

__author__ = "NTF Trader"
__version__ = "0.1.0"
__license__ = "MIT"

import datetime
import pandas as pd
import numpy as np


class dl:
    trades = None
    positions = None
    history = None
    broker_stats = None

    exchanges = {
        'O': {'exchange': 'O', 'name': 'NASDAQ', 'active': False, 'premarket_open': '', 'open': '', 'close': '',
              'postmarket_close': '', 'trades': True, 'positions': True, 'history': True},
        'N': {'exchange': 'N', 'name': 'Nyse', 'active': True},
        'A': {'exchange': 'A', 'name': 'Amex', 'active': True},
        'FXSX': {'name': 'currency'},
        'GTIS': {'name': 'commodities'}}

    nicknames = {
        'WTI': {'name': 'West Texas Intermediate Crude Oil', 'instrument': 'C-EWTIUSDBR-SP', 'exchange': 'GTIS'},
        }

    def __init__(self, instrument, exchange="OSE", day="today", download=False, exclude_derivate=True):
        """Init Netfonds downloader
        Date: current date, accepts today, yesterday, monday-friday and datetime obj
        @TODO: verify date, verify not weekend, verify open exchange that day"""

        if day == "today":
            self.date = datetime.datetime.now()
        elif isinstance(day, datetime.datetime) or isinstance(day, datetime.date):
            self.date = day
        else:
            self.date = datetime.datetime.strptime(day, '%Y%m%d')

        self.date = self.date.strftime('%Y%m%d')

        self.exchange_pre = '%s 08:15:00' % self.date
        self.exchange_open = '%s 09:00:00' % self.date
        self.exchange_closed = '%s 16:25:59' % self.date
        self.exchange_post = '%s 17:00:00' % self.date

        self.instrument = instrument
        self.exchange = exchange

        self.exclude_derivative = exclude_derivate

        self.pos_url = 'http://hopey.netfonds.no/posdump.php?date=%s&paper=%s.%s&csv_format=csv'
        self.trade_url = 'http://hopey.netfonds.no/tradedump.php?date=%s&paper=%s.%s&csv_format=csv'
        self.history_url = 'http://www.netfonds.no/quotes/paperhistory.php?paper=%s.%s&csv_format=csv'

        if download:
            self._update()

    def _update(self):
        self._get_trades()
        self._get_positions()

    def new_date(self, date):

        self.date = date
        self._update()

    def get_date(self):

        return self.date

    def _get_trades(self):
        """Get all trades for given date"""

        trade_url = self.trade_url % (self.date, self.instrument, self.exchange)
        self.trades = pd.read_csv(trade_url, parse_dates=[0],
                                  date_parser=lambda t: pd.to_datetime(str(t), format='%Y%m%dT%H%M%S'))

        self.trades.fillna(np.nan)
        self.trades.index = pd.to_datetime(self.trades.time, unit='s')
        self.trades.time = pd.to_datetime(self.trades.time, unit='s')
        self.trades.columns = ['time', 'price', 'volume', 'source', 'buyer', 'seller', 'initiator']
        # del self.trades['time']

        if self.exclude_derivative:
            self.trades = self.trades[(self.trades.source != 'Derivatives trade') & (self.trades.source != 'Official')]

    def _get_positions(self):
        """Get position summary for date
        """
        pos_url = self.pos_url % (self.date, self.instrument, self.exchange)
        self.positions = pd.read_csv(pos_url, parse_dates=[0],
                                     date_parser=lambda t: pd.to_datetime(str(t), format='%Y%m%dT%H%M%S'))
        self.positions.fillna(np.nan)
        self.positions.index = pd.to_datetime(self.positions.time, unit='s')
        self.positions.columns = ['time', 'bid', 'bid_depth', 'bid_depth_total', 'ask', 'ask_depth', 'ask_depth_total']
        self.positions = self.positions[self.exchange_pre:self.exchange_post]

    def _get_broker_stats(self):
        stock_seller = pd.concat([self.trades.groupby(by=['seller'])['volume'].sum(),
                                  self.trades.groupby(by=['seller'])['price'].count()],
                                 axis=1, keys=['sold', 'sold_trades'])

        stock_buyer = pd.concat([self.trades.groupby(by=['buyer'])['volume'].sum(),
                                 self.trades.groupby(by=['buyer'])['price'].count()],
                                axis=1, keys=['bought', 'bought_trades'])

        self.broker_stats = pd.concat([stock_seller, stock_buyer], axis=1).fillna(np.nan)
        self.broker_stats.fillna(0, inplace=True)
        self.broker_stats['total'] = self.broker_stats.bought - self.broker_stats.sold

        self.broker_stats.sort_values(by='total', axis='index', ascending=False, inplace=True)

        self.broker_stats.reset_index(inplace=True)
        self.broker_stats.rename(columns={'index': 'broker'}, inplace=True)

        self.broker_stats.reset_index(inplace=True)

        self.broker_stats['positive'] = self.broker_stats['total'] > 0

    def _get_history(self):
        """"""

        self.history_url = self.history_url % (self.instrument, self.exchange)
        self.history = pd.read_csv(self.history_url, encoding="ISO-8859-1", parse_dates=[0])

        del self.history['paper']
        del self.history['exch']
        self.history.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        self.history.index = pd.to_datetime(self.history.date, unit='s', format="%Y%m%d")
        self.history.fillna(np.nan)
        self.history = self.history.reindex(index=self.history.index[::-1])

        # del self.history['date']

    def get_trades(self, vwap=False, fix_nmbr=False):

        if self.trades is None:
            self._get_trades()

        if vwap and 'vwap' not in self.trades.columns:
            self.vwap()

        if fix_nmbr:
            self.trades[(self.trades.buyer == 'NMBR') & (self.trades.seller != 'NMBR')].buyer = self.trades[
                (self.trades.buyer == 'NMBR') & (self.trades.seller != 'NMBR')].seller
            self.trades[(self.trades.seller == 'NMBR') & (self.trades.buyer != 'NMBR')].seller = self.trades[
                (self.trades.seller == 'NMBR') & (self.trades.buyer != 'NMBR')].buyer

        return self.trades

    def vwap(self):
        self.trades['vwap'] = (self.trades.volume * self.trades.price).cumsum() / self.trades.volume.cumsum()

    def get_broker_stats(self):
        """
        Columns: ['index', 'broker', 'sold', 'sold_trades', 'bought', 'bought_trades', 'total', 'positive']
        :return:
        """

        if self.broker_stats is None:
            self._get_broker_stats()

        return self.broker_stats

    def get_positions(self):
        """
        Columns: ['time', 'bid', 'bid_depth', 'bid_depth_total', 'ask', 'ask_depth', 'ask_depth_total']
        :return:
        """

        if self.positions is None:
            self._get_positions()

        return self.positions

    def get_history(self, mas=[], value='close'):
        """Returns ohlc and volume per trading day.
        Optional moving averages defined in the mas list will be applied and added as 'ma<window>' columns
        Columns: ['date', 'open', 'high', 'low', 'close', 'volume', 'turnover' (,'ma<window>', ...)]"""

        if self.history is None:
            self._get_history()

        if len(mas) > 0:
            for ma in mas:
                self.history['ma%i' % ma] = self.history[value].rolling(center=False, window=ma).mean()

        return self.history

    def get_ohlcv(self, interval='5min', vwap=False):
        """Returns a resampled dataframe for date with or without vwap
        Columns: ['time', 'open', 'high', 'low', 'close', 'volume' (,'vwap')]
        New resampler:
        ticks.Price.resample('1min').ohlc())
        """
        if self.trades is None:
            self._get_trades()

        if self.trades.shape[0] == 0:
            return None
            # tmp = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume']).set_index('time', inplace=True)
            # print(tmp)
            # return tmp

        vol = self.trades.volume.resample(interval).sum()  # resample(interval, how={'volume': 'sum'})
        vol.columns = pd.MultiIndex.from_tuples([('vol', 'volume')])

        price = self.trades.price.resample(interval).ohlc()  # resample(interval, how={'price': 'ohlc'})

        if vwap:
            if 'vwap' not in self.trades.columns:
                self.vwap()

            vwap_rs = self.trades.wvap.resample(interval).mean()  # .resample(interval, how={'vwap': 'mean'})
            vwap_rs.columns = pd.MultiIndex.from_tuples([('vwap_rs', 'vwap')])
            ohlcv = pd.concat([price, vol, vwap_rs], axis=1)

        else:
            ohlcv = pd.concat([price, vol], axis=1)

        # ohlcv.columns = ohlcv.columns.droplevel()

        # Fix nan's forward fill last close
        ohlcv = ohlcv.assign(close=ohlcv['close'].ffill()).bfill(axis=1)
        # Fix nan's volume to 0
        ohlcv.volume.fillna(0, inplace=True)

        ohlcv['time'] = pd.to_datetime(ohlcv.index.to_series())
        ohlcv = ohlcv.reindex_axis(['time', 'open', 'high', 'low', 'close', 'volume'], axis=1)

        return ohlcv
