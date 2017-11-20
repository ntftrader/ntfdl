from ntfdl import Dl
import pandas as pd
import datetime


class Multi():
    def __init__(self, instrument, exchange='OSE'):

        self.instrument = instrument
        self.exchange = exchange

    def _daterange(self, start_date, end_date):
        """Date generator"""
        if start_date <= end_date:
            for n in range((end_date - start_date).days + 1):
                yield start_date + datetime.timedelta(n)
        else:
            for n in range((start_date - end_date).days + 1):
                yield start_date - datetime.timedelta(n)

    def _dates(self, start, end):

        if isinstance(start, datetime.datetime) and isinstance(end, datetime.datetime):
            pass
        else:
            start = datetime.datetime.strptime(start, '%Y%m%d')
            end = datetime.datetime.strptime(end, '%Y%m%d')

        return start.date(), end.date()

    def get_trades(self, start, end):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date)

                data = i.get_trades()

                if isinstance(data, pd.DataFrame):
                    if 'trades' in locals() and isinstance(trades, pd.DataFrame):
                        trades = trades.append(data)
                    else:
                        trades = data

        return trades

    def get_ohlcv(self, start, end, interval='5min'):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date, download=True)

                data = i.get_ohlcv(interval)

                if isinstance(data, pd.DataFrame):
                    if 'ohlcv' in locals() and isinstance(ohlcv, pd.DataFrame):
                        ohlcv = ohlcv.append(data)
                    else:
                        ohlcv = data

        return ohlcv

    def get_positions(self, start, end):
        """Uses dl to download and appends each day with data"""

        start, end = self._dates(start, end)

        for date in self._daterange(start, end):

            if date.isoweekday() not in [6, 7]:
                # print(date.__format__("%Y%m%d"))
                i = Dl(self.instrument, self.exchange, day=date)

                data = i.get_positions()

                if isinstance(data, pd.DataFrame):
                    if 'positions' in locals() and isinstance(positions, pd.DataFrame):
                        positions = positions.append(data)
                    else:
                        positions = data

        return positions
