import jqdatasdk as jq
from datetime import timedelta, datetime
import datetime
from typing import List

from vnpy.trader.constant import Exchange, Interval
# from vnpy.trader.mddata.dataapi import MdDataApi
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.setting import SETTINGS
from typing import Callable

from pytz import timezone



CHINA_TZ = timezone("Asia/Shanghai")

INTERVAL_VT2JQ = {
    Interval.MINUTE: '1m',
    Interval.HOUR: '60m',
    Interval.DAILY: '1d',
}

INTERVAL_ADJUSTMENT_MAP_JQ = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()  # no need to adjust for daily bar
}



class JqdataDatafeed(BaseDatafeed):
    """聚宽JQData客户端封装类"""

    def __init__(self):

        """"""
        self.username = SETTINGS["datafeed.username"]
        self.password = SETTINGS["datafeed.password"]

        self.inited = False
        print("here")

    def init(self, username="", password=""):
        """"""
        if self.inited:
            return True

        if username and password:
            self.username = username
            self.password = password

        if not self.username or not self.password:
            return False

        try:
            jq.auth(self.username, self.password)
        except Exception as ex:
            print("jq auth fail:" + repr(ex))
            return False

        self.inited = True
        return True

    def query_bar_history(self, req: HistoryRequest,output: Callable):
        """
        Query history bar data from JQData.
        """
        # 检查是否登录
        if not self.inited:
            self.init()    
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        jq_symbol = jq.normalize_code(symbol)

        jq_interval = INTERVAL_VT2JQ.get(interval)
        if not jq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        # adjustment = INTERVAL_ADJUSTMENT_MAP_JQ.get(interval)
        adjustment = INTERVAL_ADJUSTMENT_MAP_JQ[interval]
        # For querying night trading period data
        end += timedelta(1)
        try:
            df = jq.get_price(
                jq_symbol,
                frequency=jq_interval,
                fields=["open", "high", "low", "close", "volume"],
                start_date=start,
                end_date=end,
                skip_paused=True,
            )
        except Exception as ex:
            output("jq get_price fail:" + repr(ex))

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=row.name.to_pydatetime() - adjustment,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    gateway_name="JQ"
                )
                data.append(bar)

        return data
