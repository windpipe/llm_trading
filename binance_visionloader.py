import os
import csv
import datetime
import pandas as pd

from binance.client import Client
from binance.enums import HistoricalKlinesType


# -----------------------------------------------------------------------------
# 2) 바이낸스 데이터 수집 함수
# -----------------------------------------------------------------------------
def fetch_binance_futures_data(symbol, interval, start_str, end_str):
    """
    바이낸스 선물 kline (start_str~end_str, UTC 기준)을 DataFrame으로 반환.
    Columns: [symbol, timeframe, timestamp, open, high, low, close, volume]
    """
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

    klines = client.get_historical_klines(
        symbol,
        interval,
        start_str,
        end_str,
        klines_type=HistoricalKlinesType.FUTURES
    )

    if not klines:
        return pd.DataFrame()

    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
    ])

    # 필요한 컬럼만
    df = df[["open_time", "open", "high", "low", "close", "volume"]]
    df.rename(columns={"open_time": "timestamp"}, inplace=True)

    # timestamp -> datetime(UTC)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    df.insert(0, "symbol", symbol)
    df.insert(1, "timeframe", interval)
    return df
