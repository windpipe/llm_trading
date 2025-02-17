from binance.client import Client
from binance.enums import HistoricalKlinesType

import pandas as pd
import datetime

def get_ohlcv_data(ticker, interval, end_time, api_key="", api_secret=""):
    """
    Returns the last 60 OHLCV data points as a Pandas DataFrame for a given ticker and interval, ending at a specified time.

    Args:
        ticker (str): The ticker symbol (e.g., "BTCUSDT").
        interval (str): The candlestick interval (e.g., '1m', '5m', '1h', '1d'). See Binance API documentation for valid intervals.
        end_time (str or datetime): The end time for the data. Can be a string in 'YYYY-MM-DD HH:MM:SS' format or a datetime object.
        api_key (str, optional): Your Binance API key. Defaults to "".
        api_secret (str, optional): Your Binance API secret key. Defaults to "".

    Returns:
        pandas.DataFrame: A Pandas DataFrame containing the OHLCV data with columns ['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'].
                           Returns an empty DataFrame if there is an error or no data is found.

    Raises:
        Exception: If there's an issue with the Binance API request.
    """

    client = Client(api_key=api_key, api_secret=api_secret)

    try:
        # Calculate start time based on the interval and number of data points
        if isinstance(end_time, str):
            end_time_dt = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        else:
            end_time_dt = end_time # Assume datetime object

        # Calculate the start time based on the desired number of candles and interval.
        if interval == '1m':
            time_delta = datetime.timedelta(minutes=60) # approx 60 one minute candles
        elif interval == '5m':
            time_delta = datetime.timedelta(minutes=60 * 5) # approx 60 five minute candles
        elif interval == '1h':
            time_delta = datetime.timedelta(hours=60) # approx 60 one hour candles
        elif interval == '1d':
            time_delta = datetime.timedelta(days=60) # approx 60 one day candles
        else:
            raise ValueError("Unsupported interval.  Must be '1m', '5m', '1h', or '1d'.")


        start_time_dt = end_time_dt - time_delta
        start_time_str = start_time_dt.strftime('%Y-%m-%d %H:%M:%S')


        # Fetch historical klines using start_str and end_str
        klines = client.get_historical_klines(
            symbol=ticker,
            interval=interval,
            start_str=start_time_str,
            end_str=end_time_dt.strftime('%Y-%m-%d %H:%M:%S'),  # Convert to string format
            limit=60,
            klines_type= HistoricalKlinesType.FUTURES
        )


        if not klines:
            print(f"No data found for {ticker} with interval {interval} ending at {end_time}")
            return pd.DataFrame()  # Return an empty DataFrame if no data is found

        # Create a Pandas DataFrame
        df = pd.DataFrame(klines, columns=['Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'])

        # Convert numeric columns to appropriate data types
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col])

        df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
        df['Close Time'] = pd.to_datetime(df['Close Time'], unit='ms')

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame() # Return an empty DataFrame in case of error


if __name__ == '__main__':
    # Example usage
    api_key = ""  # Replace with your actual API key
    api_secret = ""  # Replace with your actual API secret

    ticker = "BTCUSDT"
    interval = "1h"
    end_time = "2023-12-25 00:00:00" # Example end time

    df = get_ohlcv_data(ticker, interval, end_time, api_key, api_secret)

    if not df.empty:
        print(df)
        print(df.dtypes)
    else:
        print("No data retrieved or an error occurred.")

    # Example with datetime object
    end_time_dt = datetime.datetime(2023, 12, 26, 12, 0, 0)
    df2 = get_ohlcv_data(ticker, interval, end_time_dt, api_key, api_secret)
    if not df2.empty:
      print("Dataframe with datetime object")
      print(df2)
