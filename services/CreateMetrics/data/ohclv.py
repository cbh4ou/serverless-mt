import pandas as pd
from datetime import datetime
# -----------------------------------------------------------------------------
import ccxt  # noqa: E402
# -----------------------------------------------------------------------------

def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    num_retries = 0
    try:
        num_retries += 1
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
        return ohlcv
    except Exception as e:
        if type(e).__name__ == 'BadSymbol':
            print(type(e).__name__)
            print('Delete Symbol')
        else:
            print(type(e).__name__, str(e))
        


def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    now = exchange.milliseconds()
    all_ohlcv = []
    fetch_since = since
    while fetch_since < now:
        ohlcv = retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, fetch_since, limit)
        try:
            fetch_since = (ohlcv[-1][0] + 1) if len(ohlcv) else (fetch_since + timedelta)
        except:
            return 'error'
        all_ohlcv = all_ohlcv + ohlcv
    
    return exchange.filter_by_since_limit(all_ohlcv, since, None, key=0)


def scrape_candles( exchange_id, max_retries, symbol, timeframe, since, limit):
    # instantiate the exchange by id
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,  # required by the Manual
    })
    # convert since from string to milliseconds integer if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # preload all markets from the exchange
    exchange.load_markets()
    # fetch all candles
    ohlcv = scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit)
    df = pd.DataFrame(ohlcv, columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['Date'] = [datetime.fromtimestamp(float(time)/1000) for time in df['Date']]
    df.set_index('Date', inplace=True)
    return df