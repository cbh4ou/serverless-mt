import pandas as pd

def wwma(values, n):
    return values.ewm(alpha=1/n,ignore_na=True, adjust=False).mean()

def get_atr(df, n=12):
    data = df.copy()
    high = data['High'].astype(float)
    low = data['Low'].astype(float)
    close = data['Close'].astype(float)
    data['tr0'] = abs(high - low)
    data['tr1'] = abs(high - close.shift())
    data['tr2'] = abs(low - close.shift())
    tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
    atr = wwma(tr, n)
    atr = atr/close
    if len(atr.index) < 1:
        return(0)
    return atr.iloc[-1]