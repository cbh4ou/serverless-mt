import pandas as pd
from datetime import datetime, date
import numpy as np
def get_risk(df, price):
    
    
    df = df.append({'Date': date.today(), 'Close': price}, ignore_index=True)

    df['Close'] = df['Close'].astype(float)

    df['date'] = pd.to_datetime(df['Date'])

    # Calculate the rolling moving average, rolling standard deviation, and z-score
    df['4MA'] = df['Close'].rolling(window=1400, min_periods=1).mean()
    df['STD2'] = (df['Close'].rolling(window=1400, min_periods=1).std())
    df['z'] = (df['Close'] - df['4MA'])/df['STD2']
    df['z'].replace([np.inf, -np.inf, np.nan], 0, inplace=True)

    # Normalized values
    normalized_df = (df['z']-df['z'].min())/(df['z'].max()-df['z'].min())

    return normalized_df.iloc[-1]