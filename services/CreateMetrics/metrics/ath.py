import pandas as pd
import numpy as np
import math as mth

def get_ath(df, price):
    one = float(price)
    peak = df['Close'].max()
 
    if mth.isinf(one) or one is None:
        ath_pct = 0
   
    if (one < peak):
        ath_pct = ((peak - one) / one) * 100
   
    else:

        ath_pct = 0
        
    if mth.isinf(ath_pct):
    
        ath_pct = 0
    return ath_pct
    
def get_yearly(df, price):
    
    if len(df.index) > 366:
        df = df.iloc[:-1]
        df = df.tail(365)
        resp = 1 if df['Close'].max() < price else 0
        return resp
    else:
        return 1