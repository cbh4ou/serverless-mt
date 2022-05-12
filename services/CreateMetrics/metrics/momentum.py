import pandas as pd


def get_momentum(df, last_price):
    if len(df.index) < 7 or len(df.index) < 30 or last_price == None :
        return 0
    print(len(df.index), last_price)
    weight_1 = (df.tail(6).head(1)['Close']).values[0]
    weight_2 = (df.tail(10).head(1)['Close']).values[0]
    weight_3 = (df.tail(26).head(1)['Close']).values[0]
    score = (last_price/weight_1 * .60 + last_price/weight_2 * .20 + last_price/weight_3 * .20)
    
    # Send Values Back to Task to save metrics
    if score == None:
        return 0
    else:
        return float(score)