import pandas as pd


def get_volume(ticker):
    
    volume = ticker['baseVolume']
    if volume == None:
        return None
    if ticker['last'] == None:
            price = 0
    else:
        price = float(ticker['last'])
        
    # Send Values Back to Task to save metrics
  
    return round(price * volume,2)
    