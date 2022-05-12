import json
from metrics import atr, momentum, risk, ath, volume
from data import ohclv
import ccxt
from sqs_handler import send_message, send_messages
from portfolios import dual_momentum
import uuid
import queries 

def lambda_handler(event, context):
    if 'Records' in event:
        batch_arr = []
 
        for record in event['Records']:
            pair_resp = json.loads(record["body"])
            pair = pair_resp['pair']
            exchange = pair_resp['exchange']
            print(pair, exchange)
            try:
                print('in df step', pair + exchange)
                df = ohclv.scrape_candles( exchange_id = exchange, 
                max_retries=3, 
                symbol=pair,
                timeframe='1d', 
                since = '2011-11-19T00:00:00Z', 
                limit=700)
            except Exception as e:
                
                print( e, 'DF failure ' + pair + ' ' + exchange)
                continue
                    
            if len(df.index) < 7 and len(df.index) < 30:
                continue
            base_quote = pair.split("/")     
            cex = getattr(ccxt, exchange)()
            try:
                ticker = cex.fetch_ticker(pair)
            except Exception as e:
                print( e, 'Ticker failure ' + pair + ' ' + exchange)
                continue
            
            if ticker['last'] == None:
                price = 0
            else:
                price = float(ticker['last'])
            resp = {
                "exchange" : exchange,
                "pair" : pair,
                "base" : base_quote[0],
                "quote" : base_quote[1],
                "atr": atr.get_atr(df),
                "momentum" : price if price == 0 else momentum.get_momentum(df, price),
                "until_ath": price if price == 0 else ath.get_ath(df,price),
                "risk" : price if price == 0 else risk.get_risk(df, price),
                "volume" : volume.get_volume(ticker),
                "price" : price,
                "past_yearly_ath": 0 if price == 0 else ath.get_yearly(df, price),
                "daily_candles" : len(df.index),
                "stablecoin" : 'x' if pair_resp['stablecoin'] == True else 'o'
            }
            print(resp)
            batch_arr.append({
            'Id' : str(uuid.uuid1()),
            "MessageGroupId" : f'{exchange}-{pair}',
            "MessageBody" : json.dumps(resp)
            })
          
        send_messages(batch_arr)   
        return {
            'statusCode': 200,
            'body': json.dumps({"data" : 'success'})
            }
    
        
    elif 'metrics' in event: 
    
        pair = event['metrics']["pair"].upper()
        exchange = event["exchange"].lower()
        
        # Gets the new Pair and Exchange based on Highest Candles
        exchange_result = queries.get_exchange(pair)
        exchange, pair = exchange if exchange_result == None else exchange_result
        
        print(pair, exchange)
        
        try:
            df = ohclv.scrape_candles( exchange_id = exchange, 
            max_retries=3, 
            symbol=pair,
            timeframe='1d', 
            since = '2011-11-19T00:00:00Z', 
            limit=700)
        except Exception as e:
            print( e, 'DF failure ' + pair + ' ' + exchange)
            return 
        
        cex = getattr(ccxt, exchange)()
        ticker = cex.fetch_ticker(pair)
        base_quote = pair.split("/")
        print(ticker)
        
        if ticker['last'] == None:
            price = 0
        else:
            price = float(ticker['last'])
        resp = {
                "exchange" : exchange,
                "pair" : pair,
                "base" : base_quote[0],
                "quote" : base_quote[1],
                "atr": atr.get_atr(df),
                "momentum" : price if price == 0 else momentum.get_momentum(df, price),
                "until_ath": price if price == 0 else ath.get_ath(df,price),
                "risk" : price if price == 0 else risk.get_risk(df, price),
                "volume" : volume.get_volume(ticker),
                "price" : price,
                "past_yearly_ath": 0 if price == 0 else ath.get_yearly(df, price),
                "daily_candles" : len(df.index)
            }
        print(resp)
        return { 
         "metrics" : resp
            }
 
    elif 'portfolio' in event:
        #resp = dual_momentum.query_pairs()
        print(resp)
    
        return { 
         "portfolio" : resp
            }
 
    return {
            'statusCode': 500,
            'body': json.dumps({"data" : 'Failed Checks'})
            }