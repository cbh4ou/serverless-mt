import json
from asyncio import gather, get_event_loop
from pprint import pprint
import ccxt.async_support as ccxt
from sqs_handler import *
from random import shuffle
import uuid

def lambda_handler(event, context):

    results = run()
    merged_pairs = []
    for x in results:
        merged_pairs = merged_pairs + x
    shuffle(merged_pairs)
    data= []
   
    for pair in merged_pairs:
        data.append({
        'Id' : str(uuid.uuid4),
        "MessageGroupId" : 'exchange-pair',
        "MessageBody" : json.dumps({ 
            "pair" : pair['pair'],
            "exchange": pair['exchange'],
            "stablecoin": pair['stablecoin']
            
        })})
               
    send_message(data)
    
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "pairs": merged_pairs,
            }
        ),
        "headers": {
                "Content-Type": "application/json"
            }
    }


async def load_markets(exchange):
    results = None

    try:
        await exchange.load_markets()
        print('Loaded', len(exchange.symbols), exchange.id, 'symbols')
        results = []
        for market in exchange.markets.values():
            print(market)
            quote = market['quote']
            base = market['base']
            if base in ['USD', 'USDT', 'MIM', 'UST', 'USDC']:
                continue
            elif quote in ['USD', 'USDT', 'MIM', 'UST', 'USDC']:
                print( market['symbol'])
                results.append({'exchange' : exchange.id, 'pair': base+'/'+quote, 'stablecoin' : True })
            elif quote in ["BTC", "ETH"]:
                print( market['symbol'])
                results.append({'exchange' : exchange.id, 'pair': base+'/'+quote, 'stablecoin' : False })
        if len(results) < 1:
            results = None
    except Exception as e:
        results = None
    await exchange.close()
    print(results)
    return results

async def main(looper):
    # Change exchangearr with GET from DynamoDB Lambda
    exchangesarr = ['gemini', 'binanceus','coinbasepro', 'binance', 'ftx', 'ftxus', 'kucoin'] 
    
    config = {'enableRateLimit': True, 'asyncio_loop': looper}
    exchanges = [getattr(ccxt, exchange_id)(config) for exchange_id in exchangesarr]

    results = await gather(*[load_markets(exchange) for exchange in exchanges])
    results = [result for result in results if result is not None]
    return results

def run():
    looper = get_event_loop()
    results = looper.run_until_complete(main(looper))
    return results

