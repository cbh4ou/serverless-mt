import json
import boto3
from decimal import Decimal
from datetime import datetime
import time 
import boto3.dynamodb.conditions as conditions
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('maintrade')
    table_data = []
    for record in event['Records']:
     
        dumped_json = record['body']
        metrics_dict = json.loads(dumped_json, parse_float=Decimal)

        print(metrics_dict)
        pair = metrics_dict['pair']
        print(pair)
        if '/' not in pair:
            continue
        stablecoin = metrics_dict['stablecoin']
        print(stablecoin)
        exchange = metrics_dict['exchange']
        risk = metrics_dict['risk']
        volume = metrics_dict['volume']
        momentum = metrics_dict['momentum']
        until_ath = metrics_dict['until_ath']
        atr = metrics_dict['atr']
        past_yearly_ath = metrics_dict['past_yearly_ath']
        daily_candles = metrics_dict['daily_candles']
        
        # Split Pair To Base and Quote
        base_quote = pair.split("/")
        base = base_quote[0]
        quote = base_quote[1]
        try: 
            metric_pair = table.put_item(
                Item={
                    'pk' : exchange,
                    'sk' : pair,
                    'base' : base,
                    'quote' : quote,
                    'momentum' : momentum,
                    'risk' : risk,
                    'exchange' : exchange,
                    'metrics': {
                        'volume' : volume,
                        'alt_rank': 0,
                        'until_ath': until_ath,
                        "atr12": atr,
                        "past_yearly_ath": past_yearly_ath,
                        "marketcap" : 0
                        },
                    "daily_candles": daily_candles,
                    "last_updated_string" : datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'),
                    "last_updated_unix" : int( time.time() ),
                    "stablecoin" : stablecoin
                        
                    }
                )
        except Exception as e:
            print(e)
            return {
            'statusCode': 200,
            'body': json.dumps('Item Exists')
                 }
            
            
        
        # if stablecoin == True:
        #     response = table.update_item(
        #     Key={
        #     'year': year,
        #     'title': title
        #     },
        #     UpdateExpression="set metrics.marketcap=:mc, metrics.alt_rank=:acr, metrics.volume",
        #     ExpressionAttributeValues={
        #         ':v' : volume,
        #         ':acr': 0,
        #         'until_ath': until_ath,
        #         "atr12": atr,
        #         "past_yearly_ath": past_yearly_ath,
        #         ":mc" : 0
        #     },
        #     ReturnValues="UPDATED_NEW"
        #     )
        #     print(metric_pair)
                
        # elif stablecoin == False:
        #     response = table.update_item(
        #     Key={
        #     'pk': base,
        #     'sk': False
        #     },
        #     UpdateExpression="set info.rating=:r, info.plot=:p, info.actors=:a",
        #     ExpressionAttributeValues={
        #         ':r': Decimal(rating),
        #         ':p': plot,
        #         ':a': actors
        #     },
        #     ReturnValues="UPDATED_NEW"
        #     )
        #     print(metric_pair)
        
    
    return {
        'statusCode': 200,
        'body': json.dumps('Item ')
    }
