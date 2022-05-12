import pandas
import boto3 
import boto3.dynamodb.conditions as conditions
from boto3.dynamodb.conditions import Key, Attr
import requests, json
from operator import itemgetter

def get_exchange(pair):
    #Query to get top momentum pairs for an exchange
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('maintrade')
    base_quote = pair.split("/")
    #print(base_quote)
    base = base_quote[0]
    quote = base_quote[1]
    if base_quote[1] in ['USD', 'USDT', 'MIM', 'UST', 'USDC']:
        response = table.scan(FilterExpression=Attr("stablecoin").eq('x')  & Attr("base").eq(base))
        items = response['Items']
        print(items)
    else:
        response = table.scan(FilterExpression=Attr("quote").eq(quote) & Attr("base").eq(base))
        items = response['Items']

    if items == None:
        return None
    else: 
        most_candles = max(items, key=lambda x:x['daily_candles'])
        return (most_candles['exchange'], base+ '/' +most_candles['quote'])
    
    
    # response = table.scan(FilterExpression=Attr("quote").eq('UsT') & Attr("base").eq("BTC"))
    # items = response['Items']
    # while 'LastEvaluatedKey' in response:
    #     print(response['LastEvaluatedKey'])
    #     response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=Attr("quote").eq('UT') & Attr("base").ne("BTC") & Attr("pk").gt(1))
    #     items.extend(response['Items'])
        
        
        
   