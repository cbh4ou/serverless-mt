import json
import boto3
from operator import itemgetter
import time

# def make_update(coin, masterlist, exchanges, quotes):
#     dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
#     table = dynamodb.Table('mt-coins')
#     response = table.update_item(
#         Key={
#         'base': coin
#         },
#         UpdateExpression='SET exchanges = :cex, #met = :met, quotes = :quotes',
#         ExpressionAttributeNames ={
#             '#met' : "metrics"
#         },
#         ExpressionAttributeValues={
#             ':met': masterlist, 
#             ':cex' : exchanges,
#             ':quotes' : quotes
#             },
#         ReturnValues="UPDATED_NEW"
#             )
    
def make_update(coin, masterlist, exchanges, quotes):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('mt-coins')
    response = table.update_item(
        Key={
        'base': coin
        },
        UpdateExpression='SET exchanges = :cex, #met = :met, quotes = :quotes',
        ExpressionAttributeValues={
            ':cex' : exchanges,
            ':quotes' : quotes,
            ':met' : masterlist
            },
        ExpressionAttributeNames ={
            '#met' : "metrics"
        },
        ReturnValues="UPDATED_NEW"
            )   
    
def clean_up(coin, data_set):
    master_list = {}
    supported_quotes = []
    # Gets All Rows for one Base, then gets all Unique exchanges associated 
    exchanges = list(filter(lambda item: item['base'] == coin, data_set))
    supported_exchanges = [item['pk'] for item in {v['pk']:v for v in exchanges}.values()]
    # Filters Data Set by Base and Quote
    filtered_coins = list(filter(lambda item: item['base'] == coin and item['stablecoin'] == 'x', data_set))
    stable_quotes = sorted(filtered_coins, key=itemgetter('daily_candles'), reverse=True)
    
    try:
        master_list.update({'USD' : stable_quotes[0]})
        supported_quotes.append('USD')
    except:
        pass
    
    filtered_coins = list(filter(lambda item: item['base'] == coin and item['stablecoin'] == 'o', data_set))
    quote_list = [item['quote'] for item in {v['quote']:v for v in filtered_coins}.values()]
    
    for quote in quote_list:
        nonstable = list(filter(lambda item: item['quote'] == quote, filtered_coins))
        nonstable_quotes = sorted(nonstable, key=itemgetter('daily_candles'), reverse=True)
        master_list.update({quote : nonstable_quotes[0]})
        supported_quotes.append(quote)
    
    make_update(coin,master_list,supported_exchanges, supported_quotes)
    
    
def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('maintrade')
    
    response = table.scan()
    query_data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        query_data.extend(response['Items'])
    
    returns = {v['base']:v for v in query_data}.values()
    coins = [item['base'] for item in returns]
    
    [clean_up(coin, query_data) for coin in coins]
        
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
