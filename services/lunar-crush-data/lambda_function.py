import json
import requests
import boto3
import time
from decimal import Decimal
def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('mt-coins')
    
    response = table.scan(
        ProjectionExpression="#b",
        ExpressionAttributeNames={"#b": "base"}
    )
    data = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'],
        ProjectionExpression="#b",
        ExpressionAttributeNames={"#b": "base"})
        data.extend(response['Items'])
       

    resp = requests.get("https://api2.lunarcrush.com/v2/assets?data=market&type=fast&key=5h39zshjp7u43smcph575j")
    lunar_data = json.loads(resp.content)
    lunar_list = sorted(lunar_data['data'], key=lambda d: d['mc'],reverse=True) 
    
    for coin in lunar_list:
        match = next((item for item in data if item["base"] == coin['s']), None)
        if match != None:
            response = table.update_item(
                Key={
                    'base': match['base']
                },
            UpdateExpression='SET #acr = :acr, #mc = :mc, #v = :v',
            ExpressionAttributeNames= {
                "#mc": "marketcap",
                '#acr' : 'alt_rank',
                '#v' : 'volume',
            },
            ExpressionAttributeValues={
                ':acr': coin['acr'],
                ':mc' : coin['mc'],  # coin['mc'] if coin['mc'] != None else 0
                ':v' : Decimal(str(coin['v']))
            },
        ReturnValues="UPDATED_NEW"
            )
            print(response)
        else:
            pass
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
