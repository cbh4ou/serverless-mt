"""
Your module description
"""

def query_pairs():
    import boto3

    #Query to get top momentum pairs for an exchange
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('maintrade')
    
    response = table.scan(FilterExpression=Attr("quote").eq('UsT') & Attr("base").eq("BTC") & Attr("pk").eq("binance"))
    items = response['Items']
    
    while 'LastEvaluatedKey' in response:
        print(response['LastEvaluatedKey'])
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression=Attr("quote").eq('UT') & Attr("base").ne("BTC") & Attr("pk").gt(1))
        items.extend(response['Items'])
    pair_arr = [] 
    
    #Update all pairs and mark them as Stablecoin (1) if they are a stablecoin. Condition Quote exists  
    resp = requests.get('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=stablecoins&order=market_cap_desc&per_page=250&page=2&sparkline=false')
    j = json.loads(resp.content)
    print(j[0])
    
    
    return items