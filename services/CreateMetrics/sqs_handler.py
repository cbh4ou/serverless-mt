import os
import logging
import boto3 
import json
logging = logging.getLogger()

QUEUE_NAME = os.getenv("QUEUE_NAME")
SQS = boto3.client("sqs")

def getQueueURL():
    """Retrieve the URL for the configured queue name"""
    q = SQS.get_queue_url(QueueName=QUEUE_NAME).get('QueueUrl')
    #print("Queue URL is %s", q)
    return q

                # "event" : 'put',
                # "exchange" : exchange,
                # "pair" : pair,
                # "base" : base_quote[0],
                # "quote" : base_quote[1],
                # "atr": atr.get_atr(df) ,
                # "momentum" : momentum.get_momentum(df, float(ticker['last'])),
                # "until_ath": ath.get_ath(df,float(ticker['last'])),
                # "risk" : risk.get_risk(df, float(ticker['last'])),
                # "volume" : volume.get_volume(ticker),
                # "past_yearly_ath": ath.get_yearly(df, ticker)
            

def send_messages(batch):
    u = getQueueURL()
    batch_list = []
    for message in batch:
        batch_list.append(message)
        if len(batch_list) == 10 or len(batch) == len(batch_list):
            try:
                resp = SQS.send_message_batch(QueueUrl=u,Entries=batch_list)
                batch_list *= 0
                print("Send result: %s", resp)
            except Exception as e:
                raise Exception("Could not record link! %s" % e)
                
def send_message(batch):
    u = getQueueURL()
    batch_list = []
    for message in batch:
        batch_list.append(message)
        if len(batch_list) == 1:
            try:
                resp = SQS.send_message_batch(QueueUrl=u,Entries=batch_list)
                batch_list *= 0
                print("Send result: %s", resp)
            except Exception as e:
                raise Exception("Could not record link! %s" % e)