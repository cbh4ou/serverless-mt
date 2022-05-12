import os
import logging
import boto3 
import time
logging = logging.getLogger()

QUEUE_NAME = os.getenv("QUEUE_NAME")
SQS = boto3.client("sqs")

def getQueueURL():
    """Retrieve the URL for the configured queue name"""
    q = SQS.get_queue_url(QueueName=QUEUE_NAME).get('QueueUrl')
    print("Queue URL is %s", q)
    return q

def send_message(data):
    u = getQueueURL()
    batch_list = []
    for message in data:
        batch_list.append(message)
        if len(batch_list) == 10: 
            try:
                print("Got queue URL %s", u)
                resp = SQS.send_message_batch(QueueUrl=u,Entries=batch_list)
                print("Send result: %s", resp)
                batch_list *= 0
            except Exception as e:
                raise Exception("Could not record link! %s" % e)
                