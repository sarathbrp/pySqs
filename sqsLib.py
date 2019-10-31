import boto3
import json
from botocore.exceptions import ClientError


class SQS(object):
    def __init__(self, region, queue):
        self._sqs = boto3.resource("sqs", region_name=region)
        self._client = boto3.client("sqs", region_name=region)
        self._queue = self._sqs.Queue(queue)
        self._url = queue

    def debug(self):
        print(self._queue)

    def send_message_to_queue(self, qMsg):
        return self._client.send_message(
            QueueUrl=self._url,
            MessageBody=qMsg
        )

    def sendMessages(self, messages, pid=""):
        #print("sending messages")
        # create body
        entries = []
        for i in range(len(messages)):
            entry = {
                "Id": str(pid) + "_" + str(i),
                "MessageBody": json.dumps(messages[i]),
            }
            entries.append(entry)
        self._queue.send_messages(Entries=entries)
       
    def getMessages(self, count):
        #print("retrieving messages via long poll")
        ret = []
        try:
            response = self._queue.receive_messages(
                AttributeNames=["All"], MaxNumberOfMessages=count, WaitTimeSeconds=10
            )
        except ClientError as e:
            #print("recv errored")
            print(e.response["Error"]["Message"])
            print(e.response["Error"]["Code"])
            return None
        else:
            #print("got messages")
            for msg in response:
                ret.append(msg.body)
                msg.delete()
            #print("end recv")
            return ret

    def getMessagesBoto(self, count):
        ret = []
        queued_messages = self._client.receive_message(
            QueueUrl=self._url,
            MaxNumberOfMessages=count,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=10
        )

        if 'Messages' in queued_messages and len(queued_messages['Messages']) >= 1:
            for message in queued_messages['Messages']:
                ret.append(message['Body'])
                receipt_handle = message['ReceiptHandle']
                self._client.delete_message(QueueUrl=self._url, ReceiptHandle=receipt_handle)
            return ret
        else:
            return None
