from kafka import KafkaProducer
from json import dumps
from time import sleep
import requests

response = requests.get('http://localhost:9090/api/v1/label/__name__/values')
metrixNames = response.json()['data']


def json_serializer(data):
    return dumps(data).encode("utf-8")

def getMetrics():
    metricsPerTick = {}
    datastampFlag = True
    for metricName in metrixNames:
        req = requests.get('http://localhost:9090/api/v1/query', params={'query': metricName})
        metrics = req.json()['data']['result'][0]
        if metrics['metric']['job'] == 'alekseyarkhipov':
            if datastampFlag:
                datastampFlag = False
                metricsPerTick['timestamp'] = metrics['value'][0]

            metricsPerTick[metrics['metric']['__name__']] = metrics['value'][1]

    return metricsPerTick

# print(getMetrics())

producer =  KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)

while(True):
    metrics = getMetrics()
    print(metrics)
    producer.send('spark-topic', metrics)
    sleep(10)