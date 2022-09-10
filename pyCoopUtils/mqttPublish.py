import paho.mqtt.client as mqtt
import sys
import json
import datetime

#usage python mqttPublish.py 124.11.164.192 test helloWorld
#brokerIP = sys.argv[1]
#msgTopic = sys.argv[2]
#sendData = sys.argv[3]

def nowDateTime():
    myDateTime = datetime.datetime.now()
    return myDateTime.strftime("%Y-%m-%d %H:%M:%S")

def doMqttSend(brokerIP, msgTopic, sendData):
    # initial JSON object
    JMQTT = {}        					#JSON object
    JMQTT["sendData"]= sendData         #measure in JSON                    
    JMQTT["sendCurDT"]= nowDateTime()   #datetime in JSON
    sendMsg=json.dumps(JMQTT)           #dumps to string

    mqttc = mqtt.Client()
    #mqttc.connect("192.168.1.166")
    mqttc.connect(connBrokerIP)

    mqttc.publish(msgTopic, sendMsg)
    print("publish "+msgTopic+" msg:" +sendMsg)
