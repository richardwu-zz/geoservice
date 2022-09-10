import os, sys
import json
import paho.mqtt.client as mqtt
#import str2jsonNpub2user
import time

#-- call os.system cmdStr to query geomesa dataStore --
#chDirCmd = 'cd /home/stdb/richard'
#os.system(chDirCmd)

geoQryFilePath = '/home/stdb/richard/ftpServer1/mqttCenter'

savePath = '/home/stdb/richard/ftpServer1/geoSearch'
saveResFile = 'resultGeoSrh.txt'

# in moment only one topic
listenTopic = '/GeoXpert/+/jobBegin'

# The callback for when the mqttc receives a CONNACK response from the server.
def on_connect(mqttc, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    mqttc.subscribe(listenTopic)
    print("subscribe topic: "+listenTopic)

# event handler for mqtt subscribe
def on_message(mqttc, userdata, msg):
    # when package incoming ...
    recvByte = msg.payload
    msg2Str = recvByte.decode("utf-8")
    print(msg.topic+" -> "+msg2Str)
    recvMsgObj = json.loads(msg2Str)
    uname_xxx = recvMsgObj["sendUser"]
    srhType = recvMsgObj["geoType"]     # in moment hardcoding! = geom-bbox
    #qryParams = recvMsgObj["geoType"] <-- info in txt file

    print(uname_xxx+' '+srhType)
    # then run the geomesa runtime command:
    cmdStr = "python3 "+geoQryFilePath+"/"+"dataStoreGeomQry.py "+uname_xxx+" "+srhType
    print(cmdStr)
    os.system(cmdStr)

    # from str to json and then pub 'finish' to user
    #wrPathFile = savePath+'/'+uname_xxx+'/'+saveResFile
    #str2jsonNpub2user.filterNpub2user(execRes, geoType) <-- this is bad for system update!
    #cmdStr = "python3 str2jsonNpub2user.py "+uname_xxx+" "+srhType+" "+wrPathFile
    #print(cmdStr)
    #os.system(cmdStr)

 
mqttc = mqtt.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message
# https://test.mosquitto.org/
#mqttc.connect("test.mosquitto.org", 1883, 60)
mqttc.connect("38.242.216.125", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a manual interface.
mqttc.loop_forever()


