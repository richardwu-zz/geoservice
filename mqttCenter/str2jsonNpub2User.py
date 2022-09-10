import json, sys
import paho.mqtt.client as mqtt
import datetime

uname_xxx = sys.argv[1]	# for send back to who
#geoSrhRes = sys.argv[2]	# the geosearch result str to rework
txtFile = sys.argv[2]
srhType = sys.argv[3]

fr = open(txtFile, "r")
geoSrhRes = fr.read()
fr.close()


connBrokerIP = '127.0.0.1'
send2Topic = '/GeoXert/jobFinish'

def nowDateTime():
    myDateTime = datetime.datetime.now()
    return myDateTime.strftime("%Y-%m-%d %H:%M:%S")

#def filterNpub2user():
# parse keyword : 'BBOX' and dahiten 'features'
#fr = open("resultGeoSrh.txt", "r")
#geoSrhRes = fr.read()
#fr.close()

pos1 = geoSrhRes.find('BBOX')
pos2 = geoSrhRes.find('features', pos1)
reworkStr = geoSrhRes[pos1:pos2]
print(reworkStr)

lList = reworkStr.split('\n')
resList = []

for i in range(1, len(lList)-2, 1):
    #print(lList[i])
    tmpStr = lList[i]
    parseA = tmpStr.split('|')
    #print(parseA[2]+ ' , '+parseA[3])
    pos1=parseA[2].find('(')
    pos2=parseA[2].find(')')
    coord= parseA[2][pos1+1:pos2-1]
    #print(coord+ ' , '+parseA[3])
    parseB=coord.split(' ')
    print(parseB[0]+','+parseB[1]+','+parseA[3]+','+parseA[6])
    obj ={}
    obj["lat"] = parseB[0]
    obj["lon"] = parseB[1]
    obj['filename'] = parseA[6]+'/'+parseA[3]
    resList.append(obj)

#print(resList)
resListStr = json.dumps(resList)
wf = open("resultGeoSrh.json", "w")
wf.write(resListStr)
wf.close()

## andthen do mqtt publish ##
# initial JSON object
JMQTT = {}                          # JSON object inital
JMQTT["sender"]= "geoXpertSrv"      # publisher in mqtt
JMQTT["receive"] = uname_xxx    	# single subscriber in mqtt         
JMQTT["geoType"] = geoType          # pass info geo-search type:= geom-bbox
JMQTT["geoSrhRes"] = resListStr     # geomesa search result direct send back for realtime 
JMQTT["sendCurDT"] = nowDateTime()  # current datetime
JMQTT["qryResult"] = resListStr     # [{"lat":xxx,"lon":yyy,"filename":...},{"lat":xxx,"lon":yyy,"filename":...},{..}]
sendMsg=json.dumps(JMQTT)           # dumps to string

# connect mqtt broker
mqttc = mqtt.Client()
try {
    mqttc.connect(connBrokerIP)
    mqttc.publish(send2Topic, sendMsg)
    print("publish "+msgTopic+" msg:" +sendMsg)
} catch (MqttException e) {
    e.printStackTrace();
}

