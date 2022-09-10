import os, sys
import json
import paho.mqtt.client as mqtt
import datetime

tblName = sys.argv[1]   # for prepare to future feature
params = sys.argv[2]
parseA = params.split('@')
uname_xxx = parseA[0] # for send back to who
srhType = parseA[1]


savePath = '/home/stdb/richard/ftpServer1/geoSearch'
saveResFile = 'resultGeoSrh.txt'

# then run the geomesa runtime command:
hisCatalog = srhType+'_'+uname_xxx
wrPathFile = savePath+'/'+uname_xxx+'/'+saveResFile

qryGeoCmdStr = "java -cp /home/stdb/richard/geomesa-tutorials-hbase-quickstart-3.5.0-SNAPSHOT.jar org.geomesa.example.hbase.HBaseQuickStart "
qryGeoCmdStr = qryGeoCmdStr+"--hbase.zookeepers localhost --hbase.catalog "+hisCatalog+' '+ uname_xxx + ' > '+ wrPathFile
os.system(qryGeoCmdStr)
#print(execResStr)

fr = open(wrPathFile, "r")
geoSrhRes = fr.read()
fr.close()

connBrokerIP = '127.0.0.1'
#send2Topic = '/GeoXpert/jobFinish' -- old version
send2Topic = '/GeoXpert/'+uname_xxx+'/jobFinish'

def nowDateTime():
    myDateTime = datetime.datetime.now()
    return myDateTime.strftime("%Y-%m-%d %H:%M:%S")

pos1 = geoSrhRes.find('BBOX')
pos2 = geoSrhRes.find('features', pos1)
reworkStr = geoSrhRes[pos1:pos2]
#print(reworkStr)

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
    #print(parseB[0]+','+parseB[1]+','+parseA[3]+','+parseA[6])
    obj ={}
    obj["lng"] = parseB[0]
    obj["lat"] = parseB[1]
    obj['filename'] = parseA[6]+'/'+parseA[3]
    resList.append(obj)
    
## print back first ##
#print(resList)

# write back in file
resListStr = json.dumps(resList)
qryResFileName = "resultGeoSrh.json"
wf = open(savePath+'/'+uname_xxx+'/'+qryResFileName, "w")
wf.write(resListStr)
wf.close()

## and then do mqtt publish ##
# initial JSON object
JMQTT = {}                          # JSON object inital
JMQTT["sender"]= "geoXpertSrv"      # publisher in mqtt
JMQTT["receive"] = uname_xxx        # single subscriber in mqtt         
JMQTT["geoType"] = srhType          # pass info geo-search type:= geom-bbox
JMQTT["sendCurDT"] = nowDateTime()  # current datetime
JMQTT["qryResult"] = resListStr     # [{"lat":xxx,"lon":yyy,"filename":...},{"lat":xxx,"lon":yyy,"filename":...},{..}]
sendMsg=json.dumps(JMQTT)           # dumps to string

## print back first ##
print(sendMsg)

# connect mqtt broker
mqttc = mqtt.Client()

try:
    mqttc.connect(connBrokerIP)
    mqttc.publish(send2Topic, sendMsg)
    #print("publish "+send2Topic+" msg:" +sendMsg)
    mqttc.disconnect()
except:
    print('mqtt connect Broker fail')

