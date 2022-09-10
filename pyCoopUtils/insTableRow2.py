# start command first : hbase thrift start-port:9090
# here ref. website: https://happybase.readthedocs.io/en/latest/api.html
#
# usage: python3 insTableRow.py geoXpert jsonFileName <~ rewrite to function call.
import os.path
import happybase
from thriftpy2 import thrift
import json
import random
import sys
from datetime import datetime

dictFile = sys.argv[1]
fileName = os.path.basename(dictFile)
theUser = sys.argv[2]

# readin curDate tokenUser_yyyy-MM-dd.json for parse username & password
userInfo = []
absoPath='/home/stdb/richard/ftpServer1/loginLog'
curDateStr = datetime.today().strftime('%Y-%m-%d')
loginJsonFile = 'tokenUser_'+curDateStr+'.json'
fr = open(absoPath+'/'+loginJsonFile, "r")
userInfoStr = fr.read()
fr.close()
userInfo = json.loads(userInfoStr)

def takeUserIDback(theToken):
    retStr = ''
    idx = 0
    stopF = False
    while idx< len(userInfo) and stopF == False:
        cmpObj = userInfo[idx]
        #print(theToken+ ' <?> '+cmpObj["token"])
        if theToken == cmpObj["token"]:
            stopF = True
            retStr = cmpObj["username"]+'_'+cmpObj["password"][-3:]
        else:
            idx += 1
    return retStr
# add end

def takeBackFamCol(chkStr, key):
    retStr = ''
    parseA = chkStr.split('@')
    stopF = False
    idx = 0
    while idx < len(parseA) and stopF == False:
        if parseA[idx].find(key) != -1:
            stopF = True
            retStr = parseA[idx]
        else:
            idx += 1
    return retStr

def insRowAction(theTblName, jsonObjStr, dictFileName, uname_xxx):
    tmpDataDict = json.loads(jsonObjStr)
    print(tmpDataDict)
    ch1 = theTblName[:1]
    #rdm = random.randint(1000, 2000)
    if ch1 == 't':
        taskID = dictFileName[5:-5]	# take front task_ and tail .dict away
        print(taskID)
        #print(tmpDataDict["userid"])
        #uname_xxx = takeUserIDback(tmpDataDict["userid"])
        keyWord = 't-' + uname_xxx + '-'+taskID
        chkColStr = 'taskInfo1:taskid@taskInfo2:taskname@taskInfo2:tasktype@taskInfo3:userdata@taskInfo3:polygon@taskInfo4:userid@taskInfo4:orderdt'
    else:
        if ch1 == 'u':
            #rdm = random.randint(3000, 4000)
            chkColStr = 'userCls1:userid@userCls1:username@userCls2:email@userCls3:password'
        else:
            # ch1 = 'd'
            #rdm = random.randint(6000, 7000)
            chkColStr = 'dataGrp1:taskid@dataGrp1:userid@dataGrp2:datatype@dataGrp2:filename@dataGrp3:obname@dataGrp3:tasktype@dataGrp4:timestamp@dataGrp4:latlong@dataGrp4:msc@dataGrp5:notice'
    #print("Random number between 1000 and 7000 is % s" % (rdm))
    
    # for moment, portal no create identity yet ! <~ now update to t-uname_xxx_taskid -- 20220621
    print('----------')
    print(keyWord)
    print('----------')
    # do mutate cols insert action ~ this do lately
    # do check connect server
    conn = happybase.Connection('localhost', 9070)
    conn.open()
    #print(conn.tables())
    bRowKey = bytes(keyWord,'UTF-8')
	# action in table which user want
    theTblBytes = bytes(theTblName,'UTF-8')
    workTable = conn.table(theTblBytes)
    
    insDataDict = {}
    for k, v in tmpDataDict.items():
        print(k, v)
        # take uname_xxx out -- 20220621
        if k == 'userid':
            v = uname_xxx
        
        # if k in chkColStr then insert it, otherwise bypass
        if chkColStr.find(k) != -1:
            theFC = takeBackFamCol(chkColStr, k)
            print('found: '+theFC)
            #insDataDict[bytes(theFC,'UTF-8')] = bytes(v,'UTF-8')
            workTable.put(bRowKey,{bytes(theFC,'UTF-8'):bytes(v,'UTF-8')})
        else:
            print('not found')
    print('---------')
    # print out for check
    for k2, v2 in insDataDict.items():
        print(k2, v2)
    # do action insert
    #workTable.put(bRowKey, insDataDict)
    print('insert down')
    # take it back for confirm
    
    conn.close()
    
if dictFile.find('user') >= 0:
    tblName = 'users'
if dictFile.find('task') >= 0:
    tblName = 'task'
if dictFile.find('data') >= 0:
    tblName = 'data'

with open(dictFile, 'r') as fr:
    dataStr = fr.read()

insRowAction(tblName, dataStr, fileName, theUser)
