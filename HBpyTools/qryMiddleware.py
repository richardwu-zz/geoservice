# http://localhost:16010/ is HBase Web UI
# sudo $HBASE_HOME/bin/start-hbase.sh	<~ start HBase frist
# hbase thrift start -p <port> --infoport <infoport>
# ex. sudo $HBASE_HOME/bin/hbase-daemon.sh restart thrift -p 9070 --infoport 9020
# attension please !!! the content use python3 entry and line by line is fail by read data.

import happybase
from thriftpy2 import thrift

import sys

tableName = 'data'
uName_xxx = 'dummyStr'
param1 = 'dummyStr@geom-bbox'

defColsArr=['filename','latitude','longitude','GPSLatitude','GPSLongitude','GPSAltitude','ExifImageWidth','ExifImageHeight','DateTime','timeStamp','imgfilePath']
defColsStr= '\t'.join(defColsArr)
#print(defColsStr)

if len(sys.argv) < 3:
    print('usage: python3 %s tblName uname_xxx@geoType ' % sys.argv[0])	# geoType = "geom-bbox"
    sys.exit()
else:
    tableName = sys.argv[1]
    param1 = sys.argv[2]

parseA = param1.split('@')
uName_xxx = parseA[0]
geoType = parseA[1]

saveFilePath = '/home/stdb/richard/ftpServer1/geoSearch'
saveFilePath = saveFilePath+'/'+uName_xxx

conn = happybase.Connection('localhost', 9070, autoconnect=False)

# open conn before first use:
try:
    conn.open()
except thrift.TException as ev:
    print(ev.message)
    print('cmd line run first: start-hbase.sh && hbase thrift start-port:9070')
    sys.exit()

workTable = conn.table(tableName)

#list1 = workTable.scan(row_prefix=b'row-')
ch1 = tableName[:1]
bPrefix = bytes(ch1+'-'+uName_xxx, 'utf-8')
#print(bPrefix)
#print('--------')
dict1 = workTable.scan(row_prefix=bPrefix)
resArr = []
for key, data in dict1:
    #print(key, data)
    resArr.append(data)
# check the result
#print(resArr)
# for quick to get count in <class 'generator'> here list2 is only generator.
#list2 = workTable.scan()
#print(len(list(list2)))
conn.close()
#print('')
# generate csv with | split symbol
# do first header columns:
lineStr = ''

# uname_xxx-geom-bbox_dataStore.csv
wrFilename = param1 + '_dataStore.csv'
#wf = open('20180101000000.export.CSV', 'w')
wf = open(saveFilePath+'/'+wrFilename, 'w')
#wf.write(defColsStr+'\n')

# and then add each record value into csv file
for i in range(len(resArr)):
    itemObj = resArr[i]
    #print(itemObj)
    lineStr = ''
    #list1 = itemObj.keys()
    keyArray = []
    valArray = []
    for sKey,sVal in itemObj.items():
        # filter column:value without 'FamilyColumn'
        strK = sKey.decode("utf-8")
        parseA = strK.split(':')
        colK = parseA[1]
        strV = sVal.decode("utf-8")
        keyArray.append(colK)
        valArray.append(strV)

    # print out for check
    #print('\t'.join(keyArray))
    #print('\t'.join(valArray))

    # check in which item of defColsArr
    assignRowVal =[]
    for idx in range(len(defColsArr)):
        assignRowVal.append('\t')

    # update its correspont val
    for k in range(len(keyArray)):
        chkIdx = 0
        matchF = False
        while matchF == False and chkIdx < len(defColsArr):
            #print(defColsArr[chkIdx]+' ?= '+keyArray[k])
            if defColsArr[chkIdx] != keyArray[k]:
                chkIdx += 1
            else:
                if defColsArr[chkIdx] == keyArray[k]:
                    #print(str(k)+'. '+keyArray[k]+' chg val='+valArray[k])
                    assignRowVal[chkIdx] = valArray[k]
                    matchF = True
                else:
                    chkIdx += 1
    # concat a row with join symbol '|'
    resStr = '\t'.join(assignRowVal)
    prnStr = '\t'.join(assignRowVal)
    #print(prnStr)
    wf.write(resStr+'\n')

# close csv writer
wf.close()
#print(saveFilePath+'/'+wrFilename+' is write done')

# update uname_xxx dataFile.txt
csvDataFile = uName_xxx+"-dataFile.txt"
wf = open(saveFilePath+'/'+csvDataFile, 'w')
wf.write(wrFilename)
wf.close()
print(saveFilePath+'/'+csvDataFile+' is update OK')

