# start command first : hbase thrift start-port:9090
# here ref. website: https://happybase.readthedocs.io/en/latest/api.html
#
# usage: python3 insTableRow.py geoXpert jsonFileName <~ rewrite to function call.
import os, os.path
import happybase
from thriftpy2 import thrift
import json
import random
import sys
import glob
import datetime
#from datetime import datetime

# img file shall take metadata out and gen. to imgDict file
imgDictFile = sys.argv[1]
metaDictName = os.path.basename(imgDictFile)
print(metaDictName)

uname_xxx = sys.argv[2]
print('uname_xxx='+uname_xxx)

# add third params for imgfilePath in HDFS -- 20220713
if len(sys.argv) >3:
    imgfilePath = '/geoxpert/'+uname_xxx+'/'+sys.argv[3]	# in case upld zip img files
else:
    imgfilePath = '/geoxpert/'+uname_xxx
    
def getCreateTime(theFile):
	stat = os.stat(theFile)
	try:
		return stat.st_mtime  #stat.st_birthtime
	except AttributeError:
		# We're probably on Linux. No easy way to get creation dates here,
		# so we'll settle for when its content was last modified.
        # str in form of 
		return stat.st_ctime

def takeActionTask(theTaskPath, allUpldTaskArr, taskBegDTstr):
    # check the file createDT to decide it is new task file or not
    retArr = []
    for j in range(len(allUpldTaskArr)):
        theTmlFile = allUpldTaskArr[j]
        print(theTmlFile)
        fileCreateTime = getCreateTime(theTmlFile)
        # check createtime > beg-uname_xxx-yyyyMMddHHmm.txt
        createDTstr = datetime.datetime.fromtimestamp(fileCreateTime, tz=datetime.timezone.utc).strftime('%Y%m%d_%H%M')
        # cmp two strings in form of yyyyMMdd_HHmm
        print('createDTstr='+createDTstr+ ' , taskBegDTstr='+taskBegDTstr)
        if createDTstr >= taskBegDTstr:
            fileName = os.path.basename(theTmlFile)[:-4]    # .tml take away
            theDictFile = fileName+'.dict'
            print(theDictFile+' add into')
            retArr.append(theTaskPath+'/'+theDictFile)
        
    return retArr

insItemList = ['filename','DateTime','GPSLatitude','GPSLongitude','GPSAltitude','ExifImageWidth','ExifImageHeight','GPSInfo']
'''
row-key : d-uname_xxx-yyyyMMddHHmm_yyy
			dataGrp1: DateTime, filename, latitude, longitude
			dataGrp2: taskid, taskname, tasktype, userid
			dataGrp3: GPSLatitude, GPSLongitude, GPSAltitude, msc
			dataGrp4: ExifImageWidth, ExifImageHeight, 
	   		dataGrp5: imgfilePath, metadataPath,timeStamp
'''
chkColStr =  'dataGrp1:DateTime@dataGrp1:filename@dataGrp1:latitude@dataGrp1:longitude@'
chkColStr = chkColStr+'@dataGrp2:taskid@dataGrp2:taskname@dataGrp2:datatype@dataGrp2:userid'
chkColStr = chkColStr+'@dataGrp3:GPSLatitude@dataGrp3:GPSLongitude@dataGrp3:GPSAltitude@dataGrp3:msc'
chkColStr = chkColStr+'@dataGrp4:ExifImageWidth@dataGrp4:ExifImageHeight'
chkColStr = chkColStr+'@dataGrp5:imgfilePath@dataGrp5:metadataPath@dataGrp5:timeStamp'

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

def insRowAction(theTblName, dictObjStr, uname_xxx, taskID, imgDictFileName):
    ch1 = theTblName[:1]
    #rdm = random.randint(1000, 2000)
    # ch1 = 'd'
    #rdm = random.randint(100, 1000)		# yyy ~ 3 digits
    #print("Random number between 100 and 1000 is % s" % (rdm))
    # fileName[-14:-5] is 9 chars = MMdd_HHmm as yyy in doc.
    imageID = imgDictFileName[-14:-5]	# take front dji_... and tail .dict away

    # 2022-08-30 modify to s3 index rowkey with space time factor, replace taskID to space
    keyWord = ch1+'-'+uname_xxx+'-'+taskID+'_'+imageID
    print('----------')
    print(keyWord)
    print('----------')

    insItemListStr = '@'.join(insItemList)
    #insItemListStr = insItemListStr.lower()

    # do mutate cols insert action ~ this do lately
    # do check connect server
    conn = happybase.Connection('localhost', 9070)
    conn.open()
    #print(conn.tables())
    bRowKey = bytes(keyWord,'UTF-8')
	# action in table which user want
    theTblBytes = bytes(theTblName,'UTF-8')
    workTable = conn.table(theTblBytes)
    
    tmpDataDict = json.loads(dictObjStr)
    print(tmpDataDict)
    insDataDict = {}
    for k, v in tmpDataDict.items():
        print(k, v)
        # if k in chkColStr then insert it, otherwise bypass
        if insItemListStr.find(k) != -1:
            if k == 'GPSInfo':
                # key divide two keys: latitude and longitude
                print(str(v[0])+' , '+ str(v[1]))
                theFC = takeBackFamCol(chkColStr, 'latitude')
                insDataDict[bytes(theFC,'UTF-8')] = bytes(str(v[0]),'UTF-8')
                theFC = takeBackFamCol(chkColStr, 'longitude')
                insDataDict[bytes(theFC,'UTF-8')] = bytes(str(v[1]),'UTF-8')
            else:
                theFC = takeBackFamCol(chkColStr, k)
                print('found: '+theFC)
                if k == 'DateTime':
                    # here need change date format yyyy:MM:dd ~~> yyyy-MM-dd
                    parseA = v.split(' ')
                    dateTimeStr = parseA[0].replace(':','-')+' '+ parseA[1]
                    insDataDict[bytes(theFC,'UTF-8')] = bytes(dateTimeStr,'UTF-8')
                else:
                    insDataDict[bytes(theFC,'UTF-8')] = bytes(v,'UTF-8')
            #workTable.put(bRowKey,{bytes(theFC,'UTF-8'):bytes(v,'UTF-8')})
        else:
            print('not found')
    
    print('----extra----')
    # extra manual add two items ~ timeStamp & imgfilePath when path give
    theFC='dataGrp5:timeStamp'
    print(theFC+'='+nowTimeStamp)
    insDataDict[bytes(theFC,'UTF-8')] = bytes(nowTimeStamp,'UTF-8')
    theFC='dataGrp5:imgfilePath'
    print(theFC+'='+imgfilePath)
    insDataDict[bytes(theFC,'UTF-8')] = bytes(imgfilePath,'UTF-8')
    print('---------')
    
    # print out for check
    for k2, v2 in insDataDict.items():
        print(k2, v2)
    # do action insert
    workTable.put(bRowKey, insDataDict)
    print('insert down')
    # take it back for confirm
    conn.close()
    
tblName = 'data'

with open(imgDictFile, 'r') as fr:
    dataStr = fr.read()

# take all task with this image out, insert image data for each task
absoPath = '/home/stdb/richard/ftpServer1/GeoXpertJson'

# take the task beg datetimeStr
yyyyMMdd = datetime.datetime.today().strftime('%Y%m%d')
absoPath2 = '/home/stdb/richard/ftpServer1/dir/'+yyyyMMdd+'/'+uname_xxx
''' ~~ 20220713 chg --
yyyyMMdd = datetime.datetime.today().strftime('%Y%m%d')
absoPath2 = '/home/stdb/richard/ftpServer1/dir/'+yyyyMMdd+'/'+uname_xxx
chkTaskDTlist = glob.glob(os.path.join(absoPath2, 'beg-'+uname_xxx+'*.txt'))
taskBegDTfileN = os.path.basename(chkTaskDTlist[0])
parseA = taskBegDTfileN.split('-')
chkDatetimeStr = parseA[2][:-4]     # prevention to task not upload down yet,
'''
# replace above snippet --
dtST = datetime.datetime.now().timestamp()
dtSTint = int(dtST)
chkDatetimeStr = str(dtSTint)
print(chkDatetimeStr)
nowTimeStamp = chkDatetimeStr

# here we chk upld tml file as after rework dict file.
taskPathArr = glob.glob( os.path.join(absoPath2,"*.tml") )
print(taskPathArr)

curTaskArr = takeActionTask(absoPath+'/'+uname_xxx, taskPathArr, chkDatetimeStr)
print('task info files:')
print(curTaskArr)

# and at last do each select task add record into data table
''' -- chg to no use theTaskId --
if len(curTaskArr) > 0 :
    for i in range(len(curTaskArr)):
        taskFile = curTaskArr[i]
        theTaskId = taskFile[5:-5]
        print('insRowAction '+tblName+ ' '+uname_xxx+' '+theTaskId)
        insRowAction(tblName, dataStr, uname_xxx, theTaskId, metaDictName)
else:
    # case, when img file upload down before task tml file
    print('insRowAction '+tblName+ ' '+uname_xxx+' '+chkDatetimeStr)
    insRowAction(tblName, dataStr, uname_xxx, chkDatetimeStr, metaDictName)
'''
# # replace above snippet --
print('insRowAction '+tblName+ ' '+uname_xxx+' '+chkDatetimeStr)
insRowAction(tblName, dataStr, uname_xxx, chkDatetimeStr, metaDictName)
