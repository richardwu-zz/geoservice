import os, sys
import datetime
from datetime import datetime
import json
import os.path
import glob


userID = sys.argv[1]

savePath = '/home/stdb/richard/ftpServer1/dir'
if len(sys.argv) < 3:
    curDT = datetime.now()
    dateStr = curDT.strftime("%Y%m%d")
else:
    dateStr = sys.argv[2]
    print(dateStr)

filePath = savePath+'/'+dateStr+'/'+userID

fInfoList = []

isExist = os.path.exists(filePath)
if isExist:
    # list all files
    files = os.listdir(filePath)
    #print(files)
    for f in files:
        fInfoObj = {}
        info = os.stat(filePath+'/'+f)
        fSize = os.path.getsize(filePath+'/'+f)
        #fcTime = os.path.getmtime(filePath+'/'+f)
        #print(f+'\t'+str(fSize).rjust(9," ")+'\t'+str(fcTime))
        print('')
        fInfoObj["fileName"] = f
        fInfoObj["fileSize"] = fSize
        #fInfoObj["fileCtime"] = fcTime		# timestamp
        fInfoList.append(fInfoObj)
else:
    print(filePath+' is not exists')
    
#print('')   
retStr = json.dumps(fInfoList)
print(retStr)

