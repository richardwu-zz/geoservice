# check file exists and then read out 
import sys
import pathlib
import time

params = sys.argv[1]
parseA = params.split('@')
if len(parseA) > 1:
    # prepare for future multiple query types
    uname_xxx = parseA[0]
    geoQryType = parseA[1]
else:
    uname_xxx = parseA[0]

geoResFilename = 'resultGeoSrh.json'

filePath = '/home/stdb/richard/ftpServer1/geoSearch'+'/'+uname_xxx
chkFileName = filePath+'/'+geoResFilename
theFile = pathlib.Path(chkFileName)

# here we need wait some time until nodejs unlink the file
time.sleep(3)

idx = 0
existF = False
# we check 90 seconds for resultGeoSrh.json
while idx < 90 and existF == False :
    existF = theFile.exists()
    if existF :
        #print ("File exist")
        existF = True
    else:
        #print ("File not exist")
        time.sleep(1)
    idx = idx+1

if existF :
    # read out
    with open(theFile) as f:
        contents = f.read()
        print(contents)
else:
    print('check file exists timeout')
