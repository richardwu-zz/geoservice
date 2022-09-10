import os, sys
import xmltodict, json

chkFlag = True
if len(sys.argv) >= 3:
    GXdataFile = sys.argv[1]
    saveJsonPath = sys.argv[2]
elif len(sys.argv) == 2:
    GXdataFile = sys.argv[1]
    saveJsonPath = os.getcwd()
else:
    chkFlag = False

if chkFlag == True:
    tmpfileName = os.path.basename(GXdataFile)
    saveJsonFile = os.path.splitext(tmpfileName)[0]+'.json'
    print('will be saved '+saveJsonFile)

    with open(GXdataFile, 'r') as fr:
        tmpContStr = fr.read()

    tmpContStr = tmpContStr.replace('1.0" ?>', '1.0" >')
    print(tmpContStr)
    obj = xmltodict.parse(tmpContStr)
    dataSetStr = json.dumps(obj)
    dataSetStr = dataSetStr.replace('\\t', '')
    dataSetStr = dataSetStr.replace('\\n', '@')
    with open(saveJsonPath+'/'+saveJsonFile, 'w') as wf:
        wf.write(dataSetStr)

    print(saveJsonPath+'/'+saveJsonPath+" write success")
    print('')
else:
    print('miss paramter')

