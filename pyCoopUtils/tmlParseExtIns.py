import sys, os
import xmltodict, json
#from threading import Thread
import extrachtKV
import insTableRow2

# in fileChgInotify1.py define : python3 tmlParseExtIns.py ' + mv2Path + ' ' + jsonPath
tmlFilePath = sys.argv[1]
saveJsonPath = sys.argv[2]

def runTheFunc(tmlFile, saveFile):
    #rework fileName ext from tml to json
    saveJsonPath = saveJsonPath.replace('.tml', '.json')
    with open(tmlFilePath, 'r') as fr:
        obj = xmltodict.parse(fr.read())
        
    dataSetStr = json.dumps(obj)
    print(dataSetStr)
    ''' here is old snippet
    nameOnly = os.path.basename(tmlFilePath)
    print(nameOnly)
    parseA = nameOnly.split('.')
    fileFront = parseA[0]
    print(fileFront)
    '''
    with open(saveJsonPath, 'w') as wf:
        wf.write(dataSetStr)

    print(saveJsonPath+" write success")

    extResStr = extrachtKV.doExtracht(saveJsonPath)

    print('takeKeyVal: '+extResStr)
    print('')

    insTableRow2.insRowAction(extResStr)
    print('insert data finish')

# run the main function
if __name__ == '__main__':
    runTheFunc(tmlFilePath, saveJsonPath)