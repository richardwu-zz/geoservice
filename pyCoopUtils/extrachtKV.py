import json
import os, sys

doneJsonFile = sys.argv[1]

def doExtracht(jsonFile):
    with open(jsonFile, 'r') as fr:
        dataSetStr = fr.read()
        print(dataSetStr)
        
    dataSetObj = json.loads(dataSetStr)
    print(dataSetObj)
    print('-------')
    #f=lambda d:`d`[0]in'{['and sum([[str(k)]+['%s.'%k+q for q in f(v)]for k,v in(enumerate,dict.items)['{'<`d`](d)],[])or[]

    resDict = {}

    for k1, v1 in dataSetObj.items():
        print(k1)
        print('-------')
        if isinstance(v1, str):
            print(k1+'-->'+v1)
            matchObj = {}
            matchObj[k1] = v1
            resDict.append(matchObj)
        else:	
            for k2, v2 in v1.items():
                # second step
                if isinstance(v2, str):
                    print('  '+k2+' --> '+v2)
                    resDict[k2] = v2
                else:
                    for k3, v3 in v2.items():
                        if isinstance(v3, str):
                            print('    '+k3+' --> '+str(v3))
                            resDict[k3] = v3
                        else:
                            #print('here parse only 3 steps')
                            for k4, v4 in v3.items():
                                if isinstance(v4, str):
                                    print('      '+k3+':'+k4+' --> '+str(v4))
                                    resDict[k3+':'+k4] = v4
                                else:
                                    if not isinstance(v4, list):
                                        if v4 is None:
                                            v4 = ''
                                            print('      '+k3+':'+k4+' --> '+str(v4))
                                            resDict[k3+':'+k4] = str(v4)
                                        else:
                                            for k5, v5 in v4.items():
                                                if isinstance(v5, str):
                                                    print('        '+k4+':'+k5+'-->'+str(v5))
                                                    resDict[k4+':'+k5] = v5
                                                else:
                                                    print('too tief steps we do bypass')
                                    else:
                                        print('      '+k3+':'+k4+' --> '+str(v4))
                                        resDict[k3+':'+k4] = str(v4)
    print()
    print('===== result =======')
    for k,v in resDict.items():
        print(k,v)
        
    tmpfileName = os.path.basename(doneJsonFile)
    tmpfilePath = os.path.dirname(doneJsonFile)
    if len(tmpfilePath) == 0:
        tmpfilePath = os.getcwd()
    doneDictFile = os.path.splitext(tmpfileName)[0]+'.dict'
    resStr = json.dumps(resDict)
    # rework for insTableRow2.py better to insert into HBase
    resStr = resStr.replace("uuid","taskid")
    resStr = resStr.replace("name","taskname")

    # write into for debug
    with open(tmpfilePath+'/'+doneDictFile, 'w') as wf:
        wf.write(resStr)
    print(tmpfilePath+'/'+doneDictFile+' write success')
    # to string and return
    return resStr

doExtracht(doneJsonFile)



