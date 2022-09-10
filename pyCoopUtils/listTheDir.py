import os
import json

def listTheDir(dir):
    curDir = os.getcwd()
    resultArr = []
    
    resultArr = os.listdir(root_dir)
    resultStr = json.dumps(resultArr)
    
    return resultStr


