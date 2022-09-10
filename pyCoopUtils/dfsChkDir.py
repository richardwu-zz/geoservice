import os

#theDir = sys.argv[1]

def chkDirExist(dirStr):
    cmdStr = 'hdfs dfs -stat '+dirStr
    retVal = os.system(cmdStr)
    return retVal
