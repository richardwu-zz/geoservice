import os
from os.path import exists

def isExist(dirStr):
    retVal = 0
    chkRes = exists(dirStr)
    # True or False ~> 1 or 0
    if chkRes :
      retVal = 1
    return retVal
