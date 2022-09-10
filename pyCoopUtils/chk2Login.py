import uuid
import os, sys
import datetime
from datetime import datetime
import json
from os.path import exists
import qryUser

#-- we rewrite to function call --
tblName = sys.argv[1]
userName = sys.argv[2]			#userName = 'tippi'
passWd = sys.argv[3]			#passWd = 'tippi'


curDT = datetime.now()
dateStr = curDT.strftime("%Y-%m-%d")
dateJsonF = 'loginLog/'+'tokenUser_'+dateStr+'.json'
dateHolder = dateStr.replace('-','')

# https://github.com/python/cpython/blob/3.5/Lib/uuid.py
def uuid4():
    """Generate a random UUID."""
    return uuid.UUID(bytes=os.urandom(16), version=4)

def writeLogFile(uName, uPw, theToken):
    userObj = {}
    userObj["username"] = uName
    userObj["password"] = uPw
    userObj["token"] = theToken
    userObj["datetime"] = curDT.strftime("%Y-%m-%d_%H:%M:%S")
    allUserArr = []
    if exists(dateJsonF):
        fr = open(dateJsonF, "r")
        tmpJsonStr = fr.read()
        fr.close()
        allUserArr = json.loads(tmpJsonStr)
    allUserArr.append(userObj)
    newJsonStr = json.dumps(allUserArr)
    wf = open(dateJsonF, "w")
    wf.write(newJsonStr)
    wf.close()


def takeOutToken(uName, uPw):
    retToken = ''
    fr = open(dateJsonF, "r")		# username,password,token,datetime
    tmpJsonStr = fr.read()
    fr.close()
    allUserArr = json.loads(tmpJsonStr)
    idx = 0
    stopF = False
    while idx < len(allUserArr) and stopF == False:
        chkObj = allUserArr[idx]
        if uName == chkObj["username"] and uPw == chkObj["password"]:
            stopF = True
            retToken = chkObj["token"]
        else:
            idx += 1
    return retToken

# check tokenUser_yyyy-mm-dd.json
# if file exists, then open it and check
# when user was login system then return the token to user;
# otherwise generate one and save into file token-yyyy-mm-dd.json
def generateToken(name, passwd):
    tmpJsonStr = ''
    userToken = ''
    if exists(dateJsonF):
        fr = open(dateJsonF, "r")
        tmpJsonStr = fr.read()
        fr.close()
        findRes1 = tmpJsonStr.find(name)
        findRes2 = tmpJsonStr.find(passwd)
        if findRes1 == -1 or findRes2 == -1:
            userToken = uuid4().hex.upper()
            writeLogFile(name, passwd, userToken)
            
        else:
            # take token from file
            allUserArr = json.loads(tmpJsonStr)
            userToken = takeOutToken(name, passwd)
    else:
        # date first customer use system, create one tokenUser_yyyy-mm-dd.json
        userToken = uuid4().hex.upper()
        writeLogFile(name, passwd, userToken)
        # and create&append in tokenUser_yyyy-mm-dd.json
        # write into tokenUser_yyyy-mm-dd.json
        
    return userToken

# check user is customer or not in user table
#resObjStr = qryUser.chkExist(userName, passWd)
resObjStr = '[]'
if len(resObjStr) < 3:
    tokenType = 'bearer'
else:
    tokenType = 'tester'

theToken = generateToken(userName, passWd)

# construct total json object, send back json.dumps(..)
totalObj = {}
payloadObj = {}
payloadObj["access_token"] = theToken
payloadObj["token_type"] = tokenType
payloadObj["expires_in"] = 86400
totalObj["payload"] = payloadObj
totalObj["version"] = "1.0"
totalObj["status"] = 200
totalObj["message"] = "OK"
# current date and time
now = datetime.now()
timeStamp = datetime.timestamp(now)
totalObj["time"] = timeStamp
print(json.dumps(totalObj))
