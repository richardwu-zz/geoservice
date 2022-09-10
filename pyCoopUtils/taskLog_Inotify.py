import sys, os
import pyinotify
import time
from datetime import datetime
import os.path
import json
#import dfsChkDir
#from threading import Thread
#import zipfile          # for zip file extract and then do the same handle routine
#import glob

listenPath = "/home/stdb/richard/ftpServer1/GeoXpertFiles"

# for multi zip files case
def readOutDTstr(curUserPath):
    # all zip file generated the datetimeStr txt file in user folder
    #print(curUserPath)
    subPrefix = datetime.today().strftime('%Y%m%d_%H%M')
    idx = 0
    for obj in os.listdir(curUserPath):
        if obj.startswith(subPrefix):
            idx += 1
    if idx > 0 :
        subDir = subPrefix+'-'+str(idx)
    else:
        subDir = subPrefix
    return subDir


def takeOutTaskname(fileName):
    fr = open(fileName, "r")
    content = fr.read()
    fr.close()
    pos1 = content.find("<name>")
    pos2 = content.find("</name>",pos1+1)
    taskName = content[pos1+len('<name>'):pos2]
    taskName = taskName.replace('\n','')
    taskName = taskName.replace(' ','')
    return taskName


class MyEventHandler2(pyinotify.ProcessEvent):
    
    def process_IN_ACCESS(self, event):
        print("ACCESS event:", event.pathname)

    def process_IN_ATTRIB(self, event):
	    print("ATTRIB event:", event.pathname)

    def process_IN_MODIFY(self, event):
	    print("ATTRIB event:", event.pathname)

    def process_IN_CLOSE_NOWRITE(self, event):
        print("CLOSE_NOWRITE event:", event.pathname)

    def process_IN_CREATE(self, event):
        print("CREATE event:", event.pathname)
        # do here, add the holder to listen -- newUser --
        parseA = event.pathname.split('.')
        if len(parseA) == 1:
            mask = pyinotify.IN_CLOSE_WRITE
            wm.add_watch(event.pathname, mask, auto_add=True, rec=False)
            print('add_watch '+event.pathname+ ' done.')
        else:
            print('imcoming file is unknown ?')

    def process_IN_CLOSE_WRITE(self, event):
        print('---@---')
        comingFile = event.pathname
        print("CLOSE_WRITE event: ", comingFile)

        theFilePath = os.path.dirname(comingFile)
        parseA = theFilePath.split('/')

        theFileName = os.path.basename(comingFile)
        print(comingFile)

        filename, file_ext = os.path.splitext(theFileName)
        file_ext = file_ext.lower()

        curDateStr = datetime.today().strftime('%Y%m%d_%H%M')
        
        if file_ext == '.tml':
            # action for task or single JPG file
            uname_xxx = parseA[len(parseA)-1]
            print(uname_xxx)
            userTaskInfo = 'taskInfo-'+uname_xxx+'.json'
            taskInfoArr = []
            taskInfoObj = {}

            # add the new task object into uname_xxx's taskInfo-uname_xxx.json
            taskname = takeOutTaskname(comingFile)
            print(taskname)
            taskInfoObj["actionDate"] = curDateStr
            taskInfoObj["taskName"] = taskname
            taskInfoObj["includeFiles"] = ""

            # rework taskInfo-uname_xxx.json file
            if (os.path.exists(listenPath+'/'+uname_xxx+'/'+userTaskInfo)):
                fr = open(listenPath+'/'+uname_xxx+'/'+userTaskInfo, "r")
                tmpStr = fr.read()
                fr.close()
                taskInfoArr = json.loads(tmpStr)

            taskInfoArr.append(taskInfoObj)
            taskInfoStr = json.dumps(taskInfoArr)
            wf = open(listenPath+'/'+uname_xxx+'/'+userTaskInfo, "w")
            wf.write(taskInfoStr)
            wf.close()

        else:
            if file_ext == '.jpg':
                # check unzip include file or not :
                # '/home/stdb/richard/ftpServer1/GeoXpertFiles/rich_ich/20220728'   split('/') = 8
                # '/home/stdb/richard/ftpServer1/GeoXpertFiles/rich_ich'            split('/') = 7
                if len(parseA) == 7: 
                    print(theFileName)
                    taskInfoArr = []
                    taskInfoObj = {}
                    # add file item info into taskInfo-uname_xxx.json
                    if (os.path.exists(listenPath+'/'+uname_xxx+'/'+userTaskInfo)):
                        # append into the item 'includeFiles' of last object
                        fr = open(listenPath+'/'+uname_xxx+'/'+userTaskInfo, "r")
                        tmpStr = fr.read()
                        fr.close()
                        taskInfoArr = json.loads(tmpStr)
                        taskInfoObj = taskInfoArr[len(taskInfoArr)-1]
                        lastItemStr = taskInfoObj["includeFiles"]
                        if len(lastItemStr) == 0:
                            lastItemStr = theFileName
                        else:
                            lastItemStr = lastItemStr+'@'+theFileName
                        taskInfoObj["includeFiles"] = lastItemStr
                        taskInfoArr[len(taskInfoArr)-1] = taskInfoObj
                        taskInfoStr = json.dumps(taskInfoArr)
                    else:
                        print(userTaskInfo+ ' is not exists')
                        taskInfoObj["actionDate"] = curDateStr
                        taskInfoObj["taskName"] = "unknown"
                        taskInfoObj["includeFiles"] = theFileName
                        taskInfoStr = json.dumps(taskInfoArr)
                        
                    #write back
                    wf = open(listenPath+'/'+uname_xxx+'/'+userTaskInfo, "w")
                    wf.write(taskInfoStr)
                    wf.close()

            else:
                '''
                # zip of JPG folder case -- we do add this info in fileChgInotify1.py, after unzip --
                if file_ext == '.zip':
                    print(theFileName)
                else:
                    print(file_ext+'file not for us, bypass')
                '''
                if file_ext == '.zip':
                    print(file_ext+' file do folder-taskInfo in fileChgInotify1.py!')
                else:
                    print(file_ext+' file not for us, bypass')
        
    def process_IN_DELETE(self, event):
        print("DELETE event:", event.pathname)

    def process_IN_MODIFY(self, event):
        print("MODIFY event:", event.pathname)

    def process_IN_OPEN(self, event):
        print("OPEN event:", event.pathname)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        listenPath = sys.argv[1]
    print('watch chg in path %s' % listenPath)
    # define global var. def. initial vaule
    
    # To specify two or more events codes just orize them
    pyinotify_flags = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
    # Or if you want to be notified for all events just use this shortcut
    #pyinotify_flags = pyinotify.ALL_EVENTS

    # watch manager
    wm = pyinotify.WatchManager()
    watch_path = listenPath
    wm.add_watch(watch_path, pyinotify_flags, rec=True, auto_add=True) # rec = recursive

    # event handler
    eh = MyEventHandler2()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()
