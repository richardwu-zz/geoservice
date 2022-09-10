import sys, os
import pyinotify
import time
from datetime import datetime
import os.path

import dfsChkDir
from threading import Thread
import zipfile          # for zip file extract and then do the same handle routine
import glob

import json

# ref. website: https://vimsky.com/zh-tw/examples/detail/python-method-pyinotify.WatchManager.html

def rework4Imgfile(f, mv2Path, uname_xxx, dtHDFSdir):
    # JPG ~> xml & dict -> HBase
    dictPath = mv2Path.replace('GeoXpertFiles','GeoXpertDict')
    print(dictPath)
    if not(os.path.exists(dictPath)):
        #create dir fist
        cmdStr = 'mkdir -p '+ dictPath
        os.system(cmdStr)
        print(cmdStr)
        print('')

    # jpg -> metadata extracht into dict file
    pyTransPath = dictPath.replace('GeoXpertDict/'+ uname_xxx, 'ImgMetadata')
    print(pyTransPath)
    # save diff path : file from mv2Path rework to save in dictPath
    cmdStr = 'python3 '+pyTransPath+'/readExifData.py '+ f +' '+dictPath
    print(cmdStr)
    os.system(cmdStr)

    # and then do transfer from .dict to .xml
    theFileName = os.path.basename(f)           # -- richard add for subroutine --
    dictFileName = theFileName.lower()
    dictFileName = dictFileName[:-4]+'.dict'    # .jpg ~> .dict
    cmdStr = 'python3 '+pyTransPath+'/dictJson2Xml.py '+ dictPath + ' ' +dictFileName
    print(cmdStr)
    os.system(cmdStr)

    # insert metadata into HBase -- 20220622 add uname_xxx as sec. params -- 20220713 add dtHDFSdir --richard
    cmdStr = 'python3 '+pyTransPath+'/geoData2HBase.py '+ dictPath +'/'+dictFileName+ ' '+uname_xxx+' '+dtHDFSdir
    print(cmdStr)
    os.system(cmdStr)

    ## -- by the way do iso19115xml.py  -- ##
    cmdStr = 'python3 '+pyTransPath+'/iso19115xml.py ' + f +' '+ dictPath
    print(cmdStr)
    os.system(cmdStr)
    ## -- end -- ##

    # xml -> direct put into HDFS
    pyCoopPath = pyTransPath.replace('ImgMetadata', 'pyCoopUtils')
    xmlFileName = dictFileName[:-5]+'.xml'

    cmdStr = 'python3 '+pyCoopPath+'/filePutHDFS.py '+ dictPath+'/'+xmlFileName+' '+ uname_xxx+'/'+dtHDFSdir
    print(cmdStr)
    os.system(cmdStr)
    
    # jpg -> direct put into HDFS, cmd: hdfs dfs -put srcFile dstPath
    cmdStr = 'python3 '+pyCoopPath+'/filePutHDFS.py '+ f +' '+ uname_xxx+'/'+dtHDFSdir
    print(cmdStr)
    os.system(cmdStr)

def addPathInTaskInfo(mv2Path, uname_xxx, pathStr):

    taskFile = mv2Path+'/'+'taskInfo-'+uname_xxx+'.json'

    if (os.path.exists(taskFile)):
        # append into the item 'includeFiles' of last object
        # read out
        fr = open(taskFile, "r")
        tmpStr = fr.read()
        fr.close()
        taskInfoArr = json.loads(tmpStr)
        taskInfoObj = taskInfoArr[len(taskInfoArr)-1]
        lastItemStr = taskInfoObj["includeFiles"]
        # add into
        if len(lastItemStr) == 0:
            lastItemStr = pathStr
        else:
            lastItemStr = lastItemStr+'@'+pathStr
        #write back
        taskInfoObj["includeFiles"] = lastItemStr
        taskInfoArr[len(taskInfoArr)-1] = taskInfoObj
        taskInfoStr = json.dumps(taskInfoArr)
        wf = open(taskFile, "w")
        wf.write(taskInfoStr)
        wf.close()
        print(pathStr+' add into '+taskFile)
    else:
        print('confuse ! '+'taskInfo-'+uname_xxx+'.json'+ ' is not exists')

def unzipFileRoutine(zipFile, unzipPath, mv2Path, uname_xxx, dtDirStr):
    
    print('begin to unzip...')
    with zipfile.ZipFile(zipFile, 'r') as zip_ref:
        zip_ref.extractall(unzipPath)

    print('')
    print('endunzip')
    # chk zip with holder or not?
    userSelfDir = ''
    for theObj in os.listdir(unzipPath):
        chkObj = os.path.join(unzipPath, theObj)
        if os.path.isdir(chkObj):
            print(chkObj)
            userSelfDir = chkObj
            #dirArr.append(chkObj)
    print('userSelfDir='+userSelfDir)
    # do unzip path in uname_xxx/taskInfo-uname_xxx.json
    if userSelfDir == '':
        addPathInTaskInfo(mv2Path, uname_xxx, dtDirStr)
    else:
        parseB = userSelfDir.split('/')
        tailFolder = parseB[len(parseB)-1]
        addPathInTaskInfo(mv2Path, uname_xxx, dtDirStr+'/'+tailFolder)

    files = []
    # when it exist, then chdir into the holder
    if len(userSelfDir) > 0:
        # all img files take out os.path.join("Folder_1", "*.txt")
        files = glob.glob(os.path.join(userSelfDir, '*.JPG'))
    else:
        print('NoUserDir => '+unzipPath);
        files = glob.glob(os.path.join(unzipPath, '*.JPG'))

    #[print(file) for file in files] -- this show one more time 'None' at end --
    idx = 0

    for idx in range(len(files)):
        f = files[idx]
        print(str(idx)+'). '+os.path.basename(f))
        rework4Imgfile(f, mv2Path, uname_xxx, dtDirStr)
        

def thread_filePutHDFS(filePathName, saveUserPath):

    dstPath = '/geoxpert'+'/'+ saveUserPath

    # chk dstPath exists or not
    resVal = dfsChkDir.chkDirExist(dstPath)
    if resVal != 0 :
        # mkdir first 'hdfs dfs -mkdir theDir'
        cmdStr = 'hdfs dfs -mkdir '+ dstPath
        os.system(cmdStr)
        print( 'mkdir %s success' % dstPath )
    fileName = os.path.basename(filePathName)
    dstPathName = dstPath+'/'+fileName
    # hdfs dfs -put 
    cmdStr = 'hdfs dfs -put '+ filePathName + ' ' + dstPathName
    os.system(cmdStr)
    print( 'put %s to %s success' % (fileName, dstPath) )

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

class MyEventHandler(pyinotify.ProcessEvent):
    
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
        # do here, add the holder to listen -- 20220620 --
        parseA = event.pathname.split('.')
        if len(parseA) == 1:
            mask = pyinotify.IN_CLOSE_WRITE
            wm.add_watch(event.pathname, mask, auto_add=True, rec=True)
            print('add_watch '+event.pathname+ ' done.')
            # by the way check HDFS geoxpert/uname_xxx exists or not, when not then create it.
            parseB = event.pathname.split('/')
            theUser = parseB[len(parseB)-1]
            print(theUser)
            resVal = dfsChkDir.chkDirExist('/geoxpert/'+theUser)
            if resVal != 0 :
                cmdStr = 'hdfs dfs -mkdir '+'/geoxpert/'+theUser
                os.system(cmdStr)
                print( 'mkdir %s success' % theUser )
        else:
            print('bypass here for it is file. but notice the create DateTimeStr.')
            # here need notice datetime for big size zip file coming!
            comingFile = event.pathname
            print("comingFile = ", comingFile)
            thePath = os.path.dirname(comingFile)
            theFileName = os.path.basename(comingFile)
            filename, file_ext = os.path.splitext(theFileName)
            parseA = thePath.split('/')
            uname_xxx = parseA[len(parseA)-1]
            print('uname_xxx='+uname_xxx)
            curDateTimeStr = datetime.today().strftime('%Y%m%d_%H%M')
            print(curDateTimeStr)
            # create txt named beg-uname_xxx-yyyyMMddHHmm.txt, when done of geomesa-qry
            # rename to end-uname_xxx-yyyyMMddHHmm-ddHHmm.txt; all files createtime inzweichen
            # belong to all between task.   glob.glob(os.path.join(userSelfDir, '*.JPG'))
            chkTaskStatus = glob.glob(os.path.join(thePath, 'beg-'+uname_xxx+'*.txt'))
            # when exists, it means that user do not geomesa-qry yet; all created files belong to these task files.
            if(len(chkTaskStatus) == 0):
                # thePath = ftpServer1/dir/yyyyMMdd/uname_xxx/
                wf = open(thePath+'/'+'beg-'+uname_xxx+'-'+curDateTimeStr+'.txt', "w")
                wf.write(curDateTimeStr)
                wf.close()

            if file_ext == '.zip':
                # here struct modified, wait chk
                theDateStr = datetime.today().strftime('%Y%m%d')
                createFilePath = thePath.replace('dir/'+theDateStr,'GeoXpertFiles')
                # write the file for process_IN_CLOSE_WRITE to take out with readOutDTstr func.
                #theFileName = curDateTimeStr+'.txt'  <~~ chg other technique! look the func readOutDTstr
                # check holder exists createFilePath
                if not(os.path.exists(createFilePath)):
                    #create dir fist
                    cmdStr = 'mkdir -p '+ createFilePath
                    os.system(cmdStr)

    def process_IN_CLOSE_WRITE(self, event):
        print('---*---')
        comingFile = event.pathname
        print("CLOSE_WRITE event: ", comingFile)
        thePath = os.path.dirname(comingFile)
        theFileName = os.path.basename(comingFile)
        filename, file_ext = os.path.splitext(theFileName)
        file_ext = file_ext.lower()

        curDateStr = datetime.today().strftime('%Y%m%d')
        mv2Path = thePath.replace('dir/'+curDateStr,'GeoXpertFiles')
        print('mv2Path='+mv2Path)
        # and extracht customerID out
        parseA = mv2Path.split('/')
        uname_xxx = parseA[len(parseA)-1]
        print('uname_xxx = '+uname_xxx)
        if not(os.path.exists(mv2Path)):
            #create dir fist
            cmdStr = 'mkdir -p '+ mv2Path
            os.system(cmdStr)
            print('.../dir/'+curDateStr+' ~> '+mv2Path)

        if file_ext == '.tml' or file_ext == '.jpg':
            # do weiter action: mv, tml->json, extracht-> dict, and last insert into HBase.
            #mv action ex. dir/20220610/kevin_vin ~> GeoXpertFiles/kevin_vin ; da. kevin_vin is assigned by ftpJSserver.js
            #						  mv2Path is only path
            cmdStr = 'cp '+ comingFile+ ' '+ mv2Path
            print(cmdStr)
            os.system(cmdStr)
            print('')
            if file_ext == '.tml':
                # tml -> json parse
                jsonPath = mv2Path.replace('GeoXpertFiles','GeoXpertJson')
                print(jsonPath)
                if not(os.path.exists(jsonPath)):
                    #create dir fist
                    cmdStr = 'mkdir -p '+ jsonPath
                    os.system(cmdStr)
                    print(cmdStr)
                    print('')
                jsonFileName = theFileName.replace('.tml','.json')
                pyTransPath = mv2Path.replace('GeoXpertFiles/'+ uname_xxx, 'pyCoopUtils')
                print(pyTransPath)
                cmdStr = 'python3 '+pyTransPath+'/tmlParse.py ' +mv2Path+'/'+theFileName+ ' ' +jsonPath
                print(cmdStr)
                os.system(cmdStr)
                print('')
                # json parse extracht to flat dict file
                cmdStr = 'python3 '+pyTransPath+'/extrachtKV.py ' + jsonPath+'/'+jsonFileName
                print(cmdStr)
                os.system(cmdStr)
                print('')
                # dict -> insert HBase
                dictFileName = jsonFileName.replace('.json', '.dict')
                cmdStr = 'python3 '+pyTransPath+'/insTableRow2.py ' + jsonPath+'/'+dictFileName +' '+ uname_xxx
                print(cmdStr)
                os.system(cmdStr)
            else:
                # JPG ~> dict ~> xml & dict -> HBase
                dictPath = mv2Path.replace('GeoXpertFiles','GeoXpertDict')
                print(dictPath)
                if not(os.path.exists(dictPath)):
                    #create dir fist
                    cmdStr = 'mkdir -p '+ dictPath
                    os.system(cmdStr)
                    print(cmdStr)
                    print('')

                # jpg -> metadata extracht into dict file
                pyTransPath = dictPath.replace('GeoXpertDict/'+ uname_xxx, 'ImgMetadata')
                print(pyTransPath)
                # save diff path : file from mv2Path rework to save in dictPath
                cmdStr = 'python3 '+pyTransPath+'/readExifData.py '+mv2Path+'/'+theFileName +' '+dictPath
                print(cmdStr)
                os.system(cmdStr)
                # and then do transfer from .dict to .xml
                dictFileName = theFileName.lower()
                dictFileName = dictFileName[:-4]+'.dict'	# .jpg ~> .dict
                cmdStr = 'python3 '+pyTransPath+'/dictJson2Xml.py '+ dictPath + ' ' +dictFileName
                print(cmdStr)
                os.system(cmdStr)
                # insert metadata into HBase -- 20220622 add uname_xxx as sec. params
                cmdStr = 'python3 '+pyTransPath+'/geoData2HBase.py '+ dictPath +'/'+dictFileName+ ' '+uname_xxx
                print(cmdStr)
                os.system(cmdStr)

                ## -- by the way do iso19115xml.py  -- ##
                cmdStr = 'python3 '+pyTransPath+'/iso19115xml.py ' + mv2Path+'/'+theFileName +' '+ dictPath
                print(cmdStr)
                os.system(cmdStr)
                ## -- end -- ##

                # xml -> direct put into HDFS
                pyCoopPath = pyTransPath.replace('ImgMetadata', 'pyCoopUtils')
                xmlFileName = dictFileName[:-5]+'.xml'

                cmdStr = 'python3 '+pyCoopPath+'/filePutHDFS.py '+ dictPath+'/'+xmlFileName+' '+ uname_xxx
                print(cmdStr)
                #os.system(cmdStr)
                
                # jpg -> direct put into HDFS, cmd: hdfs dfs -put srcFile dstPath
                thread1 = Thread(target = thread_filePutHDFS, args = (mv2Path+'/'+theFileName, uname_xxx))
                thread1.start()
                # jpg -> direct put into HDFS, cmd: hdfs dfs -put srcFile dstPath
                #cmdStr = 'python3 '+pyCoopPath+'/filePutHDFS.py '+ mv2Path+'/'+theFileName+' '+ uname_xxx
                #print(cmdStr)
                #os.system(cmdStr)

        else:
            if file_ext == '.zip':
                # massen viele files in zip
                print('----  zip files  ----')
                print(theFileName)
                theMVpath = mv2Path + '/'+curDateStr
                #create dir fist
                cmdStr = 'mkdir -p '+ theMVpath
                print(cmdStr)
                os.system(cmdStr)
                # and then move the big zip -- here need add yyMMddHHmm holder for query
                cmdStr = 'mv '+ comingFile+ ' '+ theMVpath
                print(cmdStr)
                os.system(cmdStr)
                time.sleep(1)
                
                #curDateTimeStr = datetime.today().strftime('%Y%m%d_%H%M') -- replace to use the following snippet
                zipHDFSdir = readOutDTstr(mv2Path)
                # how to check is unzip down ? <-- we do one after one action process --
                # create first HDFS uname_xxx+'/'+zipHDFSdir
                cmdStr = 'hdfs dfs -mkdir '+'/geoxpert/'+uname_xxx+'/'+zipHDFSdir
                os.system(cmdStr)
                time.sleep(3)
                print( 'mkdir %s success' % uname_xxx )
                #zipFileRoutine(comingFile, mv2Path, uname_xxx, curDateTimeStr) -- old source --
                thread2 = Thread(target = unzipFileRoutine, args = (theMVpath+'/'+theFileName, mv2Path+'/'+zipHDFSdir, mv2Path, uname_xxx, zipHDFSdir))
                thread2.start()

            else:
                print('file not for us, bypass')

        
    def process_IN_DELETE(self, event):
        print("DELETE event:", event.pathname)

    def process_IN_MODIFY(self, event):
        print("MODIFY event:", event.pathname)

    def process_IN_OPEN(self, event):
        print("OPEN event:", event.pathname)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        preDefPath = os.getcwd()
        listenPath = preDefPath.replace('pyCoopUtils', 'dir')
    else:
        listenPath = sys.argv[1]
    print('watch chg in path %s' % listenPath)
    # define global var. def. initial vaule
    mv2Path = ''
    uname_xxx = ''

    # To specify two or more events codes just orize them
    #pyinotify_flags = pyinotify.IN_CREATE | pyinotify.IN_DELETE | pyinotify.IN_MODIFY
    pyinotify_flags = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
    # Or if you want to be notified for all events just use this shortcut
    #pyinotify_flags = pyinotify.ALL_EVENTS

    # watch manager
    wm = pyinotify.WatchManager()
    watch_path = listenPath
    wm.add_watch(watch_path, pyinotify_flags, rec=True, auto_add=True) # rec = recursive

    # event handler
    eh = MyEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()
