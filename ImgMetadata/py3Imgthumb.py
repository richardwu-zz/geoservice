# importing Image class from PIL package 
from PIL import Image
import sys
import os.path
from pathlib import Path
import glob
import datetime


localHome = str(Path.home())
imgBasePath = localHome+'/richard/ftpServer1/GeoXpertFiles'

# for history note file
noteFolder = localHome+'/richard/ftpServer1/dateActionNote'
nowDTstr = datetime.datetime.now()
curMonth = nowDTstr.strftime("%Y%m")
if not os.path.exists(noteFolder+'/'+curMonth):
    os.makedirs(noteFolder+'/'+curMonth)
todayStr = nowDTstr.strftime("%Y%m%d")
noteFilename = todayStr+'.log'

imgBasePath = localHome+'/richard/ftpServer1/GeoXpertFiles'
uname_xxx = sys.argv[1]
#print(uname_xxx)

# '/geoxpert/rich_ich/DJI_20210908151718_0038.JPG'
# '/geoxpert/rich_ich/20220727_2048/DJI_20210908151718_0038.JPG'
#   find path maybe by 20220727_2048 or 20220727_2048/733_745

hdfsFileName = sys.argv[2]
#print(hdfsFileName)

userIP = sys.argv[3]
#
dateTimeStr = nowDTstr.strftime("%H:%M:%S")
wf = open(noteFolder+'/'+curMonth+'/'+noteFilename, "a")
#     -- userIP | uname_xxx | funcName | funcFileName | noteStr | datetime
wf.write(userIP+'|'+uname_xxx+'|'+'entry'+'|'+'py3Imgthumb.py'+'|'+uname_xxx+'@'+hdfsFileName+'|'+dateTimeStr+'\n')
wf.close()

def searchImgSubDir(theUser, srhDir, inclFile):
    #print('in func searchImgSubDir')
    fullPath = ''
    chkDir = imgBasePath+'/'+theUser+'/'+srhDir
    fileList = glob.glob(chkDir+'/*.JPG')
    allNameStr = '@'.join(fileList)
    if allNameStr.find(inclFile) >= 0:
        # found the file
        fullPath = chkDir+'/'+inclFile

    return fullPath

parseA = hdfsFileName.split('/')
theFullPath = ''
fileName = parseA[len(parseA)-1]    # DJI_20210908151718_0038.JPG
#print('thumb fileName='+fileName)

# /geoxpert/rich_ich/20220727_2048/DJI_20210908151718_0038.JPG
if len(parseA) > 4:
    # the file in subfolder(20220727_2048) or 20220727_2048/733_745
    hdfsPath = parseA[len(parseA)-2]    # i.e 20220727_2048
    #print('hdfsPath='+hdfsPath)
    # check only all *JPG in 20220727_2048/
    theFullPath = searchImgSubDir(uname_xxx, hdfsPath, fileName)
    if len(theFullPath) == 0:
        # check only all *JPG in 20220727_2048/733_745/
        chkDir = imgBasePath+'/'+uname_xxx+'/'+hdfsPath
        inDir = os.listdir(chkDir)
        #print(inDir)
        for element in inDir:
            subChkObj = os.path.join(chkDir, element)
            #print(subChkObj)
            if os.path.isdir(subChkObj):
                # in case ../20220727_2048/733_745/               i.e element=733_745
                theFullPath = searchImgSubDir(uname_xxx, hdfsPath+'/'+element, fileName)
                if len(theFullPath) > 0:
                    break

else:
    # /geoxpert/rich_ich/DJI_20210908151718_0038.JPG  len(parseA)=4
    #print('the file is in user hdfs home')
    theFullPath = imgBasePath+'/'+uname_xxx+'/'+fileName

if len(theFullPath) > 0:
    #print('theFullPath='+theFullPath)
    #print('')
    # creating a object
    try:
        image = Image.open(theFullPath)
        width, height = image.size
        #MAX_SIZE = (int(width/percent), int(height/percent))
        MAX_SIZE = (int(width/10), int(height/10))

        # creating thumbnail
        image.thumbnail(MAX_SIZE)

        thumbSubFolder = 'thumb'
        if not os.path.exists(imgBasePath+'/'+uname_xxx+'/'+thumbSubFolder):
            os.mkdir(imgBasePath+'/'+uname_xxx+'/'+thumbSubFolder)
        saveFilePath = imgBasePath+'/'+uname_xxx+'/'+thumbSubFolder+'/'+fileName
        image.save(saveFilePath)
        print(saveFilePath)
        #image.show()
        dateTimeStr = nowDTstr.strftime("%H:%M:%S")
        wf = open(noteFolder+'/'+curMonth+'/'+noteFilename, "a")
        #     -- userIP | uname_xxx | funcName | funcFileName | noteStr | datetime
        wf.write(userIP+'|'+uname_xxx+'|'+'result'+'|'+'py3Imgthumb.py'+'|'+saveFilePath+'|'+dateTimeStr+'\n')
        wf.close()
    except:
        print("image file miss")
else:
    print('notFound')
