import sys, os
import os.path
from pathlib import Path
import glob

localHome = str(Path.home())
imgBasePath = localHome+'/richard/ftpServer1/GeoXpertFiles'

uname_xxx = sys.argv[1]
print(uname_xxx)
# here need modify:
# '/geoxpert/rich_ich/20220727_2048/DJI_20210908151718_0038.JPG' to maybe found subfolder from 20220727_2048
# imgBasePath+'rich_ich/20220727_2048/'+subfolder+'DJI_20210908151718_0038.JPG'. subfolder maybe empty!
hdfsFileName = sys.argv[2]
print(hdfsFileName)

def searchImgSubDir(theUser, srhDir, inclFile):
    #print('in func searchImgSubDir')
    fullPath = ''
    chkDir = imgBasePath+'/'+theUser+'/'+srhDir
    inDir = os.listdir(chkDir)
    print(inDir)
    for element in inDir:
        if os.path.join(chkDir, inclFile) == os.path.join(chkDir, element):
            fullPath = os.path.join(chkDir, inclFile)
            print(fullPath)
            return fullPath

        else:
            if os.path.isdir(os.path.join(chkDir, element)):
                subDir = os.path.join(chkDir, element)
                print('subDir='+subDir)
                fileOrig = os.path.join(subDir, inclFile)
                print('fileOrig='+fileOrig)
                for theFile in glob.glob(subDir+'/*.JPG'):
                    print(theFile+' ?= '+fileOrig)
                    if  fileOrig == theFile:
                        fullPath = os.path.join(subDir, inclFile)
                        print(fullPath)
                        break

                return fullPath

            else:
                return fullPath

parseA = hdfsFileName.split('/')
theFullPath = ''
fileName = parseA[len(parseA)-1]    # DJI_20210908151718_0038.JPG
print('fileName='+fileName)

if len(parseA) > 4:
    
    hdfsPath = parseA[len(parseA)-2]    # i.e 20220727_2048
    print('hdfsPath='+hdfsPath)
    theFullPath = searchImgSubDir(uname_xxx, hdfsPath, fileName)
else:
    print(' is in user hdfs home')
    theFullPath = imgBasePath+'/'+uname_xxx+'/'+fileName
print('theFullPath='+theFullPath)
