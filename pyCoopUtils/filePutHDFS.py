import os, sys
import dfsChkDir

#hdfs dfs -put srcFile dstPath
if len(sys.argv) < 3:
    print('usage:python3 %s fileName uname_xxx' % sys.argv[0])
    sys.exit()
else:
    srcPathFile = sys.argv[1]
    srcPath = os.path.dirname(srcPathFile)
    srcFile = os.path.basename(srcPathFile)
    dstFile = srcFile
    saveUserPath = sys.argv[2]	# = uname_xxx

dstPath = '/geoxpert'+'/'+ saveUserPath

# chk dstPath exists or not
resVal = dfsChkDir.chkDirExist(dstPath)
if resVal != 0 :
    # mkdir first 'hdfs dfs -mkdir theDir'
    cmdStr = 'hdfs dfs -mkdir '+ dstPath
    os.system(cmdStr)
    print( 'mkdir %s success' % dstPath )
    
filePathName = dstPath+'/'+srcFile
# hdfs dfs -put 
cmdStr = 'hdfs dfs -put '+ srcPathFile + ' ' + filePathName
os.system(cmdStr)
print( 'put %s to %s success' % (srcFile, dstPath) )



