import os, sys
import json

#-- call os.system cmdStr to query geomesa dataStore --
#tblName = sys.argv[1]
uname_xxx = sys.argv[1]
chDirCmd = 'cd /home/stdb/richard'
os.system(chDirCmd)

savePath = '/home/stdb/richard/'
saveResFile = savePath+'resultGeoSrh.txt'
#def runGeoQry(uname_xxx):
# then run the geomesa runtime command:
qryGeoCmdStr = 'java -cp geomesa-tutorials-hbase-quickstart-3.5.0-SNAPSHOT.jar org.geomesa.example.hbase.HBaseQuickStart --hbase.zookeepers localhost --hbase.catalog GeoXpert '+ uname_xxx + ' > ' + saveResFile
execRes = os.system(qryGeoCmdStr)
print(execRes)
#return resultStr
