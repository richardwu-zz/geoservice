# http://localhost:16010/ is HBase Web UI
# sudo $HBASE_HOME/bin/start-hbase.sh	<~ start HBase frist
# hbase thrift start -p <port> --infoport <infoport>
# ex. sudo $HBASE_HOME/bin/hbase-daemon.sh restart thrift -p 9070 --infoport 9020
# attension please !!! the content use python3 entry and line by line is fail by read data.

import happybase
from thriftpy2 import thrift

import sys

tableName = ''
user = 'Dummy'
passwd = '12345'

'''
-- 20220623 current ver. list all user's task info
   when take only today will need add date timestamp --
'''
if len(sys.argv) < 4:
    print('usage: python3 %s tblName username passwd' % sys.argv[0])
    sys.exit()
else:
    tableName = sys.argv[1]
    user = sys.argv[2]
    passwd = sys.argv[3]

conn = happybase.Connection('localhost', 9070, autoconnect=False)

# open conn before first use:
try:
    conn.open()
except thrift.TException as ev:
    print(ev.message)
    print('cmd line run first: start-hbase.sh && hbase thrift start-port:9070')
    sys.exit()

workTable = conn.table(tableName)

#list1 = workTable.scan(row_prefix=b'row-')
if tableName == 'task':
    ch1 = 't-'
else:
    if tableName == 'data':
        ch1 = 'd-'
    else:
        print('unknown table name')
bPrefix = bytes(ch1+user+'_'+passwd[-3:], 'utf-8')
#print(bPrefix)
dict1 = workTable.scan(row_prefix=bPrefix)
resArr = []
for key, data in dict1:
    #print(key, data)
    for subK, kVal in data.items():
        itemObj = {}
        #print(subK, kVal)
        subKstr = subK.decode("utf-8")
        kValStr = kVal.decode("utf-8")
        parseA = subKstr.split(':')
        itemObj[parseA[1]] = kValStr
        resArr.append(itemObj)
# check the result
print(resArr)
# for quick to get count in <class 'generator'> here list2 is only generator.
#list2 = workTable.scan()
#print(len(list(list2)))

conn.close()
