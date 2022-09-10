# http://localhost:16010/ is HBase Web UI
# sudo $HBASE_HOME/bin/start-hbase.sh	<~ start HBase frist
# hbase thrift start -p <port> --infoport <infoport>
# ex. sudo $HBASE_HOME/bin/hbase-daemon.sh restart thrift -p 9070 --infoport 9000
# attension please !!! the content use python3 entry and line by line is fail by read data.

import happybase
from thriftpy2 import thrift

import sys

if len(sys.argv) < 2:
    print('usage:python3 %s $tableName' % sys.argv[1])
    sys.exit()
else:
    tableName = sys.argv[1]

conn = happybase.Connection('localhost', 9070, autoconnect=False)

# open conn before first use:
try:
    conn.open()
except thrift.TException as ev:
    print(ev.message)
    print('cmd line run first: start-hbase.sh && hbase thrift start-port:9070')
    sys.exit()

workTable = conn.table(tableName)
# test scan
#for key, data in workTable.scan(row_start=b'row-key10'): ~> ok
#for key, data in workTable.scan(row_stop=b'row-key18'):  ~> ok
#list1 = workTable.scan(row_prefix=b'row-')
list1 = workTable.scan()
for key, data in list1:
    print(key, data)
    
# for quick to get count in <class 'generator'> here list2 is only generator.
list2 = workTable.scan()  
print(len(list(list2)))

conn.close()
