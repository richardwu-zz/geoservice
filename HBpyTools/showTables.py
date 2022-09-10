# http://localhost:16010/ is HBase Web UI
import happybase
from thriftpy2 import thrift
import sys

connection = happybase.Connection(host='127.0.0.1', port=9070, autoconnect=False)

# open conn before first use:
try:
    connection.open()
except thrift.TException as ev:
    print(ev.message)
    print('cmd line run first: start-hbase.sh && hbase thrift start-port:9070')
    sys.exit()


tableList = connection.tables()
print(tableList)
#read schema out for confirm
connection.close()
