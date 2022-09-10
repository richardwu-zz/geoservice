# start command first : hbase thrift start-port:9000 -i:9070
import happybase
import time
import sys

if len(sys.argv) != 2:
    print('Error ! One parameter please, \n\tUsage: python3 '+sys.argv[0]+' tableName')
    sys.exit()
    
theTable = sys.argv[1]
conn = happybase.Connection('localhost', 9000)
conn.open()

conn.create_table(
    theTable,
    {'taskInfo1': dict(max_versions=10, block_cache_enabled=False),
     'tasknfo2': dict(max_versions=3),
     'taskInfo3': dict(max_versions=2),
     'taskInfo4': dict(max_versions=10, block_cache_enabled=False)
    }
)

print(conn.tables())
print('create ' + theTable + ' is success')

conn.close()
