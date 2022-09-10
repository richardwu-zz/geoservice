# start command first : hbase thrift start-port:9000 -i:9070
import happybase
import time
import sys

if len(sys.argv) != 2:
    print('Error ! One parameter please, \n\tUsage: python3 '+sys.argv[0]+' tableName')
    sys.exit()
    
theTable = sys.argv[1]
conn = happybase.Connection('localhost', 9070)
conn.open()

#bDropTable = bytes(sys.argv[0], 'UTF-8')
#conn.delete_table(bDropTable)
#print('delete original '+ bDropTable)
conn.create_table(
    theTable,
    {
     'userCls1': dict(max_versions=10, block_cache_enabled=False),
     'userCls2': dict(max_versions=3),
     'userCls3': dict(max_versions=2),
    }
)

print(conn.tables())
print('create ' + theTable + ' is success')

conn.close()
