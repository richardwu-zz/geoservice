# start command first : hbase thrift start-port:9090
import happybase
import sys
from pyUtils import tableUtil

if len(sys.argv) != 2:
    print('Error ! One parameter please, \n\tUsage: python3 '+ sys.argv[0] +' tableName')
    sys.exit()

dropTable = sys.argv[1]
bDropTable = bytes(dropTable, 'utf-8')

conn = happybase.Connection('localhost', 9070)
conn.open()

tableList = conn.tables()
print(tableList)

foundF = tableUtil.chkTableExist(tableList, bDropTable)
if foundF == True:
    conn.disable_table(bDropTable)
    conn.delete_table(bDropTable)
    tableList = conn.tables()
    print(tableList)
    print('The table %s delete success' % dropTable)
else:
    print('The table %s is not exists' % dropTable)

conn.close()
