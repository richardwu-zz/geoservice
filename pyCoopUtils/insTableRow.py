# start command first : hbase thrift start-port:9090
# here ref. website: https://happybase.readthedocs.io/en/latest/api.html
#
# usage: python3 insTableRow.py geoXpert jsonFileName

import happybase
import sys, json
import time
#import GXuitls

if len(sys.argv) < 3:
    print('usage: python3 insTableRow.py tblName dictFileName')
    sys.exit()

tblName = sys.argv[1]
fileName = sys.argv[2]

keyWord1 = 'userId'

# we check here only zweite stuffe for HBase
def takeValfromKey(theObj, theKey):
	# initial return string value
	retVal = ''
	#print('0 -> '+ theKey)

	if theKey in  theObj:
		return theObj[theKey]
	else:
		# pruefe zweite stuffe
		found = False
		for k, v in theObj.items():
			print( k, v )
			
			for sk, sv in v.items():
				print( sk, sv )
				if sk == theKey:
					found = True
					retVal = sv
					break

		if found == True:
			return retVal
		else:
			return None

''' 
	the following as example :
	workTable.put(bRowKey, {b'cf1:col1': b'value1', b'cf1:col2': b'value2'})
	workTable.put(bRowKey, {b'cf2:col1': b'value3', b'cf2:col2': b'value4', b'cf2:col3': b'value5'})
	...
'''

def insRowAction(toTable, theRowkey, theDictObj):

	# do check connect server
	conn = happybase.Connection('localhost')
	conn.open()
	print(conn.tables())
	bRowKey = bytes(theRowkey, 'UTF-8')
	# action in table which user want
	workTable = conn.table(toTable)
	
	print(theDictObj)
	for k in theDictObj:
		print('\n')
		print(k)
		subObj = theDictObj[k]
		#print(subObj)
		for key, data in subObj.items():
			# we need do each key-val pair remark string to bytes format
			print(k+':'+key, data)
			tmpKey = bytes(k+':'+key, 'UTF-8')
			tmpData = bytes(str(data), 'UTF-8')
			workTable.put(bRowKey, {tmpKey:tmpData})
		
	conn.close()


def insRow( theTable, rowKey, jsonFileName ):

	# check rational incoming data
	fr = open(jsonFileName, "r")
	familyColsDictStr = fr.read()
	fr.close()

	# dictStr parse to dictObj
	dictObj = json.loads(familyColsDictStr)
	print(dictObj)

	tmpVal = takeValfromKey(dictObj, keyWord1)
	print(tmpVal)
	if tmpVal != None :
		if tmpVal == rowKey:
			insRowAction(theTable, rowKey, dictObj)
			time.sleep(1)
			#chkInsRowResult(theTable, rowKey)
		else:
			print('row-key in data is confuse')

	else:
		print('miss row-key in data')

# do here for test 
theRowkey = fileName[:len(fileName)-5]	# 5 = len('.json')
print(theRowkey)
insRow(tblName, theRowkey, fileName)
