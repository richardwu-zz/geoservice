#!/usr/bin/python
# -*- coding: utf-8 -*-

# http://localhost:16010/ is HBase Web UI

import sys
import happybase

showSchemaTbl = sys.argv[1]
tblNameBytes = bytes(showSchemaTbl,'UTF-8')

conn = happybase.Connection('localhost', 9070, autoconnect=False)

# open conn before first use:
conn.open()

tblList = conn.tables()
print(tblList)

print('--- '+showSchemaTbl+' table info content ---')

if len(tblList) > 0:
    idx = 0
    stopF = False
    while idx < len(tblList) and stopF == False:
        theTblOne = tblList[idx]
        if theTblOne == tblNameBytes:
            viewTable = happybase.Table(theTblOne, conn)
            print('--- region ---')
            print(viewTable.regions())
            print('')
            print('--- families ---')
            print(viewTable.families())
            stopF = True
        else:
            idx += 1
    
else:
    print('Sorry, No tables in HBase')
    
conn.close()
