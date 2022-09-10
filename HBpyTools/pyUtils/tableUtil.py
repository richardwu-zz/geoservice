### common functions

def chkTableExist(tableList, theTable) :
	idx = 0
	stopF = False
	while idx < len(tableList) and stopF == False:
		aTable = tableList[idx]
		if aTable == theTable:
			# set found flag
			stopF = True
		else:
			idx += 1
	return stopF;


