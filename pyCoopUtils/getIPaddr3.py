import os
import re

cmdStr = 'ifconfig > allIpAddr.txt'
os.system(cmdStr)

# opening and reading the file
with open('allIpAddr.txt') as fr:
    allInfoStr = fr.readlines()

# declaring the regex pattern for IP addresses
pattern = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')

# initializing the list object
lst=[]

# extracting the IP addresses
for line in allInfoStr:
   srhRes = pattern.search(line)
   if not srhRes == None:
       lst.append(srhRes[0])

# displaying the extracted IP addresses

print(lst)
