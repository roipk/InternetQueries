import sys
import struct
import numpy as np
count = 0
readfile = open("100.txt", "r")
s = readfile.readline()
reads=[]
numMesmah=[]
while s:
    if s[0]=='*':
        reads.append(readfile.readline())
        numMesmah.append(count)
        count +=1
    # print(s)
    s = readfile.readline()
readfile.close()
s = bytearray(numMesmah)






# newFileBytes = s
newFile = open("TempFile.bin", "wb")
# newFileByteArray = bytearray(newFileBytes)
newFile.write(s)
newFile.close()




# with open("100.txt","rb") as text_file:
    # One option is to call readline() explicitly
    # single_line = text_file.readline()

    # It is easier to use a for loop to iterate each line
    # doc_list = np.array([])
    # stars='{}\r\n'.format(('*'* 80))
    # counter=0
    # for line in text_file:
    #     if(line != stars):
    #         if(line !='\r\n'):
    #             counter=counter+1
                # np.insert(doc_list,1,counter)
                # print(counter)
            # print (line)
   # data = text_file.read()
   # print(data)


