import sys
import struct
import numpy as np

with open("100.txt","rb") as text_file:
    # One option is to call readline() explicitly
    # single_line = text_file.readline()

    # It is easier to use a for loop to iterate each line
    doc_list = np.array([])
    stars='{}\r\n'.format(('*'* 80))
    counter=0
    for line in text_file:
        if(line != stars):
            if(line !='\r\n'):
                counter=counter+1
                np.insert(doc_list,1,counter)
                print(counter)
            # print (line)
   # data = text_file.read()
   # print(data)