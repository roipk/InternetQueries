import sys
import os
import struct
import numpy as np

class IndexWriter:


    def __init__(self, inputFile, dir):
        """Given a collection of documents,
        creates an on disk index inputFile is the path to the file
        containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""

        count = 0
        readfile = open(inputFile, "r")
        s = readfile.readline()
        reads = []
        numMesmah = []
        while s:
            if s[0] == '*':
                reads.append(readfile.readline())
                numMesmah.append(count)
                count += 1
            # print(s)
            s = readfile.readline()
        readfile.close()
        s = bytearray(numMesmah)

        newFile = open(dir + "\TempFile", "wb")
        # newFileByteArray = bytearray(newFileBytes)
        newFile.write(s)
        newFile.close()



    def removeIndex(self, dir):
        """Delete all index files by removing
        the given directory dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""




if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    IW = IndexWriter('100.txt',dir)