import os
import operator
import re
import string
from time import gmtime, asctime, time, sleep


class IndexWriter:
    Term=''
    docId=0
    indexer = []
    f_tuple = []
    temp_indexer= []
    def __init__(self, inputFile, dir):
        """Given a collection of documents,
        creates an on disk index inputFile is the path to the file
        containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""
        self.Term = ''
        self.docId = 0
        self.indexer = []
        frequency = 1
        self.f_tuple=[]
        self.temp_indexer = []

        count = 1
        readfile = open(inputFile, "r")
        s = readfile.readline()
        while s:
            if s[0] == '*':
                self.temp_indexer = []
                line = readfile.readline()
                words=line.split()
                for i in words:
                    i = re.sub('[^A-Za-z0-9]'," ", i)
                    i = i.lower()
                    r = i.split()
                    for j in r:
                        if ('a' <= j[0] <= 'z') or ('0' <= j[0] <= '9'):  # Ignore special signs
                            self.temp_indexer.append(j)
                firstWord = ""
                frequency = 1
                self.temp_indexer.sort() #Sort the lists by AB

                for k in range(0,len(self.temp_indexer)-1):
                    print(self.temp_indexer[k])
                    firstWord = self.temp_indexer[k]
                    if  firstWord != self.temp_indexer[k+1]:
                        self.indexer.append((firstWord, count, frequency))
                        frequency = 1
                    else:
                        frequency += 1

                if self.temp_indexer[k] == firstWord:
                    if frequency > 1:
                         self.indexer.append((firstWord, count, frequency))
                    else:
                        self.indexer.append((self.temp_indexer[k], count, 1))


                count += 1
                # self.indexer = list(dict.fromkeys(self.indexer)) #remove duplicates
                self.indexer.sort(key = operator.itemgetter(0)) #Sort the lists by AB
                # print(self.indexer)
            s = readfile.readline()
        ch = '0'
        s = ""
        backword = ""
        directory = "{}\{}".format(dir,'a-z')
        print (self.indexer)
        for word in self.indexer:
            numbers = ""
            while word[0][0] != ch:
                if not os.path.exists(directory):
                    os.makedirs(directory)
                if len(s) > 0:
                    charfile = open("{}\{}.txt".format(directory,ch), "w")
                    charfile.write(s)
                    charfile.close()
                s = ""
                backword = ""

                ch = chr(ord(ch) + 1)
                if ch > '9' and ch < 'a' :
                    ch = 'a'



            if len(s) == 0:
                s = ("{} {}\\{}".format(word[0],word[1],word[2]))
                backword = word[0]
            elif backword == word[0]:
                s = ("{} {}\\{}".format(s,word[1],word[2]))
            else:
                s = ("{} | {} {}\\{}".format(s, word[0], word[1],word[2]))
                backword = word[0]

        charfile = open("{}\{}.txt".format(directory,ch), "w")
        charfile.write(s)
        charfile.close()





    def removeIndex(self, dir):
        """Delete all index files by removing
        the given directory dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""




if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    time1 = asctime()
    print(time1)
    dir = os.getcwd()
    IW = IndexWriter('test4.txt',dir)
    time2 = asctime()
    print(time2)
