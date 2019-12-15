import os
import operator
import re
import zlib
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
                words = line.split()
                for i in words:
                    i = re.sub('[^A-Za-z0-9]'," ", i)
                    i = i.lower()
                    r = i.split()
                    for j in r:
                        if ('a' <= j[0] <= 'z') or ('0' <= j[0] <= '9'):  # Ignore special signs
                            self.temp_indexer.append(j)
                firstWord = ""
                frequency = 1
                # self.temp_indexer.sort() #Sort the lists by AB
                k=0
                for k in range(0,len(self.temp_indexer)-1):
                    firstWord = self.temp_indexer[k]
                    if  firstWord != self.temp_indexer[k+1]:
                        self.indexer.append((firstWord, count, frequency))
                        frequency = 1
                    else:
                        frequency += 1


                if len(self.temp_indexer) > 0 and self.temp_indexer[k] == firstWord:
                    if frequency > 1:
                         self.indexer.append((firstWord, count, frequency))
                    else:
                        self.indexer.append((self.temp_indexer[k], count, 1))


                count += 1
                # if count % 10000 == 0:
                    # print("done {} in {} time".format(count,asctime()))


                # self.indexer = list(dict.fromkeys(self.indexer)) #remove duplicates

                # print(self.indexer)
            s = readfile.readline()
        # print("done {} in {} time".format(count, asctime()))
        self.indexer.sort(key=operator.itemgetter(0))  # Sort the lists by AB
        # print("done sort  in {} time".format(asctime()))

        ch = '0'
        s = ""
        backword = ""
        directory = "{}\{}".format(dir,'a-z')
        # print (self.indexer)
        for word in self.indexer:
            numbers = ""
            while word[0][0] != ch:
                if not os.path.exists(directory):
                    os.makedirs(directory)
                if len(s) > 0:
                    charfile = open("{}\{}.bin".format(directory,ch), "wb")

                    sb = zlib.compress(s.encode('utf-8'))
                    charfile.write(sb)
                    charfile.close()
                    # print("done write {} in {} time".format(ch,asctime()))
                s = ""
                backword = ""

                ch = chr(ord(ch) + 1)
                if ch > '9' and ch < 'a' :
                    ch = 'a'



            if len(s) == 0:
                s = "{}-{}:{}".format(word[0],word[1],word[2])
                backword = word[0]
            elif backword == word[0]:
                s += ("-{}:{}".format(word[1],word[2]))
            else:
                s += ("|{}-{}:{}".format(word[0], word[1],word[2]))
                backword = word[0]

        charfile = open("{}\{}.bin".format(directory,ch), "wb")
        sb = zlib.compress(s.encode('utf-8'))
        charfile.write(sb)
        charfile.close()





    def findDoc(self,directory,word):
        charfile = open("{}\\a-z\\{}.bin".format(directory, word[0]), "rb")
        s = charfile.read()
        charfile.close()
        s = zlib.decompress(s).decode('utf-8')


        wordArray = s.split("|")
        # print(wordArray)
        # wordArray[2]
        # numdoc =   wordArray[2].split("-")
        # print(numdoc)
        # doc = numdoc[1].split(":")
        # print(doc[0])
        docs=[]
        for i in wordArray:
            numdoc = i.split("-")
            if numdoc[0] == word:
                # print(numdoc[0])
                for j in range(len(numdoc)):
                    if j > 0:
                        doc = numdoc[j].split(":")
                        docs.append(doc[0])

        # print(docs)







    def removeIndex(self, dir):
        """Delete all index files by removing
        the given directory dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""




if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    time1 = asctime()
    print(time1)
    dir = os.getcwd()
    IW = IndexWriter('10000.txt',dir)
    # IW.findDoc(dir,"book")
    time2 = asctime()
    print(time2)
