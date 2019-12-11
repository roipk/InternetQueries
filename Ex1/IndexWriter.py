import os
import operator
import re
import string
from time import gmtime, asctime, time, sleep


class IndexWriter:
    Term=''
    docId=0
    indexer = []

    def __init__(self, inputFile, dir):
        """Given a collection of documents,
        creates an on disk index inputFile is the path to the file
        containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""
        self.Term = ''
        self.docId = 0
        self.indexer = []
        count = 1
        readfile = open(inputFile, "r")
        s = readfile.readline()
        while s:
            if s[0] == '*':
                line = readfile.readline()
                words=line.split()
                for i in words:
                    i = i.lower()
                    i = i.replace('!' , ' ')
                    i = i.replace('&', ' ')
                    i = i.replace('.', ' ')
                    i = i.replace('-', ' ')
                    i = i.replace(',', ' ')
                    i = i.replace('?', ' ')
                    i = i.replace(']', ' ')
                    i = i.replace('[', ' ')
                    i = i.replace('}', ' ')
                    i = i.replace('{', ' ')
                    r = re.sub(r'!'or r'.'or r','or r'?' or r'-' , ' ', i)
                    r = r.split()
                    for j in r:
                        if 'a' <= j[0] <= 'z':  # Ignore special signs
                            self.indexer.append((j, count))
                count += 1
                self.indexer = list(dict.fromkeys(self.indexer)) #remove duplicates
                self.indexer.sort(key = operator.itemgetter(0)) #Sort the lists by AB
            s = readfile.readline()
        # print(self.indexer)
        ch = 'a'
        s = ""
        backword = ""
        directory = "{}\{}".format(dir,'a-z')
        for word in self.indexer:
            while word[0][0] != ch:
                # print(s)
                if not os.path.exists(directory):
                    os.makedirs(directory)
                charfile = open("{}\{}.txt".format(directory,ch), "w")
                charfile.write(s)
                charfile.close()
                s = ""
                backword = ""
                # print (ch)
                ch = chr(ord(ch) + 1)
            if len(s) == 0:
                s = ("{} {}".format(word[0],word[1]))
                backword = word[0]
            elif backword == word[0]:
                s = ("{} {}".format(s,word[1]))
            else:
                s = ("{} | {} {}".format(s, word[0], word[1]))
                backword = word[0]



        charfile = open("{}\{}.txt".format(directory,ch), "w")
        charfile.write(s)
        charfile.close()
#     for char in  range(97,123):
        #         if word[0][0] == ch:
        #             print(ch)
        #         ch = '{}'.format(chr(char))






    def removeIndex(self, dir):
        """Delete all index files by removing
        the given directory dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""




if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    time1 = asctime()
    print(time1)
    dir = os.getcwd()
    IW = IndexWriter('10000000.txt',dir)
    time2 = asctime()
    print(time2)
