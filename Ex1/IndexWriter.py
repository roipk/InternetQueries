import os ,os.path
import operator
import re
import sys
import zlib
import shutil
import string
import datetime
from time import gmtime, asctime, time, sleep


class IndexWriter:
    Term = ''
    docId=0
    indexer = []
    f_tuple = []
    temp_indexer= []
    maxread = 50000
    blocks = 0
    b = 40
    startTime=""
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
        self.blocks = 0
        self.startTime = datetime.datetime.now()
        self.createTempFolder(inputFile, dir)
        self.mergeFolders(dir)



    def createTempFolder(self, inputFile, dir):
        count = 1
        readfile = open(inputFile, "r")
        s = readfile.readline()
        # s = readfile.read(self.maxread)
        print("done read  in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        # def test(self,s,readfile,count):
        while s:
            if s[0] == '*':
                self.temp_indexer = []
                line = readfile.readline()
                line = re.sub('[^A-Za-z0-9]', " ", line)
                line = line.lower()
                words = line.split()
                for i in words:
                    self.temp_indexer.append(i)
                print("done temp  in {} time".format(asctime()))
                    # # i = re.sub('[^A-Za-z0-9]'," ", i)
                    # # i = i.lower()
                    # r = i.split()
                    # for j in r:
                    #     if ('a' <= j[0] <= 'z') or ('0' <= j[0] <= '9'):  # Ignore special signs

                print(datetime.datetime.now() - self.startTime)
                firstWord = ""
                frequency = 1
                self.temp_indexer.sort() #Sort the lists by AB
                print("done temp sort in {} time".format(asctime()))
                print(datetime.datetime.now() - self.startTime)
                k=0
                for k in range(0,len(self.temp_indexer)-1):
                    firstWord = self.temp_indexer[k]
                    if  firstWord != self.temp_indexer[k+1]:
                        self.indexer.append((firstWord, count, frequency))
                        # print(firstWord, count, frequency)
                        frequency = 1
                    else:
                        frequency += 1


                if  len(self.temp_indexer) > 0 and  self.temp_indexer[k] == firstWord:
                    if frequency > 1:
                         self.indexer.append((firstWord, count, frequency))
                         # print(firstWord, count, frequency)
                    else:
                        self.indexer.append((self.temp_indexer[k], count, 1))

                print("done indexer  in {} time".format(asctime()))
                print(datetime.datetime.now() - self.startTime)
                if sys.getsizeof(self.indexer)> self.maxread:
                    print("1 write - {}".format(asctime()))
                    self.writeToFile(dir)
                    self.blocks += 1
                    self.indexer=[]
                count += 1
                if count % 100000 == 0:
                    print("done {} in {} time".format(count,asctime()))



                    # self.indexer = list(dict.fromkeys(self.indexer)) #remove duplicates

                    # print(self.indexer)
            s = readfile.readline()
            # s = readfile.read(self.maxread)
            # print("done {} in {} time".format(count, asctime()))
        if len(self.indexer) > 0:
            print("2 write - {}".format(asctime()))
            self.writeToFile(dir)

            # self.MargeFile(dir)
        return



    def MargeFile(self,dir):
        directory = "{}\{}".format(dir, 'temp')
        countFiles = list(os.walk(directory))[0][1]
        print(len(countFiles))
        if (len(countFiles) % 2 > 0):
            print((countFiles))

        directory0 = "{}\{}\{}\{}.bin".format(dir, 'temp', 0, 0)
        directory1 = "{}\{}\{}\{}.bin".format(dir, 'temp', 1, 0)
        dst = "{}\{}\{}.bin".format(dir, 'temp1', 0)
        self.mergeandsort(directory0,directory1,dst)
        # while countFiles > 1:
        #     for i in countFiles:
        #
        #         pass

    def mergeFolders(self,dir):
        directory = "{}\{}".format(dir, 'temp')
        if os.path.exists(directory):
            folders = list(os.walk(directory))
            countfolders = len(folders[0][1])
            print(countfolders)
            if countfolders <= 1:
                return
            print("test")
            self.blocks += 1
            path = '{}\{}\\'.format(directory,self.blocks)
            if not os.path.exists(path):
                os.makedirs(path)
                self.createFolders(path)


            for f, b in zip(folders[1::2], folders[2::2]):
                for i in range (len(f[2])):
                    newpath ='{}{}'.format(path,f[2][i])
                    folder1 = '{}\{}'.format(f[0],f[2][i])
                    folder2 = '{}\{}'.format(b[0], b[2][i])
                    self.mergeandsort(folder1,folder2,newpath)
            self.delfolder(f[0])
            self.delfolder(b[0])


            print(countfolders)
            print(folders)
        return



    def delfolder(self,path):
        if os.path.exists(path):
            shutil.rmtree(path)
        return
    def mergeandsort(self, src1, src2, dst):
        # Use `with` statements to close file automatically
        with open(src1, 'r') as s1, open(src2, 'r') as s2, open(dst, 'w') as d:
            file1 = s1.read(self.maxread)
            file2 = s2.read(self.maxread)
            file1 = file1.split("|")
            file2 = file2.split("|")
            i=0
            j=0
            str = ''
            while i < len(file1) and j < len(file2):
                sub1 = file1[i].split("-")
                sub2 = file2[j].split("-")

                # i+=1


                if len(sub1[0]) <= 0 and len(sub2[0]) > 0:
                    if len(str) == 0:
                        str += "{}-{}".format(sub2[0], sub2[1])
                    else:
                        str += "|{}-{}".format(sub2[0], sub2[1])
                    i += 1

                elif  len(sub2[0]) <= 0 and len(sub1[0]) > 0 :
                    if len(str) == 0:
                        str+="{}-{}".format( sub1[0],sub1[1])
                    else:
                        str += "|{}-{}".format(sub1[0], sub1[1])
                    i+=1

                elif  sub1[0] < sub2[0]:
                    if len(str) == 0:
                        str+="{}-{}".format( sub1[0],sub1[1])
                    else:
                        str += "|{}-{}".format(sub1[0], sub1[1])
                    i+=1

                elif   sub1[0] > sub2[0]:
                    if len(str) == 0:
                        str+="{}-{}".format( sub2[0],sub2[1])
                    else:
                        str+="|{}-{}".format( sub2[0],sub2[1])
                    j+=1
                elif len(sub2[0]) <= 0 and len(sub1[0]) <= 0:
                    i += 1
                    j += 1
                else:
                    if len(str) == 0:
                        str += "{}-{}_{}".format(sub1[0], sub1[1],sub2[1])
                    else:
                        str += "|{}-{}_{}".format(sub1[0], sub1[1],sub2[1])
                    i += 1
                    j += 1

            while i < len(file1):
                sub1 = file1[i].split("-")
                if len(sub1[0]) > 0:
                    if len(str) == 0:
                        str += "{}-{}".format(sub1[0], sub1[1])
                    else:
                        str += "|{}-{}".format(sub1[0], sub1[1])
                i += 1

            while j < len(file2):
                sub2 = file2[j].split("-")
                if len(sub2[0]) > 0:
                    if len(str) == 0:
                        str += "{}-{}".format(sub2[0], sub2[1])
                    else:
                        str += "|{}-{}".format(sub2[0], sub2[1])
                j += 1


            # print ("s1 = {}".format(file1))
            # print ("s2 = {}".format(file2))


            # l.sort # Since you seem to want them in reverse order...
            # c = ''.join(l)
            d.write(str)
        return



    def writeToFile(self,dir):
        self.indexer.sort(key=operator.itemgetter(0))  # Sort the lists by AB
        print("done sort  in {} time".format(asctime()))

        ch = '0'
        s = ""
        backword = ""
        # directory = "{}\{}\{}".format(dir,'a-z',self.blocks)
        print("done blocks  in {}".format(self.blocks))
        directory = "{}\{}\{}".format(dir, 'temp', self.blocks)
        # print (self.indexer)
        for word in self.indexer:
            numbers = ""
            while word[0][0] != ch:
                if not os.path.exists(directory):
                    os.makedirs(directory)

                if len(s) > 0:
                    # charfile = open("{}\{}.bin".format(directory,ch), "wb")
                    # self.compress(s, directory, ch)
                    self.writeFiles(s, directory, ch)
                    # sb = zlib.compress(s.encode('utf-8'))
                    # charfile.write(sb)
                    # charfile.close()
                    # print("done write {} in {} time".format(ch,asctime()))
                s = ""
                backword = ""

                ch = chr(ord(ch) + 1)
                if ch > '9' and ch < 'a' :
                    ch = 'a'
                # self.writeFiles("", directory, ch)



            if len(s) == 0:
                s = "{}-{}:{}".format(word[0],word[1],word[2])
                backword = word[0]
            elif backword == word[0]:
                s += ("_{}:{}".format(word[1],word[2]))
            else:
                s += ("|{}-{}:{}".format(word[0], word[1],word[2]))
                backword = word[0]

        # charfile = open("{}\{}.bin".format(directory,ch), "wb")
        # sb = zlib.compress(s.encode('utf-8'))
        # charfile.write(sb)
        # charfile.close()
        # self.compress(s, directory, ch)
        self.writeFiles("", directory, ch)
        self.writeFiles(s, directory, ch)
        print("done write  in {} time".format(asctime()))
        print( datetime.datetime.now()- self.startTime)
        self.startTime = datetime.datetime.now()





    def compress(self,s,directory,ch):
        charfile = open("{}\{}.bin".format(directory, ch), "wb")

        sb = zlib.compress(s.encode('utf-8'))
        charfile.write(sb)
        charfile.close()

    def writeFiles(self, s, directory, ch):
        charfile = open("{}\{}.txt".format(directory, ch), "w")
        charfile.write(s)
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






    def createFolders(self,dir):
        directory = "{}\\temp".format(dir)
        if not os.path.exists(directory):
            os.makedirs(directory)
        # for i in range(self.b):
        ch = '0'
        # if not os.path.exists(directory):
        #     os.makedirs(directory)
        #     self.createFolders(dir,i)
        while ch <= 'z':
            open("{}\{}.txt".format(directory, ch), "w")
            ch = chr(ord(ch) + 1)
            if ch > '9' and ch < 'a':
                ch = 'a'
        # print("done create Folders  in {} time".format(asctime()))
        return


    def removeIndex(dir):
        directory = "{}\{}".format(dir, 'temp')
        if os.path.exists(directory):
            shutil.rmtree(directory)
            # os.remove(directory)
        """Delete all index files by removing
        the given directory dir is the name of the directory in which all index files are located.
        After removing the files, the directory should be deleted."""





if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    start = time()
    time1 = datetime.datetime.now()

    dir = os.getcwd()
    file = os.getcwd()+"/text file/1000.txt"
    print(asctime())
    IW = IndexWriter(file,dir)
    # IW = IndexWriter.removeIndex(dir)

    print(asctime())
    # IW.findDoc(dir,"book")
    time2 =  datetime.datetime.now()
    time3 =  time2 - time1
    print(time3)
