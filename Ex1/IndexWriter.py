import math
import os ,os.path
import operator

import sys
import zlib
import shutil
import datetime
import glob
import bisect

from time import gmtime, asctime, time
import threading


class IndexWriter:
    Term = ''
    docId=0
    indexer = []
    f_tuple = []
    # temp_indexer= []
    maxread = 20000000000

    blocks = chr(ord('A'))
    b = 40
    startTime=""
    threads = []
    threadsWrite = []
    dir =""
    lock = threading.Lock()
    debug = True
    # debug = False
    numBlock = 0
    stopwords={}



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
        # self.temp_indexer = []
        self.blocks =  chr(ord('A') - 1)
        self.threads = []
        self.dir = dir
        self.lock = threading.Lock()
        self.numBlock = 0
        # self.stopwords={'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself','which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than'}


        for i in range(8):
            self.threads.append(threading.Thread())
        for i in range(8):
            self.threadsWrite.append(threading.Thread())



        self.delfolder(dir+'\\temp')
        self.startTime = datetime.datetime.now()
        self.createTempFolder(inputFile)


        print("done create folders in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        # self.startTime = datetime.datetime.now()


        self.mergeFolders(dir)
        print("done merge folders in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        # self.startTime = datetime.datetime.now()
        os.chdir(dir+"\\temp")
        path =list(os.walk(os.getcwd()))[0]
        if self.debug:
            print(path[1][0])
        os.rename(path[1][0], "File Compressed")


    def readfiles(self,path):
        with open(path, 'rb') as s1:
            file1 = s1.read()
            if self.debug:
                print(file1)

        with open(path, 'rb') as s1:
            file1 = s1.read(self.maxread)
            tempfile = file1
            file1 = s1.read(self.maxread)
            while file1:
                tempfile+=file1
                file1 = s1.read(self.maxread)
            txt = zlib.decompress(tempfile).decode('utf-8')
            if self.debug:
                print(tempfile)
                print(txt)


    def createTempFolder(self, inputFile):
        count = 0
        numthread = 0
        with open(inputFile,buffering=4000000) as f:
            for line in f:
                s = []
                if line[0] != '*' and line[0] != '\n':
                    count+=1
                    v = ''
                    for i in range(len(line)):
                        ch = line[i]
                        if 'A' <= ch <= 'Z':
                            v += '{}'.format(chr(ord(ch) + 32))
                        elif 'a' <= ch <= 'z' or '0' <= ch <= '9':
                            v += '{}'.format(ch)

                        elif len(v) > 2 or '0'<= v <= '9':
                            # if v not in self.stopwords:
                            if(v == 'asgrids'):
                                print(v,count)
                            s.append(v)
                            v = ''
                        else:
                            v = ''
                    if len(v) > 0:
                        # if v not in self.stopwords:
                        s.append(v)
                        v=''

                    firstWord = ""
                    frequency = 1
                    s.sort()
                    doclen = 0
                    # terms = len(s)
                    for word in s:
                        if firstWord == "":
                            firstWord = word
                        elif firstWord != word:
                            doclen += math.pow(math.log10(frequency),2)
                            firstWord = word
                            frequency = 1
                        else:
                            frequency += 1
                    if frequency > 1:
                        doclen += math.pow(math.log10(frequency), 2)
                    elif firstWord != '':
                        doclen += math.pow(math.log10(1), 2)

                    if doclen > 0:
                        doclen = 1 / (math.pow(doclen,0.5))
                    frequency = 1
                    firstWord = ""
                    for word in s:
                        if firstWord == "":
                            firstWord = word
                        elif firstWord != word:
                            # bisect.insort(self.indexer, (firstWord, count, frequency))
                            self.indexer.append((firstWord, count,math.log10(frequency)+1 , doclen))
                            firstWord = word
                            # print(firstWord, count, frequency)
                            frequency = 1
                        else:
                            frequency += 1

                    if frequency > 1:
                        # bisect.insort(self.indexer, (firstWord, count, frequency))
                        self.indexer.append((firstWord, count, math.log10(frequency)+1,doclen))
                    elif firstWord != '':
                        # bisect.insort(self.indexer, (firstWord, count, 1))
                        self.indexer.append((firstWord, count, math.log10(1)+1,doclen))


                    if len(self.indexer) > 0 and (sys.getsizeof(self.indexer) * sys.getsizeof(self.indexer[0])) > self.maxread:
                        indexer = self.indexer
                        self.indexer=[]
                        self.sortFile(indexer)
                        directory = "{}\{}".format(dir, 'temp')
                        if os.path.exists(directory):
                            folders = list(os.walk(directory))
                        if self.debug:
                            print("continue")

                    if count % 100000 == 0 and self.debug:
                        print("done {} in {} time".format(count,datetime.datetime.now() - self.startTime))




        if self.debug:
            print(count)
            # print(datetime.datetime.now() - self.startTime)
            print("done create dictionary after {} time ".format(datetime.datetime.now() - self.startTime))
        if len(self.indexer) > 0:
            indexer = self.indexer
            indexer.append(('0', count, 0,0))
            self.indexer = []
            self.sortFile(indexer)
            if self.debug:
                print("continue")

        for i in self.threads:
            if i.is_alive():
                i.join()
            if self.debug:
                print("done",i)
        return


    def sortFile(self,indexer):
        while True:
            for i in range(len(self.threads)):
                # print(i)
                if not self.threads[i].isAlive():
                    # print("i = {}".format(i))
                    numthread = i
                    # index = self.indexer
                    # print(self.indexer)
                    if self.debug:
                        print("i = {}".format(i))
                    # stime = datetime.datetime.now()
                    self.threads[numthread] = threading.Thread(target=self.writeToFileWrapper, args=(indexer,))
                    self.threads[numthread].start()
                    if self.debug:
                        print("doneThread")
                    # print(datetime.datetime.now()-stime)
                    # print("numthread = {}".format(numthread) )
                    # print( self.indexer)
                    return


    def writeToFileWrapper(self,index):

        # index = copy.deepcopy(self.indexer)
        # self.indexer = []
        self.lock.acquire()
        try:
            # chr(ord(self.blocks) + 1)
            if self.blocks[-1] < 'Z' and len(self.blocks) ==  1:
                self.blocks = chr(ord(self.blocks ) + 1)
            elif self.blocks[-1] < 'Z':
                self.blocks = '{}{}'.format(self.blocks[:-1],chr(ord(self.blocks[-1]) + 1))
            else:
                self.blocks = '{}A'.format(self.blocks)
        finally:
            self.lock.release()
        self.writeToFile(self.dir, index, self.blocks)
        # if self.debug:
        #     print("end")
        return


    def mergeFolders(self,dir):
        directory = "{}\{}".format(dir, 'temp')
        if os.path.exists(directory):
            folders = list(os.walk(directory))
            countfolders = len(folders[0][1])

            while  countfolders > 1:

                for f, b in zip(folders[1::2], folders[2::2]):
                    if f and b:
                        self.MergeFileWithThread(directory, f, b)

                for k in self.threads:
                    if k.isAlive():
                        k.join()


                # for i in self.threadsWrite:
                #     if i.isAlive():
                #         i.join()
                if self.debug:
                    print("done loop merge  in {} time".format(asctime()))
                    print(datetime.datetime.now() - self.startTime)
                os.chdir(directory)
                if countfolders > 1 and countfolders % 2 == 1:
                    if self.blocks[-1] < 'Z' and len(self.blocks) == 1:
                        self.blocks = chr(ord(self.blocks) + 1)
                    elif self.blocks[-1] < 'Z':
                        self.blocks = '{}{}'.format(self.blocks[:-1], chr(ord(self.blocks[-1]) + 1))
                    else:
                        self.blocks = '{}A'.format(self.blocks)
                    os.rename(folders[0][1][-1], self.blocks)
                os.chdir(dir)
                # self.startTime = datetime.datetime.now()
                folders = list(os.walk(directory))
                # if self.debug:
                #     print(folders)
                countfolders = len(folders[0][1])


        return


    def getCorrectFolder(self,folders,low):
        sortlist = folders[0][1]
        min = (0,0)
        for i in range(len(sortlist)):
            if int(sortlist[i]) == low[0]:
                min = (i + 1, min[1])
            elif int(sortlist[i]) == low[1]:
                min = (min[0], i + 1)
        # if self.debug:
        #     print(sortlist)
        #     print(low[0],low[1])
        #     print(min)
        return min



    def MergeFileWithThread(self,directory,f,b):
        if self.blocks[-1] < 'Z' and len(self.blocks) == 1:
            self.blocks = chr(ord(self.blocks) + 1)
        elif self.blocks[-1] < 'Z':
            self.blocks = '{}{}'.format(self.blocks[:-1], chr(ord(self.blocks[-1]) + 1))
        else:
            self.blocks = '{}A'.format(self.blocks)
        path = '{}\{}\\'.format(directory, self.blocks)
        if not os.path.exists(path):
            os.makedirs(path)
            self.createFolders(path)
        for i in range(len(f[2])):
            newpath = '{}{}'.format(path, f[2][i])
            folder1 = '{}\{}'.format(f[0], f[2][i])
            folder2 = '{}\{}'.format(b[0], b[2][i])
            self.lock.acquire()
            try:
                self.writeCharsFile(folder1, folder2, newpath)
            finally:
                self.lock.release()
        for k in self.threadsWrite:
            if k.isAlive():
                k.join()

        self.delfolder(f[0])
        self.delfolder(b[0])

        if self.debug:
            print("done merge  in {} time".format(asctime()))
            print(datetime.datetime.now() - self.startTime)
        # self.startTime = datetime.datetime.now()
        for k in self.threads:
            if k.isAlive():
                k.join()
        for m in self.threadsWrite:
            if m.isAlive():
                m.join()

        return


    def writeCharsFile(self,folder1, folder2, newpath):
        t = threading.Thread(target=self.mergeandsort, args=(folder1, folder2, newpath))
        t.start()
        t.join()
        return



    def delfolder(self,path):
        if os.path.exists(path):
            shutil.rmtree(path)
        return


    def mergeandsort(self, src1, src2, dst):
        # Use `with` statements to close file automatically

        with open(src1, 'rb') as s1, open(src2, 'rb') as s2, open(dst, 'ab') as d:
            newfile1=""
            newfile2=""
            file1 = s1.read(self.maxread)
            tempfile = file1
            file1 = s1.read(self.maxread)
            while file1:
                tempfile += file1
                file1 = s1.read(self.maxread)
            if len(tempfile)>0:
                newfile1 = zlib.decompress(tempfile).decode('utf-8')

            file2 = s2.read(self.maxread)
            tempfile = file2
            file2 = s2.read(self.maxread)
            while file2:
                tempfile += file2
                file2 = s2.read(self.maxread)
            if len(tempfile) > 0:
                newfile2 = zlib.decompress(tempfile).decode('utf-8')

            if len(newfile1)>0:
                newfile1 = newfile1.split("|")

            if len(newfile2)>0:
                newfile2 = newfile2.split("|")


            i=0
            j=0
            str = ''
            while i < len(newfile1) and j < len(newfile2):
                    sub1 = newfile1[i].split("-")
                    sub2 = newfile2[j].split("-")

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

            while i < len(newfile1):
                    sub1 = newfile1[i].split("-")
                    if len(sub1[0]) > 0:
                        if len(str) == 0:
                            str += "{}-{}".format(sub1[0], sub1[1])
                        else:
                            str += "|{}-{}".format(sub1[0], sub1[1])
                    i += 1

            while j < len(newfile2):
                    sub2 = newfile2[j].split("-")
                    if len(sub2[0]) > 0:
                        if len(str) == 0:
                            str += "{}-{}".format(sub2[0], sub2[1])
                        else:
                            str += "|{}-{}".format(sub2[0], sub2[1])
                    j += 1

            sb = zlib.compress(str.encode('utf-8'))

            d.write(sb)

        return


    def writeToFile(self , dir , index,blocks):
        # print(index)
        index.sort(key=operator.itemgetter(0))  # Sort the lists by AB
        # print("done sort  in {} time".format(asctime()))

        ch = '0'
        s = ""
        backword = ""
        # directory = "{}\{}\{}".format(dir,'a-z',self.blocks)
        # print("done blocks  in {}".format(self.blocks))
        directory = "{}\{}\{}".format(dir, 'temp', blocks)
        self.createFolders(directory)
        # print("start index")

        # print (self.indexer)
        for word in index:
            numbers = ""
            # print("ch ccccc= {}".format(ch))
            # print( word[0][0])

            while word[0][0] != ch :
                if not os.path.exists(directory):
                    os.makedirs(directory)

                if len(s) > 0:
                    # charfile = open("{}\{}.bin".format(directory,ch), "wb")
                    # self.compress(s, directory, ch)
                    numthread = -1
                    while True:
                        # print("wait")
                        for j in range(len(self.threadsWrite)):
                            if not self.threadsWrite[j].is_alive():
                                numthread = j
                                self.threadsWrite[numthread] = threading.Thread(target=self.compressFile,args=(s,directory, ch))
                                self.threadsWrite[numthread].start()
                                break
                        if numthread > -1:
                            break
                s = ""
                backword = ""

                ch = chr(ord(ch) + 1)
                if ch > '9' and ch < 'a' :
                    ch = 'a'
                # print(ch)
                # print("ch = {}".format(ch))
                # self.writeFiles("", directory, ch)



            if len(s) == 0:
                s = "{}-{}:{};{}".format(word[0],word[1],word[2],word[3])
                backword = word[0]
            elif backword == word[0]:
                # print(word)
                s += ("_{}:{};{}".format(word[1],word[2],word[3]))
            else:
                s += ("|{}-{}:{};{}".format(word[0], word[1],word[2],word[3]))
                backword = word[0]

        numthread = -1
        while True:
            # print("wait")
            for j in range(len(self.threadsWrite)):
                if not self.threadsWrite[j].is_alive():
                    numthread = j
                    self.threadsWrite[numthread] = threading.Thread(target=self.compressFile, args=(s,directory, ch))
                    self.threadsWrite[numthread].start()
                    break
            if numthread > -1:
                break
        for t in self.threadsWrite:
            if t.isAlive():
                t.join()
        if self.debug:
            print("done write  in {} time".format(asctime()))
            print( datetime.datetime.now()- self.startTime)
        # self.startTime = datetime.datetime.now()
        return


    def compressFolder(self,directoryIn,directoryOut,originPath):
        os.chdir(directoryIn)
        for f in glob.glob("*.bin"):
            fileName = f.split(".bin")[0]
            file = open(f, 'rb')
            txt = file.read()
            # print(txt)
            if len(txt) > 3:
                txt = zlib.decompress(txt).decode('utf-8')
            while len(txt) > 0 :
                dir = "{}\{}.bin".format(directoryOut, fileName)
                if not os.path.exists(dir):
                    charfile = open(dir, "wb")
                else:
                    charfile = open(dir, "ab")
                sb = zlib.compress(txt.encode('utf-8'))
                charfile.write(sb)
                charfile.close()
                txt = file.read(self.maxread)
                if(txt):
                    txt = zlib.decompress(txt).decode('utf-8')


            file.close()
        directoryIn = directoryIn[:-2]
        os.chdir(originPath)
        self.delfolder(directoryIn)
        return


    def compressFile(self,s,directory,ch):
        charfile = open("{}\{}.bin".format(directory, ch), "wb")
        sb = zlib.compress(s.encode('utf-8'))
        charfile.write(sb)
        charfile.close()



    def findDoc(self,directory,word):
        charfile = open("{}\\a-z\\{}.bin".format(directory, word[0]), "rb")
        s = charfile.read()
        charfile.close()
        s = zlib.decompress(s).decode('utf-8')


        wordArray = s.split("|")
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


    def createComprassFolder(self, dir):
        if not os.path.exists(dir):
            os.makedirs(dir)
        ch = '0'
        while ch <= 'z':
            # print("ch = {}".format(ch))
            open("{}\{}.bin".format(dir, ch), "wb")
            ch = chr(ord(ch) + 1)
            if ch > '9' and ch < 'a':
                ch = 'a'
        # print("done create Folders  in {} time".format(asctime()))
        return


    def createFolders(self,dir):
        self.lock.acquire()
        try:
            if not os.path.exists(dir):
                os.makedirs(dir)
        finally:
            self.lock.release()

        ch = '0'
        while ch <= 'z':
            # print("ch = {}".format(ch))
            open("{}\{}.bin".format(dir, ch), "wb")
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


    def worker(self):
        """thread worker function"""
        print('Worker')
        return


if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    start = time()
    time1 = datetime.datetime.now()

    dir = os.getcwd()
    file = os.getcwd()+"\\text file\\100.txt" # 100 kilo
    # file = os.getcwd()+"\\text file\\100000.txt" # 100 Mega
    # file = os.getcwd()+"\\text file\\1000000.txt" # 1 Giga
    # file = os.getcwd()+"\\text file\\10000000.txt" # 8Giga

    print(asctime())
    IW = IndexWriter(file,dir)
    # IW = IndexWriter.removeIndex(dir)

    print(asctime())
    # IW.findDoc(dir,"book")
    time2 =  datetime.datetime.now()
    time3 =  time2 - time1
    print(time3)
