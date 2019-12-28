import os ,os.path
import operator
import re
import sys
import zlib
import shutil
import datetime
import glob

from time import gmtime, asctime, time, sleep
import threading


class IndexWriter:
    Term = ''
    docId=0
    indexer = []
    f_tuple = []
    # temp_indexer= []
    # maxread = 100000000
    maxread = 500000000//10
    blocks = 0
    b = 40
    startTime=""
    threads = []
    threadsMerg = []
    dir =""
    lock = threading.Lock()
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
        self.blocks = 0
        self.threads = []
        self.dir = dir
        self.lock = threading.Lock()



        for i in range(4):
            self.threads.append(threading.Thread())
        for i in range(8):
            self.threadsMerg.append(threading.Thread())



        self.delfolder(dir+'\\temp')
        self.startTime = datetime.datetime.now()
        self.createTempFolder(inputFile)
        for i in self.threads:
            if i.is_alive():
                i.join()
            print("done")
        print("done create folders in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        self.startTime = datetime.datetime.now()


        self.mergeFolders(dir)
        print("done merge folders in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        self.startTime = datetime.datetime.now()



        # comressDir = '{}\compressFile'.format(dir)
        # self.createComprassFolder(comressDir)
        # self.compressFolder(folder,comressDir,dir)
        # print("done Comprass folder in {} time".format(asctime()))
        # print(datetime.datetime.now() - self.startTime)
        # self.startTime = datetime.datetime.now()




    def createTempFolder(self, inputFile):
        count = 1
        numthread = 0
        readfile = open(inputFile, "r")

        line = readfile.readline()

        # s = readfile.read(self.maxread)
        print("done read  in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        # def test(self,s,readfile,count):
        while line:
            if line[0] == '*':
                # self.temp_indexer = []
                self.lock.acquire()
                try:
                    line = readfile.readline()
                finally:
                    self.lock.release()
                line = re.sub(r"[^a-zA-Z0-9]+", ' ', line)
                line = line.lower()
                words = line.split()
                words.sort()
                # print(datetime.datetime.now() - self.startTime)
                firstWord = ""
                frequency = 1
                                    # self.temp_indexer.sort() #Sort the lists by AB
                                    # print("done temp sort in {} time".format(asctime()))
                                    # print(datetime.datetime.now() - self.startTime)

                for word in words :
                    if firstWord == "":
                        firstWord = word
                    elif  firstWord != word:
                        self.indexer.append((firstWord, count, frequency))
                        firstWord = word
                        # print(firstWord, count, frequency)
                        frequency = 1
                    else:
                        frequency += 1

                if frequency > 1:
                    self.indexer.append((firstWord, count, frequency))
                elif firstWord!='':
                    self.indexer.append((firstWord, count, 1))



                if sys.getsizeof(self.indexer) > self.maxread:
                    # if len(self.threads)
                    # print(numthread)
                    numthread = -1

                    while True:
                        print("wait1")
                        for i in range(len(self.threads)):
                            # print(i)
                            if not self.threads[i].isAlive():
                                print("i = {}".format(i))
                                numthread = i
                                # index = self.indexer
                                # print(self.indexer)
                                print("i = {}".format(i))
                                self.threads[numthread] = threading.Thread(target=self.writeToFileWrapper,args=(self.indexer,))
                                self.threads[numthread].start()
                                self.indexer = []
                                # print( self.indexer)
                                break
                        if numthread > -1:
                            break
                    # print("numthread = {}".format(numthread))


                    # self.threads.append(t)



                    # print("1 write - {}".format(asctime()))

                count += 1
                if count % 100000 == 0:
                    print("done {} in {} time".format(count,asctime()))



                    # self.indexer = list(dict.fromkeys(self.indexer)) #remove duplicates

                    # print(self.indexer)

            self.lock.acquire()
            try:
                line = readfile.readline()
            finally:
                self.lock.release()
            # s = readfile.read(self.maxread)
            # print("done {} in {} time".format(count, asctime()))
        if len(self.indexer) > 0:
            # print("2 write - {}".format(asctime()))
            numthread = -1
            while True:
                # print("wait")
                for i in range(len(self.threads)):
                    if not self.threads[i].is_alive():
                        numthread = i
                        self.threads[numthread] = threading.Thread(target=self.writeToFileWrapper, args = ( self.indexer,))
                        self.threads[numthread].start()
                        self.indexer=[]

                        break
                if numthread > -1:
                    break
            # index = copy.deepcopy(self.indexer)
            # self.indexer = []
            # self.threads[numthread] = threading.Thread(target=self.writeToFileWrapper)
            # self.threads[numthread].start()
            # self.writeToFile(dir,self.indexer)

            # self.MargeFile(dir)
        return

    def writeToFileWrapper(self,index):
        # index = copy.deepcopy(self.indexer)
        # self.indexer = []


        self.writeToFile(self.dir, index)
        self.blocks += 1
        # print(index)

    # def MargeFile(self,dir):
    #     directory = "{}\{}".format(dir, 'temp')
    #     countFiles = list(os.walk(directory))[0][1]
    #     # print(len(countFiles))
    #     # if (len(countFiles) % 2 > 0):
    #     #     print((countFiles))
    #
    #     directory0 = "{}\{}\{}\{}.bin".format(dir, 'temp', 0, 0)
    #     directory1 = "{}\{}\{}\{}.bin".format(dir, 'temp', 1, 0)
    #     dst = "{}\{}\{}.bin".format(dir, 'temp1', 0)
    #     self.mergeandsort(directory0,directory1,dst)

    def mergeFolders(self,dir):

        # if (self.blocks - 1) % 2 != 0:
        #     directory = "{}\{}".format(dir, 'temp')
        #     self.blocks+=1
        #     path = '{}\{}\\'.format(directory, self.blocks)
        #     if not os.path.exists(path):
        #         os.makedirs(path)
        #         self.createFolders(path)
        directory = "{}\{}".format(dir, 'temp')


        if os.path.exists(directory):
            folders = list(os.walk(directory))
            countfolders = len(folders[0][1])
            while  countfolders > 1:
                for f, b in zip(folders[1::2], folders[2::2]):
                    numthread = -1
                    while True:
                        # print("wait")
                        for i in range(len(self.threads)):
                            if not self.threads[i].is_alive():
                                numthread = i
                                self.threads[numthread] = threading.Thread(target=self.MergeFileWithThread,args=(directory,f,b))
                                self.threads[numthread].start()
                                # self.indexer = []

                                break
                        if numthread > -1:
                            break
                for i in self.threads:
                    if i.isAlive():
                        i.join()
                print("done loop merge  in {} time".format(asctime()))
                print(datetime.datetime.now() - self.startTime)
                self.startTime = datetime.datetime.now()
                folders = list(os.walk(directory))
                countfolders = len(folders[0][1])
                # if countfolders%2 != 0:
                #     directory = "{}\{}".format(dir, 'temp')
                #     self.blocks += 1
                #     path = '{}\{}\\'.format(directory, self.blocks)
                #     if not os.path.exists(path):
                #         os.makedirs(path)
                #         self.createFolders(path)
                #     directory = "{}\{}".format(dir, 'temp')
                #     folders = list(os.walk(directory))
                #     countfolders = len(folders[0][1])
        return

    def MergeFileWithThread(self,directory,f,b):
        self.blocks += 1
        path = '{}\{}\\'.format(directory, self.blocks)
        if not os.path.exists(path):
            os.makedirs(path)
            self.createFolders(path)
        if f and b:
            for i in range(len(f[2])):
                newpath = '{}{}'.format(path, f[2][i])
                folder1 = '{}\{}'.format(f[0], f[2][i])
                folder2 = '{}\{}'.format(b[0], b[2][i])
                numthread = -1
                while True:
                    # print("wait")
                    for j in range(len(self.threadsMerg)):
                        if not self.threadsMerg[j].is_alive():
                            numthread = j
                            self.threadsMerg[numthread] = threading.Thread(target=self.mergeandsort,args= (folder1, folder2, newpath))
                            self.threadsMerg[numthread].start()
                            break
                    if numthread > -1:
                        break

            for t in self.threadsMerg:
                if t.isAlive():
                    t.join()
            self.delfolder(f[0])
            self.delfolder(b[0])
        print("done merge  in {} time".format(asctime()))
        print(datetime.datetime.now() - self.startTime)
        self.startTime = datetime.datetime.now()

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
            if len(file1) > 0:
                newfile1 = zlib.decompress(file1).decode('utf-8')
            file2 = s2.read(self.maxread)
            if len(file2)>0:
                newfile2 = zlib.decompress(file2).decode('utf-8')
            while len(file1)>0 or len(file2)>0:
                if len(file1)>0:
                    newfile1 = newfile1.split("|")
                if len(file2)>0:
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


                # print ("s1 = {}".format(file1))
                # print ("s2 = {}".format(file2))


                # l.sort # Since you seem to want them in reverse order...
                # c = ''.join(l)
                # print(str)
                sb = zlib.compress(str.encode('utf-8'))
                d.write(sb)
                file1 = s1.read(self.maxread)
                if len(file1)>0:
                    newfile1 = zlib.decompress(file1).decode('utf-8')
                file2 = s2.read(self.maxread)
                if len(file2)>0:
                    newfile2 = zlib.decompress(file2).decode('utf-8')

        return



    def writeToFile(self , dir , index):
        index.sort(key=operator.itemgetter(0))  # Sort the lists by AB
        print("done sort  in {} time".format(asctime()))

        ch = '0'
        s = ""
        backword = ""
        # directory = "{}\{}\{}".format(dir,'a-z',self.blocks)
        # print("done blocks  in {}".format(self.blocks))
        directory = "{}\{}\{}".format(dir, 'temp', self.blocks)
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
                    self.compress(s, directory, ch)
                    # sb = zlib.compress(s.encode('utf-8'))
                    # charfile.write(sb)
                    # charfile.close()
                    # print("done write {} in {} time".format(ch,asctime()))
                s = ""
                backword = ""

                ch = chr(ord(ch) + 1)
                if ch > '9' and ch < 'a' :
                    ch = 'a'
                # print(ch)
                # print("ch = {}".format(ch))
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
        # print("ch = {}".format(ch))
        # self.writeFiles("", directory, ch)
        self.compress(s, directory, ch)
        print("done write  in {} time".format(asctime()))
        print( datetime.datetime.now()- self.startTime)
        self.startTime = datetime.datetime.now()



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


    def compress(self,s,directory,ch):
        charfile = open("{}\{}.bin".format(directory, ch), "wb")
        sb = zlib.compress(s.encode('utf-8'))
        charfile.write(sb)
        charfile.close()

    # def writeFiles(self, s, directory, ch):
    #     charfile = open("{}\{}.bin".format(directory, ch), "wb")
    #     sb = zlib.compress(s.encode('utf-8'))
    #     charfile.write(sb)
    #     charfile.close()



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
    file = os.getcwd()+"\\text file\\1000000.txt"
    print(asctime())
    IW = IndexWriter(file,dir)
    # IW = IndexWriter.removeIndex(dir)

    print(asctime())
    # IW.findDoc(dir,"book")
    time2 =  datetime.datetime.now()
    time3 =  time2 - time1
    print(time3)
