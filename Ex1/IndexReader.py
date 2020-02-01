# roimd 203485164
# gilili 312243561
import math
import os
import zlib
import operator


class IndexReader:
    dir =""
    wordInFile=[]
    charFile=''
    maxread = 150000000000000


    def __init__(self, dir):
        """Creates an IndexReader which will read from the given
        directory dir is the name of the directory in which all
        index files are located."""
        self.dir = '{}\\Final Project\\File Compressed\\'.format(dir)
        if not os.path.exists(self.dir):
            self.dir = dir

    def readCompressFile(self, token):
        directory = '{}\\{}.bin'.format(self.dir, token[0])
        if token[0]!=self.charFile:
            self.charFile = token[0]
            with open(directory, 'rb') as s1:
                newfile1=""
                file1 = s1.read()
                tempfile = file1
                file1 = s1.read()
                while file1:
                    tempfile += file1
                    file1 = s1.read()
                if len(tempfile)>0:
                    newfile1 = zlib.decompress(tempfile).decode('utf-8')

                if len(newfile1)>0:
                    newfile1 = newfile1.split("|")

            self.wordInFile = newfile1

    def getTokenFrequency(self,token):
        """Return the number of documents containing a given token (i.e., word)
        Returns 0 if there are no documents containing this token"""
        token = token.lower()
        self.readCompressFile(token)
        index = self.binary_search(self.wordInFile, token, 0, len(self.wordInFile) - 1)
        if index < 0:
            return 0
        s = self.wordInFile[index].split("-")
        s = s[1].split("_")
        return len(s)

    def getTokenCollectionFrequency(self,token):
        """Return the number of times that a given token (i.e., word)
         appears in the whole collection. Returns 0 if there are no documents containing this token"""
        token = token.lower()
        self.readCompressFile(token)
        index = self.binary_search(self.wordInFile, token, 0, len(self.wordInFile) - 1)
        if index < 0:
            return 0
        count = 0
        s = self.wordInFile[index].split("-")
        s = s[1].split("_")
        for j in s:
            temp = j.split(':')
            temp = temp[1].split(';')
            count += int(math.pow(10,float(temp[0])-1))
        return count

    def getDocsBy_lnn_ltc(self, token):
        """Returns a series of integers of the form id1, freq-1, id-2, freq-2, ...
        such that id-n is the n-th document containing the given token and freq-n is the number of times
        that the token appears in doc id-n Note that the integers should be sorted by id.
        Returns an empty Tuple if there are no documents containing this token"""

        token = token.lower()
        self.readCompressFile(token)
        index = self.binary_search(self.wordInFile,token,0,len(self.wordInFile)-1)

        toupels = []
        if index < 0:
            return toupels

        s = self.wordInFile[index].split("-")

        s = s[1].split("_")
        for j in s:
            tempDoc = j.split(':')
            tempFr = tempDoc[1].split(';')
            toupels.append((int(tempDoc[0]), float(tempFr[0]),float(tempFr[1])))
        if(len(toupels)>0):
            from operator import itemgetter
            toupels.sort(key=itemgetter(0))
            toupels.sort(key=itemgetter(1), reverse=True)

        return toupels

    def getDocsWithToken(self, token):
        token = token.lower()
        self.readCompressFile(token)
        index = self.binary_search(self.wordInFile,token,0,len(self.wordInFile)-1)

        toupels = []
        if index < 0:
            return toupels

        s = self.wordInFile[index].split("-")

        s = s[1].split("_")
        for j in s:
            tempDoc = j.split(':')
            tempFr = tempDoc[1].split(';')
            toupels.append((int(tempDoc[0]), int(math.pow(10,float(tempFr[0])-1))))
        if(len(toupels)>0):
            from operator import itemgetter
            toupels.sort(key=itemgetter(0))
            toupels.sort(key=itemgetter(1), reverse=True)
        return toupels

    def getNumberOfDocuments(self):
        """Return the number of documents in the collection"""
        t = self.getDocsWithToken('0')
        t.sort()
        return t[-1][0]

    def binary_search(self,arr, val, start, end):

        # we need to distinugish whether we should insert
        # before or after the left boundary.
        # imagine [0] is the last step of the binary search
        # and we need to decide where to insert -1
        if start == end:
            s = arr[start].split("-")
            if s[0] != val:
                return -1
            else:
                return start

        # this occurs if we are moving beyond left\'s boundary
        # meaning the left boundary is the least position to
        # find a number greater than val
        if start > end:
            return -1

        s = arr[start].split("-")
        if s[0] == val:
            return start

        s = arr[end].split("-")
        if s[0] == val:
            return end
        mid = (start + end) // 2
        s = arr[mid].split("-")


        if s[0] < val:
            return self.binary_search(arr, val, mid + 1, end)
        elif s[0] > val:
            return self.binary_search(arr, val, start, mid - 1)
        else:
            return mid


if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    IR = IndexReader(dir)
    print(IR.getTokenFrequency("book"))
    print(IR.getTokenCollectionFrequency("book"))
    print(IR.getDocsWithToken("Book"))
    print(IR.getNumberOfDocuments())
