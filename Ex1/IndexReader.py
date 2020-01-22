# roimd 203485164
# gilili 312243561

import os
import zlib


class IndexReader:
    dir =""
    maxread = 1500000000
    def __init__(self, dir):
        """Creates an IndexReader which will read from the given
        directory dir is the name of the directory in which all
        index files are located."""
        self.dir = '{}\\temp\\File Compressed\\'.format(dir)
        print(self.dir)
        if not os.path.exists(self.dir):
            self.dir = dir



    def readCompressFile(self, file):
        # Use `with` statements to close file automatically
        with open(file, 'rb') as s1:
            newfile1=""
            file1 = s1.read(self.maxread)
            tempfile = file1
            file1 = s1.read(self.maxread)
            while file1:
                tempfile += file1
                file1 = s1.read(self.maxread)
            if len(tempfile)>0:
                newfile1 = zlib.decompress(tempfile).decode('utf-8')

            if len(newfile1)>0:
                newfile1 = newfile1.split("|")

        return newfile1


    def getTokenFrequency(self,token):
        """Return the number of documents containing a given token (i.e., word)
        Returns 0 if there are no documents containing this token"""
        token = token.lower()
        directory = '{}\\{}.bin'.format(self.dir,token[0])
        newfile1 = self.readCompressFile(directory)
        for i in range(len(newfile1)):
            s = newfile1[i].split("-")
            # print(s)
            if s[0] == token:
                s = s[1].split('_')
                return len(s)
        return 0

    def getTokenCollectionFrequency(self,token):
        """Return the number of times that a given token (i.e., word)
         appears in the whole collection. Returns 0 if there are no documents containing this token"""
        token = token.lower()
        directory = '{}\\{}.bin'.format(self.dir, token[0])

        newfile1 = self.readCompressFile(directory)
        for i in range(len(newfile1)):
            s = newfile1[i].split("-")
            if s[0] == token:
                s = s[1].split('_')
                count = 0
                for j in s:
                    temp = j.split(':')
                    count += int(temp[1])
                return count
        return 0


    def getDocsWithToken(self, token):
        """Returns a series of integers of the form id1, freq-1, id-2, freq-2, ...
        such that id-n is the n-th document containing the given token and freq-n is the number of times
        that the token appears in doc id-n Note that the integers should be sorted by id.
        Returns an empty Tuple if there are no documents containing this token"""
        token = token.lower()
        directory = '{}\\{}.bin'.format(self.dir, token[0])

        newfile1 = self.readCompressFile(directory)
        for i in range(len(newfile1)):
            s = newfile1[i].split("-")
            if s[0] == token:
                s = s[1].split('_')
                toupels = []
                for j in s:
                    temp = j.split(':')
                    toupels.append((int(temp[0]), int(temp[1])))
                return toupels
        return []


    def getNumberOfDocuments(self):
        """Return the number of documents in the collection"""
        directory = '{}\\{}.bin'.format(self.dir,'a')
        newfile1 = self.readCompressFile(directory)
        for i in range(len(newfile1)):
            s = newfile1[i].split("-")
            if s[0] == 'a':
                s = s[1].split('_')
                count = s[-1].split(':')
                return count[0]
        return 0



if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    # print(dir)
    IR = IndexReader(dir)
    print(IR.getTokenFrequency("Book"))
    print(IR.getTokenCollectionFrequency("Book"))
    print(IR.getDocsWithToken("Book"))
    # IR.getNumberOfDocuments("Book")
    print(IR.getNumberOfDocuments())
