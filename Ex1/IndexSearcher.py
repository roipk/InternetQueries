# from InternetQueries.Ex1 import IndexReader
# import InternetQueries.Ex1.IndexReader as IR
import os
import math
from IndexReader import IndexReader
import zlib

class IndexSearcher:
    dir=''
    indexReader=''
    indexer=[]
    def __init__(self, iReader):
        """
        Constructor.
        iReader is the IndexReader object on which the search should be performed
        """
        self.indexReader = iReader
        self.indexer=[]
        



    def vectorSpaceSearch(self, query, k):
        """
        Returns a tupple containing the id-s of the k most highly ranked reviews for
        the given query, using the vector space ranking function lnn.ltc (using the SMART notation).
        The id-s should be sorted by the ranking.
        """
        s = []
        v=''
        for i in range(len(query)):
            ch = query[i]
            if 'A' <= ch <= 'Z':
                v += '{}'.format(chr(ord(ch) + 32))
            elif 'a' <= ch <= 'z' or '0' <= ch <= '9':
                v += '{}'.format(ch)

            elif len(v) > 2:
                # if v not in self.stopwords:
                s.append(v)
                v = ''
            else:
                v = ''
        if len(v) > 0:
            # if v not in self.stopwords:
            s.append(v)
        s.sort()
        firstWord = ""
        frequency = 1
        s.sort()
        for word in s:
            if firstWord == "":
                firstWord = word
            elif firstWord != word:
                self.indexer.append((firstWord, frequency))
                firstWord = word
                frequency = 1
            else:
                frequency += 1

        if frequency > 1:
            self.indexer.append((firstWord, frequency))
        elif firstWord != '':
            self.indexer.append((firstWord, 1))


        backword=''
        tokens = []
        vector=[]
        factor = 0
        N = self.indexReader.getNumberOfDocuments()
        for token in self.indexer:
            if token!= backword:
                docs = self.indexReader.getDocsWithToken(token[0])
                if(type(docs) == list):
                    print(docs)
                    for doc in docs:
                        tf = 1 + math.log10(doc[1])
                        idf = math.log10(N/len(docs))
                        vector.append(tf*idf)
                    for i in vector:
                        factor+=math.pow(i,2)
                    factor = 1/math/pow(factor,0.5)



     











if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    IR = IndexReader(dir)
    IS = IndexSearcher(IR)
    IS.vectorSpaceSearch("Book Bread",3)
    # print(dir)
    # IS=IndexSearcher(getTokenFrequency("Book"))
