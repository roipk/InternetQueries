# from InternetQueries.Ex1 import IndexReader
# import InternetQueries.Ex1.IndexReader as IR
import operator
import os
import math
from IndexReader import IndexReader
import zlib

class IndexSearcher:
    dir=''
    indexReader=''
    indexer=[]

    dic = dict()



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
        if k <= 0:
            return
        s = []
        v=''
        for i in range(len(query)):
            ch = query[i]
            if 'A' <= ch <= 'Z':
                v += '{}'.format(chr(ord(ch) + 32))
            elif 'a' <= ch <= 'z' or '0' <= ch <= '9':
                v += '{}'.format(ch)

            elif len(v) > 2:
                s.append(v)
                v = ''
            else:
                v = ''
        if len(v) > 0:
            s.append(v)
        s.sort()
        firstWord = ""
        frequency = 1
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

        N = self.indexReader.getNumberOfDocuments()  # מספר המסמכים הקיימים
        normalization = 0
        for token in self.indexer:
            docs = self.indexReader.getDocsBy_lnn_ltc(token[0])
            idf = math.log10(N / len(docs)) + 1
            if type(docs) == list:
                for i in range(len(docs)):
                    isNone = self.dic.get(docs[i][0])
                    if isNone == None:                                           #  מספר המילים בשאילתה X אחד חלקי שורש כל המופעים של המילה במסמך הנוכחי X לוג כל המסמכים חלקי המסמכים שמכילים את הביטוי X אחד ועוד לוג המילים שמופיעות במסמך
                        normalization = docs[i][1] * idf * docs[i][2] * token[1] # 1+log(tf) * log(N/df) * 1/(w1^2,w2^2,...,wn^2)^2  * tq
                    else:
                        normalization += docs[i][1] * idf * docs[i][2] * token[1]
                    self.dic.update({docs[i][0]: normalization})

        sorted_dic = sorted(self.dic.items(), key=operator.itemgetter(1) , reverse=True)
        show = ()
        if k >  len(sorted_dic):
            k = len(sorted_dic)
        for i in range(k):
            show += (sorted_dic[i][0],)
        return  show



if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    IR = IndexReader(dir)
    IS = IndexSearcher(IR)
    print(IS.vectorSpaceSearch("assembled illustrations Moosewood ", 5))

