# from InternetQueries.Ex1 import IndexReader
# import InternetQueries.Ex1.IndexReader as IR
import os
import zlib

class IndexSearcher:
    def __init__(self, iReader):
        """Constructor.
    iReader is the IndexReader object on which the search should be performed """
        IR = IndexReader(dir)
        



    def vectorSpaceSearch(self, query, k):
        """Returns a tupple containing the id-s of the k most highly ranked reviews for
    the given query, using the vector space ranking function lnn.ltc (using the SMART notation).
     The id-s should be sorted by the ranking."""


if __name__ =="__main__":
    """part 1.3.1 IndexWriter"""
    dir = os.getcwd()
    # print(dir)
    IS=IndexSearcher(getTokenFrequency("Book"))
