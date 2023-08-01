from mrjob.job import MRJob
import re

stopwords = set(["the", "and", "of", "a", "to", "in", "is", "it"])

class Problem2(MRJob):

    def mapper(self, key, value):
        words = re.compile(r"[\w']+").findall(value)
        for word in words:
            if word.lower() not in stopwords:
                yield (word.lower(), 1)

    def reducer(self, key, value):
        count = sum(value)
        yield (key, count)

if __name__ == '__main__':
    Problem2.run()
