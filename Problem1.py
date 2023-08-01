from mrjob.job import MRJob
import re

class Problem1(MRJob):

    def mapper(self, key, value):
        words = re.compile(r"[\w']+").findall(value)
        for word in words:
            yield (word.lower(), 1)

    def reducer(self, key, value):
        count = sum(value)
        yield (key, count)

if __name__ == '__main__':
    Problem1.run()
