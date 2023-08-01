from mrjob.job import MRJob
import re

class Problem3(MRJob):

    def mapper(self, key, value):
        words = re.compile(r"[\w']+").findall(value)
        for i in range(len(words)-1):
            twowords = (words[i].lower(), words[i+1].lower())
            yield (",".join(twowords), 1)

    def reducer(self, twowords, value):
        count = sum(value)
        yield (twowords, count)

if __name__ == '__main__':
    Problem3.run()
